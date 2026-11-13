//! Response invariants evaluated over the workers' outcomes.

use pgloom_engine::{Violation, WorkerOutcome};
use serde_json::Value;

use crate::config::{ApplyTo, JsonCheck, ResponseInvariant};

/// Evaluates every response invariant; `ids[i]` names the outcome `outcomes[i]`.
pub fn check_responses(
    invariants: &[ResponseInvariant],
    ids: &[String],
    outcomes: &[Option<WorkerOutcome>],
) -> Vec<Violation> {
    let successes = outcomes.iter().flatten().filter(|o| o.success).count();
    let http = outcomes
        .iter()
        .flatten()
        .any(|o| o.label.starts_with("HTTP "));
    let noun = if http {
        "HTTP 2xx"
    } else {
        "successful workers"
    };
    let roster = || {
        ids.iter()
            .zip(outcomes.iter().chain(std::iter::repeat(&None)))
            .map(|(id, o)| format!("{id}={}", describe(o.as_ref())))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut out = Vec::new();
    for inv in invariants {
        let mut failures = Vec::new();
        if let Some(max) = inv.max_2xx_count
            && successes > max
        {
            failures.push(format!(
                "expected <= {max} {noun}, got {successes} ({})",
                roster()
            ));
        }
        if let Some(min) = inv.min_2xx_count
            && successes < min
        {
            failures.push(format!(
                "expected >= {min} {noun}, got {successes} ({})",
                roster()
            ));
        }
        if let Some(allowed) = &inv.allowed_statuses {
            let offenders: Vec<String> = ids
                .iter()
                .zip(outcomes)
                .filter(|(_, o)| {
                    o.as_ref()
                        .is_none_or(|o| o.status.is_none_or(|s| !allowed.contains(&s)))
                })
                .map(|(id, o)| format!("{id}={}", describe(o.as_ref())))
                .collect();
            if !offenders.is_empty() {
                let allowed = allowed
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                failures.push(format!(
                    "status outside [{allowed}]: {}",
                    offenders.join(", ")
                ));
            }
        }
        if inv.checks_bodies() {
            failures.extend(check_bodies(inv, ids, outcomes));
        }
        if !failures.is_empty() {
            out.push(Violation::InvariantFailed {
                name: inv.name.clone(),
                description: failures.join("; "),
                observed: String::new(),
            });
        }
    }
    out
}

/// `json` and `distinct` over the HTTP responses in scope.
fn check_bodies(
    inv: &ResponseInvariant,
    ids: &[String],
    outcomes: &[Option<WorkerOutcome>],
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut documents: Vec<(&str, Value)> = Vec::new();
    for (id, outcome) in ids.iter().zip(outcomes) {
        if inv.workers.as_ref().is_some_and(|w| !w.contains(id)) {
            continue;
        }
        let Some(o) = outcome else { continue };
        let Some(body) = &o.body else { continue };
        if inv.apply_to == ApplyTo::Success && !o.success {
            continue;
        }
        match serde_json::from_str::<Value>(body) {
            Ok(doc) => documents.push((id, doc)),
            Err(_) => failures.push(format!("{id}: response body is not JSON")),
        }
    }
    for (id, doc) in &documents {
        for check in &inv.json {
            failures.extend(
                check_field(check, doc.pointer(&check.pointer)).map(|e| format!("{id}: {e}")),
            );
        }
    }
    if let Some(ptr) = &inv.distinct {
        let values: Vec<(&str, &Value)> = documents
            .iter()
            .filter_map(|(id, doc)| doc.pointer(ptr).map(|v| (*id, v)))
            .collect();
        let duplicated: Vec<String> = values
            .iter()
            .filter(|(_, v)| values.iter().filter(|(_, w)| w == v).count() > 1)
            .map(|(id, v)| format!("{id}={v}"))
            .collect();
        if !duplicated.is_empty() {
            failures.push(format!(
                "{} must differ between responses, got {}",
                pointer_label(ptr),
                duplicated.join(", ")
            ));
        }
    }
    failures
}

fn pointer_label(ptr: &str) -> &str {
    if ptr.is_empty() { "the body" } else { ptr }
}

/// The first unmet predicate of `check`, if any.
fn check_field(check: &JsonCheck, value: Option<&Value>) -> Option<String> {
    let name = pointer_label(&check.pointer);
    let Some(value) = value else {
        let required = check.exists == Some(true)
            || check.equals.is_some()
            || check.one_of.is_some()
            || check.min.is_some()
            || check.max.is_some();
        return required.then(|| format!("{name} is missing"));
    };
    if check.exists == Some(false) {
        return Some(format!("{name} = {value}, expected no such field"));
    }
    if let Some(expected) = &check.equals
        && !json_eq(value, expected)
    {
        return Some(format!("{name} = {value}, expected {expected}"));
    }
    if let Some(allowed) = &check.one_of
        && !allowed.iter().any(|a| json_eq(value, a))
    {
        let list = allowed
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Some(format!("{name} = {value}, expected one of [{list}]"));
    }
    if check.min.is_some() || check.max.is_some() {
        let Some(n) = value.as_f64() else {
            return Some(format!("{name} = {value}, expected a number"));
        };
        if let Some(min) = check.min.as_ref().and_then(serde_json::Number::as_f64)
            && n < min
        {
            return Some(format!(
                "{name} = {value}, expected >= {}",
                check.min.as_ref()?
            ));
        }
        if let Some(max) = check.max.as_ref().and_then(serde_json::Number::as_f64)
            && n > max
        {
            return Some(format!(
                "{name} = {value}, expected <= {}",
                check.max.as_ref()?
            ));
        }
    }
    None
}

/// Equality that treats `1` and `1.0` as the same number.
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y || x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

fn describe(o: Option<&WorkerOutcome>) -> String {
    match o {
        None => "unfinished".into(),
        Some(o) => match o.status {
            Some(s) => s.to_string(),
            None => o.label.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: i32) -> Option<WorkerOutcome> {
        json(status, "{}")
    }

    fn json(status: i32, body: &str) -> Option<WorkerOutcome> {
        Some(WorkerOutcome {
            success: (200..300).contains(&status),
            status: Some(status),
            label: format!("HTTP {status}"),
            body: Some(body.into()),
            ..WorkerOutcome::default()
        })
    }

    fn inv(max: Option<usize>, min: Option<usize>, allowed: Option<Vec<i32>>) -> ResponseInvariant {
        ResponseInvariant {
            name: "r".into(),
            description: None,
            max_2xx_count: max,
            min_2xx_count: min,
            allowed_statuses: allowed,
            workers: None,
            apply_to: ApplyTo::Success,
            json: Vec::new(),
            distinct: None,
        }
    }

    fn check(pointer: &str) -> JsonCheck {
        JsonCheck {
            pointer: pointer.into(),
            exists: None,
            equals: None,
            one_of: None,
            min: None,
            max: None,
        }
    }

    fn failure(v: &[Violation]) -> &str {
        match v {
            [Violation::InvariantFailed { description, .. }] => description,
            other => panic!("{other:?}"),
        }
    }

    fn ab() -> Vec<String> {
        vec!["req_a".into(), "req_b".into()]
    }

    #[test]
    fn json_field_predicates() {
        let mut i = inv(None, None, None);
        i.json = vec![
            JsonCheck {
                min: Some(0.into()),
                ..check("/balance")
            },
            JsonCheck {
                one_of: Some(vec!["ok".into(), "declined".into()]),
                ..check("/status")
            },
        ];
        let good = json(200, r#"{"balance": 0, "status": "ok"}"#);
        assert!(check_responses(&[i.clone()], &ab(), &[good.clone(), good.clone()]).is_empty());

        let v = check_responses(
            &[i.clone()],
            &ab(),
            &[
                json(200, r#"{"balance": -100, "status": "ok"}"#),
                json(200, r#"{"balance": 5}"#),
            ],
        );
        assert_eq!(
            failure(&v),
            "req_a: /balance = -100, expected >= 0; req_b: /status is missing"
        );

        // Error responses are out of scope unless `apply_to: all`.
        let declined = json(409, r#"{"error": "insufficient funds"}"#);
        assert!(check_responses(&[i.clone()], &ab(), &[good.clone(), declined.clone()]).is_empty());
        i.apply_to = ApplyTo::All;
        let v = check_responses(&[i], &ab(), &[good, declined]);
        assert!(
            failure(&v).starts_with("req_b: /balance is missing"),
            "{v:?}"
        );
    }

    #[test]
    fn equals_exists_and_type_errors() {
        let mut i = inv(None, None, None);
        i.json = vec![
            JsonCheck {
                equals: Some(1.into()),
                ..check("/wallet/id")
            },
            JsonCheck {
                exists: Some(false),
                ..check("/debug")
            },
            JsonCheck {
                max: Some(10.into()),
                ..check("/fee")
            },
        ];
        let ok = json(200, r#"{"wallet": {"id": 1.0}, "fee": 3}"#);
        assert!(check_responses(&[i.clone()], &ab(), &[ok.clone(), ok]).is_empty());
        let v = check_responses(
            &[i],
            &ab(),
            &[
                json(200, r#"{"wallet": {"id": 2}, "debug": true, "fee": "x"}"#),
                json(200, "not json"),
            ],
        );
        assert_eq!(
            failure(&v),
            "req_b: response body is not JSON; req_a: /wallet/id = 2, expected 1; \
             req_a: /debug = true, expected no such field; req_a: /fee = \"x\", expected a number"
        );
    }

    #[test]
    fn distinct_values_across_workers() {
        let mut i = inv(None, None, None);
        i.distinct = Some("/balance".into());
        let v = check_responses(
            &[i.clone()],
            &ab(),
            &[
                json(200, r#"{"balance": 0}"#),
                json(200, r#"{"balance": 0}"#),
            ],
        );
        assert_eq!(
            failure(&v),
            "/balance must differ between responses, got req_a=0, req_b=0"
        );
        assert!(
            check_responses(
                &[i.clone()],
                &ab(),
                &[
                    json(200, r#"{"balance": 100}"#),
                    json(200, r#"{"balance": 0}"#)
                ]
            )
            .is_empty()
        );
        i.workers = Some(vec!["req_a".into()]);
        assert!(
            check_responses(
                &[i],
                &ab(),
                &[
                    json(200, r#"{"balance": 0}"#),
                    json(200, r#"{"balance": 0}"#)
                ]
            )
            .is_empty()
        );
    }

    #[test]
    fn max_2xx_matches_the_reference_message() {
        let ids = vec!["req_a".to_string(), "req_b".to_string()];
        let v = check_responses(&[inv(Some(1), None, None)], &ids, &[http(200), http(200)]);
        let [Violation::InvariantFailed { description, .. }] = v.as_slice() else {
            panic!("{v:?}");
        };
        assert_eq!(
            description,
            "expected <= 1 HTTP 2xx, got 2 (req_a=200, req_b=200)"
        );
        assert!(
            check_responses(&[inv(Some(1), None, None)], &ids, &[http(200), http(409)]).is_empty()
        );
    }

    #[test]
    fn min_and_allowed_statuses() {
        let ids = vec!["a".to_string(), "b".to_string()];
        let v = check_responses(
            &[inv(None, Some(1), Some(vec![200, 409]))],
            &ids,
            &[http(500), None],
        );
        let [Violation::InvariantFailed { description, .. }] = v.as_slice() else {
            panic!("{v:?}");
        };
        assert!(
            description.contains("expected >= 1 HTTP 2xx, got 0"),
            "{description}"
        );
        assert!(
            description.contains("status outside [200, 409]: a=500, b=unfinished"),
            "{description}"
        );
    }

    #[test]
    fn sql_workers_use_neutral_wording() {
        let ok = Some(WorkerOutcome {
            success: true,
            label: "ok".into(),
            ..WorkerOutcome::default()
        });
        let ids = vec!["a".to_string(), "b".to_string()];
        let v = check_responses(&[inv(Some(1), None, None)], &ids, &[ok.clone(), ok]);
        assert!(
            matches!(&v[0], Violation::InvariantFailed { description, .. }
                if description == "expected <= 1 successful workers, got 2 (a=ok, b=ok)")
        );
    }
}
