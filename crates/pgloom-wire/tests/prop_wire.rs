//! Property-based fuzzing of `pgloom-wire` frame codecs, message decoders,
//! value renderers, turn/response state machines, and the SQL classifier.

use bytes::BytesMut;
use pgloom_wire::classify::is_copy_sql;
use pgloom_wire::frame::{
    CANCEL_REQUEST_CODE, Frame, GSSENC_REQUEST_CODE, PROTOCOL_V3, SSL_REQUEST_CODE, StartupPacket,
    decode_frame, decode_startup, encode_startup,
};
use pgloom_wire::messages::{
    BackendKeyData, BindMessage, ErrorFields, ExecuteMessage, FormatCode, ObjectRef, ParseMessage,
    decode_command_complete, decode_data_row, decode_query, decode_ready_for_query,
    decode_row_description,
};
use pgloom_wire::response::ResponseCollector;
use pgloom_wire::turn::TurnAssembler;
use pgloom_wire::values::render_value;
use pgloom_wire::{classify_access, classify_sql};
use proptest::prelude::*;

fn arb_sql_soup() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just("SELECT ".to_owned()),
            Just("UPDATE ".to_owned()),
            Just("INSERT INTO ".to_owned()),
            Just("DELETE FROM ".to_owned()),
            Just("MERGE INTO ".to_owned()),
            Just("COPY ".to_owned()),
            Just("WITH cte AS (".to_owned()),
            Just(" FROM ".to_owned()),
            Just(" WHERE ".to_owned()),
            Just(" SET ".to_owned()),
            Just(" FOR NO KEY UPDATE".to_owned()),
            Just(" ON CONFLICT DO NOTHING ".to_owned()),
            Just(" RETURNING *".to_owned()),
            Just("\"Wallet,USD\"".to_owned()),
            Just("\"a\"\"b\"".to_owned()),
            Just("'o''hare'".to_owned()),
            Just("E'it\\'s\\\\ok'".to_owned()),
            Just("$$dollar;SELECT$$".to_owned()),
            Just("$tag$inner$tag$".to_owned()),
            Just("/* nested /* comment */ */".to_owned()),
            Just("-- line comment\n".to_owned()),
            Just("$1::numeric(10, 2)".to_owned()),
            Just("CAST($2 AS integer)".to_owned()),
            Just(";".to_owned()),
            Just("(".to_owned()),
            Just(")".to_owned()),
            "[\\PC]{0,12}".prop_map(|s| s),
        ],
        0..24,
    )
    .prop_map(|parts| parts.concat())
}

fn arb_orm_query() -> impl Strategy<Value = String> {
    let comment = prop_oneof![
        Just(String::new()),
        Just("/* prisma:model=Wallet,action=update */ ".to_owned()),
        Just("/* ApplicationName=Hibernate */ -- trace=1\n".to_owned()),
        Just("-- django-orm query\n".to_owned()),
    ];
    let schema = prop_oneof![
        Just(String::new()),
        Just("public.".to_owned()),
        Just("\"public\".".to_owned()),
    ];
    let table = prop_oneof![
        Just("wallets".to_owned()),
        Just("\"Wallet\"".to_owned()),
        Just("\"user_accounts\"".to_owned()),
        Just("\"Order,Item\"".to_owned()),
    ];
    let kind = prop_oneof![
        Just(0u8), // SELECT
        Just(1u8), // UPDATE
        Just(2u8), // INSERT
        Just(3u8), // DELETE
        Just(4u8), // COPY
    ];
    (comment, schema, table, kind).prop_map(|(c, s, t, k)| match k {
        0 => format!(
            "{c}SELECT \"t0\".\"id\", \"t0\".\"balance\" FROM {s}{t} AS \"t0\" WHERE \"t0\".\"id\" = $1::INTEGER"
        ),
        1 => format!(
            "{c}UPDATE {s}{t} SET \"balance\" = CAST($1 AS numeric(18, 2)), \"note\" = E'a\\'b' WHERE \"id\" = $2 RETURNING \"id\""
        ),
        2 => format!(
            "{c}INSERT INTO {s}{t} (\"id\", \"balance\") VALUES ($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"balance\" = EXCLUDED.\"balance\""
        ),
        3 => format!("{c}DELETE FROM {s}{t} WHERE \"id\" = $1"),
        _ => format!("{c}COPY {s}{t} (\"id\", \"balance\") FROM STDIN"),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn decode_frame_never_panics_on_arbitrary_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
        chunk in 1usize..64,
    ) {
        let mut whole = BytesMut::from(bytes.as_slice());
        while let Ok(Some(_)) = decode_frame(&mut whole) {}

        let mut streamed = BytesMut::new();
        for piece in bytes.chunks(chunk) {
            streamed.extend_from_slice(piece);
            while let Ok(Some(_)) = decode_frame(&mut streamed) {}
        }
    }

    #[test]
    fn frame_encode_decode_roundtrips_across_arbitrary_chunks(
        raw_frames in prop::collection::vec(
            (any::<u8>(), prop::collection::vec(any::<u8>(), 0..128)),
            0..16,
        ),
        chunks in prop::collection::vec(1usize..37, 1..16),
    ) {
        let expected: Vec<Frame> = raw_frames
            .into_iter()
            .map(|(tag, body)| Frame::new(tag, body))
            .collect();

        let mut wire = BytesMut::new();
        for f in &expected {
            f.encode(&mut wire);
        }

        let mut decoded = Vec::new();
        let mut buf = BytesMut::new();
        let mut pos = 0;
        let mut ci = 0;
        while pos < wire.len() {
            let step = chunks[ci % chunks.len()].min(wire.len() - pos);
            buf.extend_from_slice(&wire[pos..pos + step]);
            pos += step;
            ci += 1;
            while let Some(f) = decode_frame(&mut buf).unwrap() {
                decoded.push(f);
            }
        }
        prop_assert!(buf.is_empty());
        prop_assert_eq!(decoded, expected);
    }

    #[test]
    fn decode_startup_never_panics_and_roundtrips_params(
        random_bytes in prop::collection::vec(any::<u8>(), 0..512),
        code in prop_oneof![
            Just(PROTOCOL_V3),
            Just(SSL_REQUEST_CODE),
            Just(GSSENC_REQUEST_CODE),
            Just(CANCEL_REQUEST_CODE),
            any::<i32>(),
        ],
        tail in prop::collection::vec(any::<u8>(), 0..96),
        kv in prop::collection::vec(("[a-z_]{1,12}", "[a-zA-Z0-9_./:-]{0,20}"), 0..6),
    ) {
        let mut raw = BytesMut::from(random_bytes.as_slice());
        let _ = decode_startup(&mut raw);

        let total_len = (8 + tail.len()) as i32;
        let mut crafted = BytesMut::new();
        crafted.extend_from_slice(&total_len.to_be_bytes());
        crafted.extend_from_slice(&code.to_be_bytes());
        crafted.extend_from_slice(&tail);
        let _ = decode_startup(&mut crafted);

        let refs: Vec<(&str, &str)> = kv.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let mut encoded = BytesMut::from(&encode_startup(&refs)[..]);
        match decode_startup(&mut encoded).unwrap() {
            Some(StartupPacket::Startup { params, .. }) => {
                let expected: Vec<(String, String)> = kv;
                prop_assert_eq!(params, expected);
            }
            other => prop_assert!(false, "expected Startup, got {other:?}"),
        }
    }

    #[test]
    fn message_decoders_assembler_collector_and_values_never_panic(
        frames in prop::collection::vec(
            (any::<u8>(), prop::collection::vec(any::<u8>(), 0..96)),
            0..24,
        ),
        oid in prop_oneof![
            Just(0u32),
            Just(16u32),
            Just(20u32),
            Just(21u32),
            Just(23u32),
            Just(25u32),
            Just(700u32),
            Just(701u32),
            Just(1082u32),
            Just(1114u32),
            Just(1184u32),
            Just(1700u32),
            Just(2950u32),
            any::<u32>(),
        ],
    ) {
        let mut asm = TurnAssembler::new();
        let mut collector = ResponseCollector::new();

        for (tag, body) in frames {
            let _ = render_value(Some(&body), FormatCode::Text, oid);
            let _ = render_value(Some(&body), FormatCode::Binary, oid);

            let f = Frame::new(tag, body);
            let _ = ParseMessage::decode(&f);
            let _ = BindMessage::decode(&f);
            let _ = ExecuteMessage::decode(&f);
            let _ = ObjectRef::decode(&f);
            let _ = BackendKeyData::decode(&f);
            let _ = ErrorFields::decode(&f);
            let _ = decode_query(&f);
            let _ = decode_command_complete(&f);
            let _ = decode_ready_for_query(&f);
            let _ = decode_row_description(&f);
            let _ = decode_data_row(&f);

            let _ = asm.push(f.clone());
            let _ = collector.observe(&f);
        }
        let _ = collector.finish().summary();
    }

    #[test]
    fn classify_sql_never_panics_on_arbitrary_or_soup_inputs(
        arbitrary in "\\PC{0,240}",
        soup in arb_sql_soup(),
    ) {
        for sql in [&arbitrary, &soup] {
            let meta = classify_sql(sql);
            prop_assert!(!meta.statements.is_empty());
            let _ = meta.kind();
            let _ = meta.read_tables();
            let _ = meta.write_tables();
            let _ = meta.row_lock();
            let _ = classify_access(sql);
            let _ = is_copy_sql(sql);
        }
    }

    #[test]
    fn classify_sql_handles_wild_orm_grammars(sql in arb_orm_query()) {
        let meta = classify_sql(&sql);
        prop_assert!(!meta.is_noise(), "ORM data statement classified as noise: {sql}");
        let accesses = classify_access(&sql);
        prop_assert_eq!(accesses.len(), 1);
    }
}
