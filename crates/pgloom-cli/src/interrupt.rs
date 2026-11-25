//! Ctrl+C handling. The first interrupt lets the schedule in progress finish,
//! so its transactions end and the database is restored, then reports what
//! was explored so far. A second interrupt aborts immediately.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

static REQUESTED: AtomicBool = AtomicBool::new(false);

fn notify() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

pub fn request() {
    REQUESTED.store(true, Ordering::SeqCst);
    notify().notify_waiters();
}

pub fn requested() -> bool {
    REQUESTED.load(Ordering::SeqCst)
}

/// Clears a prior interrupt flag.
pub fn reset() {
    REQUESTED.store(false, Ordering::SeqCst);
}

/// Resolves once an interrupt has been requested.
pub async fn wait() {
    loop {
        // Created before the check: `notify_waiters` wakes every `Notified`
        // that exists at the time of the call, polled or not.
        let notified = notify().notified();
        if requested() {
            return;
        }
        notified.await;
    }
}
