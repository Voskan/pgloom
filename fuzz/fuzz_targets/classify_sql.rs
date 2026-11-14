#![no_main]

use libfuzzer_sys::fuzz_target;
use pgloom_wire::classify::is_copy_sql;
use pgloom_wire::{classify_access, classify_sql};

fuzz_target!(|data: &[u8]| {
    let sql = String::from_utf8_lossy(data);
    let meta = classify_sql(&sql);
    let _ = meta.kind();
    let _ = meta.read_tables();
    let _ = meta.write_tables();
    let _ = meta.row_lock();
    let _ = classify_access(&sql);
    let _ = is_copy_sql(&sql);
});
