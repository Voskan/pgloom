#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pgloom_wire::frame::{decode_frame, decode_startup};
use pgloom_wire::messages::{
    BackendKeyData, BindMessage, ErrorFields, ExecuteMessage, FormatCode, ObjectRef, ParseMessage,
    decode_command_complete, decode_data_row, decode_query, decode_ready_for_query,
    decode_row_description,
};
use pgloom_wire::response::ResponseCollector;
use pgloom_wire::turn::TurnAssembler;
use pgloom_wire::values::render_value;

fuzz_target!(|data: &[u8]| {
    let mut startup = BytesMut::from(data);
    let _ = decode_startup(&mut startup);

    let mut buf = BytesMut::from(data);
    let mut assembler = TurnAssembler::new();
    let mut collector = ResponseCollector::new();

    while let Ok(Some(frame)) = decode_frame(&mut buf) {
        let _ = ParseMessage::decode(&frame);
        let _ = BindMessage::decode(&frame);
        let _ = ExecuteMessage::decode(&frame);
        let _ = ObjectRef::decode(&frame);
        let _ = BackendKeyData::decode(&frame);
        let _ = ErrorFields::decode(&frame);
        let _ = decode_query(&frame);
        let _ = decode_command_complete(&frame);
        let _ = decode_ready_for_query(&frame);
        let _ = decode_row_description(&frame);
        let _ = decode_data_row(&frame);

        let oid = u32::from(frame.tag);
        let _ = render_value(Some(&frame.body), FormatCode::Text, oid);
        let _ = render_value(Some(&frame.body), FormatCode::Binary, oid);

        let _ = assembler.push(frame.clone());
        let _ = collector.observe(&frame);
    }
    let _ = collector.finish().summary();
});
