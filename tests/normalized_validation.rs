mod common;

use common::anchor_package;
use fossil::normalized::read_package;

#[test]
fn block_end_is_mandatory_and_unique() {
    let package = String::from_utf8(anchor_package()).unwrap();
    let first_end = package
        .lines()
        .find(|line| line.contains("\"block_end\"") && line.contains("\"0xa\""))
        .unwrap();

    let missing_between_blocks = package.replacen(&format!("{first_end}\n"), "", 1);
    assert!(read_package(missing_between_blocks.as_bytes())
        .unwrap_err()
        .to_string()
        .contains("block_end is required before the next block"));

    let last_end = package
        .lines()
        .find(|line| line.contains("\"block_end\"") && line.contains("\"0xc\""))
        .unwrap();
    let missing_before_trailer = package.replacen(&format!("{last_end}\n"), "", 1);
    assert!(read_package(missing_before_trailer.as_bytes())
        .unwrap_err()
        .to_string()
        .contains("block_end is required before the trailer"));

    let duplicated = package.replacen(first_end, &format!("{first_end}\n{first_end}"), 1);
    assert!(read_package(duplicated.as_bytes()).is_err());
}

#[test]
fn unsupported_block_event_digest_is_rejected() {
    let package = String::from_utf8(anchor_package()).unwrap();
    let malformed = package.replacen(
        "\"event_count\":\"0x3\"",
        &format!(
            "\"event_count\":\"0x3\",\"events_sha256\":\"{}\"",
            common::hash(42)
        ),
        1,
    );
    let error = read_package(malformed.as_bytes()).unwrap_err();
    assert!(error.to_string().contains("invalid JSONL record"));
}
