#![allow(dead_code)]

use fossil::format::Hash32;
use sha3::{Digest, Keccak256};

pub const ADDRESS: &str = "0x1111111111111111111111111111111111111111";
pub const SLOT_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const SLOT_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const VALUE_A: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
pub const VALUE_B: &str = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

pub fn hash(byte: u8) -> String {
    format!("0x{}", hex::encode([byte; 32]))
}

pub fn anchor_package() -> Vec<u8> {
    let code = [0x60_u8, 0x00];
    let code_hash = Hash32(Keccak256::digest(code).into()).to_string();
    let empty_hash = Hash32(Keccak256::digest([]).into()).to_string();
    format!(
        concat!(
            "{{\"type\":\"header\",\"schema\":\"fossil-export/1\",\"chain_id\":\"0x1\",\"genesis_hash\":\"{}\",\"mode\":\"anchor\",\"anchor_number\":\"0xa\",\"anchor_hash\":\"{}\"}}\n",
            "{{\"type\":\"block\",\"number\":\"0xa\",\"hash\":\"{}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x64\"}}\n",
            "{{\"type\":\"account\",\"block\":\"0xa\",\"address\":\"{}\",\"exists\":true,\"incarnation\":\"0x0\",\"nonce\":\"0x1\",\"balance\":\"0x64\",\"code_hash\":\"{}\"}}\n",
            "{{\"type\":\"storage\",\"block\":\"0xa\",\"address\":\"{}\",\"incarnation\":\"0x0\",\"slot\":\"{}\",\"value\":\"{}\"}}\n",
            "{{\"type\":\"code\",\"code_hash\":\"{}\",\"bytes\":\"0x6000\"}}\n",
            "{{\"type\":\"block_end\",\"number\":\"0xa\",\"event_count\":\"0x3\"}}\n",
            "{{\"type\":\"block\",\"number\":\"0xb\",\"hash\":\"{}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x65\"}}\n",
            "{{\"type\":\"account\",\"block\":\"0xb\",\"address\":\"{}\",\"exists\":false,\"incarnation\":\"0x0\"}}\n",
            "{{\"type\":\"storage\",\"block\":\"0xb\",\"address\":\"{}\",\"incarnation\":\"0x0\",\"slot\":\"{}\",\"value\":\"{}\"}}\n",
            "{{\"type\":\"block_end\",\"number\":\"0xb\",\"event_count\":\"0x2\"}}\n",
            "{{\"type\":\"block\",\"number\":\"0xc\",\"hash\":\"{}\",\"parent_hash\":\"{}\",\"state_root\":\"{}\",\"timestamp\":\"0x66\"}}\n",
            "{{\"type\":\"account\",\"block\":\"0xc\",\"address\":\"{}\",\"exists\":true,\"incarnation\":\"0x1\",\"nonce\":\"0x2\",\"balance\":\"0xc8\",\"code_hash\":\"{}\"}}\n",
            "{{\"type\":\"storage\",\"block\":\"0xc\",\"address\":\"{}\",\"incarnation\":\"0x1\",\"slot\":\"{}\",\"value\":\"{}\"}}\n",
            "{{\"type\":\"block_end\",\"number\":\"0xc\",\"event_count\":\"0x2\"}}\n",
            "{{\"type\":\"trailer\",\"end_number\":\"0xc\",\"end_hash\":\"{}\",\"block_count\":\"0x3\"}}\n"
        ),
        hash(1),
        hash(10),
        hash(10),
        hash(9),
        hash(110),
        ADDRESS,
        code_hash,
        ADDRESS,
        SLOT_A,
        VALUE_A,
        code_hash,
        hash(11),
        hash(10),
        hash(111),
        ADDRESS,
        ADDRESS,
        SLOT_A,
        hash(0),
        hash(12),
        hash(11),
        hash(112),
        ADDRESS,
        empty_hash,
        ADDRESS,
        SLOT_B,
        VALUE_B,
        hash(12),
    )
    .into_bytes()
}
