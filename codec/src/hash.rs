use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, str::FromStr};

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct Hash32(pub [u8; 32]);

impl Hash32 {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn object_key(self) -> String {
        let hex = hex::encode(self.0);
        format!("objects/sha256/{}/{}", &hex[..2], hex)
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl From<Hash32> for String {
    fn from(value: Hash32) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for Hash32 {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}

impl FromStr for Hash32 {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        let raw = value
            .strip_prefix("0x")
            .ok_or_else(|| anyhow!("fixed hex value must have a 0x prefix"))?;
        if raw.len() != 64 || raw.bytes().any(|c| c.is_ascii_uppercase()) {
            bail!("expected exactly 32 lowercase hex bytes");
        }
        let decoded = hex::decode(raw).context("invalid hex")?;
        Ok(Self(
            decoded
                .try_into()
                .map_err(|_| anyhow!("expected exactly 32 bytes"))?,
        ))
    }
}
