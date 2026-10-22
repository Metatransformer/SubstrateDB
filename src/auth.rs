use crate::model::{Error, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capability {
    pub id: String,
    pub namespace: String,
    pub partition: String,
    pub resources: Vec<String>,
    pub permissions: Vec<String>,
    pub expires_at_ms: u64,
    #[serde(default)]
    pub filter: Value,
}
pub fn sign(key: &[u8], claims: &Capability) -> Result<String> {
    let encoded = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(claims).map_err(|e| Error::new("invalid_argument", e.to_string()))?,
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| Error::new("internal", "invalid signing key"))?;
    mac.update(b"substrate-capability-v1.");
    mac.update(encoded.as_bytes());
    Ok(format!(
        "{encoded}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    ))
}
pub fn verify(key: &[u8], token: &str, now: u64) -> Result<Capability> {
    let denied = || Error::new("unauthorized", "invalid or expired capability");
    if token.len() > 16384 {
        return Err(denied());
    }
    let (payload, sig) = token.split_once('.').ok_or_else(denied)?;
    let signature = URL_SAFE_NO_PAD.decode(sig).map_err(|_| denied())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| denied())?;
    mac.update(b"substrate-capability-v1.");
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature).map_err(|_| denied())?;
    let claims: Capability =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).map_err(|_| denied())?)
            .map_err(|_| denied())?;
    if claims.expires_at_ms <= now
        || claims.namespace.is_empty()
        || claims.partition.is_empty()
        || claims.id.is_empty()
    {
        return Err(denied());
    }
    Ok(claims)
}
pub fn permits(c: &Capability, permission: &str, resource: &str) -> bool {
    c.permissions.iter().any(|p| p == permission) && c.resources.iter().any(|r| r == resource)
}
pub fn signature(key: &[u8], timestamp: u64, job_id: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(job_id.as_bytes());
    mac.update(b".");
    mac.update(body);
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tampering_expiry_and_scope() {
        let c = Capability {
            id: "a".into(),
            namespace: "n".into(),
            partition: "p".into(),
            resources: vec!["orders".into()],
            permissions: vec!["subscribe".into()],
            expires_at_ms: 20,
            filter: Value::Null,
        };
        let token = sign(b"secret", &c).unwrap();
        assert!(verify(b"secret", &token, 10).is_ok());
        assert!(verify(b"other", &token, 10).is_err());
        assert!(verify(b"secret", &token, 20).is_err());
        assert!(permits(&c, "subscribe", "orders"));
        assert!(!permits(&c, "publish", "orders"));
    }
}
