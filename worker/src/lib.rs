pub mod core;

use core::{ArchiveError, ObjectStore, Reader};
use futures_util::StreamExt;
use serde_json::{json, Value};
use worker::{
    event, Bucket, Env, Headers, Method, Range, Request, Response, ResponseBuilder,
    Result as WorkerResult,
};

const MAX_RPC_BODY_BYTES: usize = 16 * 1024;
const IMMUTABLE_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

#[derive(Clone)]
struct R2Store(Bucket);
impl ObjectStore for R2Store {
    async fn get(&self, key: &str, maximum: usize) -> core::Result<Vec<u8>> {
        let object = self
            .0
            .get(key)
            .execute()
            .await
            .map_err(|_| ArchiveError::Backend)?
            .ok_or_else(|| ArchiveError::Unavailable("archive object is unavailable".into()))?;
        if object.size() > maximum as u64 {
            return Err(ArchiveError::Integrity);
        }
        let expected = object.size() as usize;
        let mut stream = object
            .body()
            .ok_or(ArchiveError::Integrity)?
            .stream()
            .map_err(|_| ArchiveError::Backend)?;
        let mut bytes = Vec::with_capacity(expected);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| ArchiveError::Backend)?;
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|length| length > maximum)
            {
                return Err(ArchiveError::Integrity);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != expected {
            return Err(ArchiveError::Integrity);
        }
        Ok(bytes)
    }
}

#[event(fetch)]
pub async fn main(mut request: Request, env: Env, _ctx: worker::Context) -> WorkerResult<Response> {
    let path = request.path();
    if (path == "/" || path == "/rpc") && request.method() == Method::Post {
        return serve_rpc(&mut request, &env).await;
    }
    if path == "/rpc" {
        return text_response(
            &request,
            405,
            "Method not allowed.\n",
            Some(("allow", "POST")),
        );
    }
    if request.method() != Method::Get && request.method() != Method::Head {
        return text_response(
            &request,
            405,
            "Method not allowed.\n",
            Some(("allow", "GET, HEAD")),
        );
    }
    if path == "/health" {
        let mut response = text_response(&request, 200, "{\"status\":\"ok\"}\n", None)?;
        response
            .headers_mut()
            .set("content-type", "application/json; charset=utf-8")?;
        return Ok(response);
    }
    if path == "/objects" || path == "/objects/" {
        return text_response(
            &request,
            400,
            "Object listings and empty keys are not supported.\n",
            None,
        );
    }
    if let Some(encoded) = path.strip_prefix("/objects/") {
        let key = match decode_cas_key(encoded) {
            Some(key) => key,
            None => return text_response(&request, 400, "Invalid immutable object key.\n", None),
        };
        return match serve_object(&request, &env, &key).await {
            Ok(response) => Ok(response),
            Err(_) => text_response(&request, 500, "Archive request failed.\n", None),
        };
    }
    text_response(&request, 404, "Not found.\n", None)
}

async fn serve_rpc(request: &mut Request, env: &Env) -> WorkerResult<Response> {
    let body = match read_bounded_body(request).await {
        Ok(body) => body,
        Err(error) => return rpc_error(Value::Null, error),
    };
    let message: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return rpc_error_code(Value::Null, -32700, "Parse error", None),
    };
    if message.is_array() {
        return rpc_error_code(
            Value::Null,
            -32600,
            "Invalid Request",
            Some("Batch requests are not supported"),
        );
    }
    let Some(object) = message.as_object() else {
        return rpc_error_code(Value::Null, -32600, "Invalid Request", None);
    };
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc") != Some(&Value::String("2.0".into()))
        || !object.contains_key("id")
        || !valid_id(&id)
        || object.get("method").and_then(Value::as_str).is_none()
    {
        return rpc_error_code(
            if valid_id(&id) { id } else { Value::Null },
            -32600,
            "Invalid Request",
            None,
        );
    }
    let method = object["method"].as_str().unwrap();
    let params = object.get("params").cloned().unwrap_or_else(|| json!([]));
    match dispatch_rpc(method, &params, env).await {
        Ok(result) => json_response(json!({"jsonrpc":"2.0", "id":id, "result":result})),
        Err(DispatchError::MethodNotFound) => rpc_error_code(id, -32601, "Method not found", None),
        Err(DispatchError::Archive(error)) => rpc_error(id, error),
    }
}

enum DispatchError {
    MethodNotFound,
    Archive(ArchiveError),
}
impl From<ArchiveError> for DispatchError {
    fn from(value: ArchiveError) -> Self {
        Self::Archive(value)
    }
}

async fn dispatch_rpc(
    method: &str,
    params: &Value,
    env: &Env,
) -> std::result::Result<Value, DispatchError> {
    if method == "web3_clientVersion" {
        require_params(params, 0)?;
        return Ok(json!("fossil-worker-rs/0.1.0"));
    }
    if !matches!(
        method,
        "eth_chainId"
            | "eth_blockNumber"
            | "eth_getBalance"
            | "eth_getTransactionCount"
            | "eth_getCode"
            | "eth_getStorageAt"
    ) {
        return Err(DispatchError::MethodNotFound);
    }
    let chain_id = configured_chain_id(env)?;
    if method == "eth_chainId" {
        require_params(params, 0)?;
        return Ok(json!(core::quantity(chain_id)));
    }
    let prefix = archive_prefix(env)?;
    let store = R2Store(env.bucket("ARCHIVE").map_err(|_| ArchiveError::Backend)?);
    let mut reader = Reader::load(&store, prefix, chain_id).await?;
    if method == "eth_blockNumber" {
        require_params(params, 0)?;
        return Ok(json!(core::quantity(reader.commit.published_number)));
    }
    let values = params
        .as_array()
        .ok_or_else(|| ArchiveError::InvalidParams("params must be an array".into()))?;
    if method == "eth_getStorageAt" {
        if values.len() != 3 {
            return Err(ArchiveError::InvalidParams("expected 3 parameters".into()).into());
        }
        let address = core::parse_address(&values[0])?;
        let slot = core::parse_slot(&values[1])?;
        let block = reader.resolve_selector(&values[2])?;
        return Ok(json!(format!(
            "0x{}",
            hex::encode(reader.storage_at(address, slot, block).await?)
        )));
    }
    if values.len() != 2 {
        return Err(ArchiveError::InvalidParams("expected 2 parameters".into()).into());
    }
    let address = core::parse_address(&values[0])?;
    let block = reader.resolve_selector(&values[1])?;
    if method == "eth_getCode" {
        return Ok(json!(format!(
            "0x{}",
            hex::encode(reader.code_at(address, block).await?)
        )));
    }
    let account = reader.account_at(address, block).await?;
    if method == "eth_getBalance" {
        return Ok(json!(account
            .map(|a| core::quantity_bytes(&a.balance))
            .unwrap_or_else(|| "0x0".into())));
    }
    Ok(json!(account
        .map(|a| core::quantity(a.nonce))
        .unwrap_or_else(|| "0x0".into())))
}

fn require_params(params: &Value, expected: usize) -> core::Result<()> {
    if params
        .as_array()
        .is_some_and(|values| values.len() == expected)
    {
        Ok(())
    } else {
        Err(ArchiveError::InvalidParams(format!(
            "expected {expected} parameters"
        )))
    }
}
fn configured_chain_id(env: &Env) -> core::Result<u64> {
    let text = env
        .var("CHAIN_ID")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "8453".into());
    parse_configured_chain_id(&text)
}
fn parse_configured_chain_id(text: &str) -> core::Result<u64> {
    if text.is_empty()
        || text == "0"
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ArchiveError::Backend);
    }
    text.parse().map_err(|_| ArchiveError::Backend)
}
fn archive_prefix(env: &Env) -> core::Result<String> {
    let mut prefix = env
        .var("IMMUTABLE_PREFIX")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "v1/".into());
    if prefix.starts_with('/')
        || prefix.contains(['\\', '\0'])
        || prefix.split('/').any(|part| part == "." || part == "..")
    {
        return Err(ArchiveError::Backend);
    }
    if !prefix.ends_with('/') {
        prefix.push('/');
    }
    Ok(prefix)
}

async fn read_bounded_body(request: &mut Request) -> core::Result<Vec<u8>> {
    if let Some(length) = request
        .headers()
        .get("content-length")
        .map_err(|_| ArchiveError::Backend)?
    {
        if length
            .parse::<usize>()
            .map_or(true, |value| value > MAX_RPC_BODY_BYTES)
        {
            return Err(ArchiveError::Limit);
        }
    }
    let mut stream = match request.stream() {
        Ok(stream) => stream,
        Err(_) => return Ok(Vec::new()),
    };
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ArchiveError::Backend)?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > MAX_RPC_BODY_BYTES)
        {
            return Err(ArchiveError::Limit);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
fn valid_id(id: &Value) -> bool {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    id.is_null()
        || id.is_string()
        || id.as_u64().is_some_and(|value| value <= MAX_SAFE_INTEGER)
        || id
            .as_i64()
            .is_some_and(|value| value.unsigned_abs() <= MAX_SAFE_INTEGER)
}
fn rpc_error(id: Value, error: ArchiveError) -> WorkerResult<Response> {
    match error {
        ArchiveError::InvalidParams(data) => {
            rpc_error_code(id, -32602, "Invalid params", Some(&data))
        }
        ArchiveError::Unavailable(data) => {
            rpc_error_code(id, -32001, "Archive unavailable", Some(&data))
        }
        ArchiveError::Integrity => rpc_error_code(id, -32002, "Archive integrity error", None),
        ArchiveError::Limit => rpc_error_code(id, -32005, "Resource limit exceeded", None),
        ArchiveError::Backend => rpc_error_code(id, -32603, "Internal error", None),
    }
}
fn rpc_error_code(
    id: Value,
    code: i32,
    message: &str,
    data: Option<&str>,
) -> WorkerResult<Response> {
    let mut error = json!({"code":code, "message":message});
    if let Some(data) = data {
        error["data"] = json!(data);
    }
    json_response(json!({"jsonrpc":"2.0", "id":id, "error":error}))
}
fn json_response(value: Value) -> WorkerResult<Response> {
    let mut bytes =
        serde_json::to_vec(&value).map_err(|e| worker::Error::RustError(e.to_string()))?;
    bytes.push(b'\n');
    let headers = base_headers()?;
    headers.set("cache-control", "no-store")?;
    headers.set("content-type", "application/json; charset=utf-8")?;
    headers.set("content-length", &bytes.len().to_string())?;
    Ok(ResponseBuilder::new().with_headers(headers).fixed(bytes))
}

async fn serve_object(request: &Request, env: &Env, relative: &str) -> WorkerResult<Response> {
    let bucket = env.bucket("ARCHIVE")?;
    let key = format!(
        "{}{}",
        archive_prefix(env).map_err(|_| worker::Error::RustError("invalid prefix".into()))?,
        relative
    );
    let metadata = match bucket.head(&key).await? {
        Some(object) => object,
        None => return text_response(request, 404, "Object not found.\n", None),
    };
    let etag = metadata.http_etag();
    if if_none_match(request.headers().get("if-none-match")?.as_deref(), &etag) {
        let headers = object_headers(&metadata, None)?;
        return Ok(ResponseBuilder::new()
            .with_status(304)
            .with_headers(headers)
            .empty());
    }
    if request.method() == Method::Head {
        let headers = object_headers(&metadata, Some(metadata.size()))?;
        return Ok(ResponseBuilder::new().with_headers(headers).empty());
    }
    let range_header = request.headers().get("range")?;
    let use_range = range_header.is_some()
        && if_range_matches(request.headers().get("if-range")?.as_deref(), &etag);
    if use_range {
        match parse_range(range_header.as_deref().unwrap(), metadata.size()) {
            RangeRequest::Unsatisfiable => {
                let headers = object_headers(&metadata, Some(0))?;
                headers.set("content-range", &format!("bytes */{}", metadata.size()))?;
                headers.set("cache-control", "no-store")?;
                return Ok(ResponseBuilder::new()
                    .with_status(416)
                    .with_headers(headers)
                    .empty());
            }
            RangeRequest::Satisfiable { offset, length } => {
                let object = bucket
                    .get(&key)
                    .range(Range::OffsetWithLength { offset, length })
                    .execute()
                    .await?
                    .ok_or_else(|| worker::Error::RustError("object disappeared".into()))?;
                let body = object
                    .body()
                    .ok_or_else(|| worker::Error::RustError("missing body".into()))?
                    .response_body()?;
                let headers = object_headers(&metadata, Some(length))?;
                headers.set(
                    "content-range",
                    &format!("bytes {offset}-{}/{}", offset + length - 1, metadata.size()),
                )?;
                return Ok(ResponseBuilder::new()
                    .with_status(206)
                    .with_headers(headers)
                    .body(body));
            }
            RangeRequest::Ignore => {}
        }
    }
    let object = bucket
        .get(&key)
        .execute()
        .await?
        .ok_or_else(|| worker::Error::RustError("object disappeared".into()))?;
    let body = object
        .body()
        .ok_or_else(|| worker::Error::RustError("missing body".into()))?
        .response_body()?;
    let headers = object_headers(&object, Some(object.size()))?;
    Ok(ResponseBuilder::new().with_headers(headers).body(body))
}
fn object_headers(object: &worker::Object, length: Option<u64>) -> WorkerResult<Headers> {
    let headers = base_headers()?;
    object.write_http_metadata(Headers(headers.0.clone()))?;
    // R2 metadata is retained, then trusted policy fields override cache/security values.
    headers.set("accept-ranges", "bytes")?;
    headers.set("cache-control", IMMUTABLE_CACHE_CONTROL)?;
    headers.set("etag", &object.http_etag())?;
    if let Some(length) = length {
        headers.set("content-length", &length.to_string())?;
    }
    Ok(headers)
}
fn if_none_match(value: Option<&str>, etag: &str) -> bool {
    value.is_some_and(|value| {
        value.split(',').any(|item| {
            let item = item.trim().strip_prefix("W/").unwrap_or(item.trim());
            item == "*" || item == etag
        })
    })
}
fn if_range_matches(value: Option<&str>, etag: &str) -> bool {
    value.is_none_or(|value| value == etag && value.starts_with('"') && value.ends_with('"'))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeRequest {
    Ignore,
    Unsatisfiable,
    Satisfiable { offset: u64, length: u64 },
}

fn parse_range(value: &str, size: u64) -> RangeRequest {
    let Some(value) = value.strip_prefix("bytes=") else {
        return RangeRequest::Ignore;
    };
    if value.contains(',') {
        return RangeRequest::Ignore;
    }
    let Some((start, end)) = value.split_once('-') else {
        return RangeRequest::Ignore;
    };
    if start.is_empty() {
        if end.is_empty() || !end.bytes().all(|byte| byte.is_ascii_digit()) {
            return RangeRequest::Ignore;
        }
        let Ok(suffix) = end.parse::<u64>() else {
            return RangeRequest::Ignore;
        };
        if suffix == 0 || size == 0 {
            return RangeRequest::Unsatisfiable;
        }
        let length = suffix.min(size);
        return RangeRequest::Satisfiable {
            offset: size - length,
            length,
        };
    }
    if !start.bytes().all(|byte| byte.is_ascii_digit())
        || (!end.is_empty() && !end.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return RangeRequest::Ignore;
    }
    let Ok(offset) = start.parse::<u64>() else {
        return RangeRequest::Ignore;
    };
    let parsed_end = if end.is_empty() {
        None
    } else {
        let Ok(end) = end.parse::<u64>() else {
            return RangeRequest::Ignore;
        };
        Some(end)
    };
    if parsed_end.is_some_and(|end| end < offset) {
        return RangeRequest::Ignore;
    }
    if offset >= size {
        return RangeRequest::Unsatisfiable;
    }
    let last = parsed_end.unwrap_or(size - 1).min(size - 1);
    RangeRequest::Satisfiable {
        offset,
        length: last - offset + 1,
    }
}

fn decode_cas_key(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let value =
                u8::from_str_radix(std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?, 16).ok()?;
            out.push(value);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    let key = String::from_utf8(out).ok()?;
    let mut segments = key.split('/');
    let namespace = segments.next()?;
    let algorithm = segments.next()?;
    let shard = segments.next()?;
    let digest = segments.next()?;
    if segments.next().is_some()
        || namespace != "objects"
        || algorithm != "sha256"
        || shard.len() != 2
        || digest.len() != 64
        || !shard
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !digest.starts_with(shard)
    {
        None
    } else {
        Some(key)
    }
}
fn base_headers() -> WorkerResult<Headers> {
    let headers = Headers::new();
    headers.set(
        "content-security-policy",
        "default-src 'none'; frame-ancestors 'none'; sandbox",
    )?;
    headers.set("referrer-policy", "no-referrer")?;
    headers.set("x-content-type-options", "nosniff")?;
    headers.set("x-frame-options", "DENY")?;
    Ok(headers)
}
fn text_response(
    request: &Request,
    status: u16,
    text: &str,
    extra: Option<(&str, &str)>,
) -> WorkerResult<Response> {
    let bytes = text.as_bytes().to_vec();
    let headers = base_headers()?;
    headers.set("cache-control", "no-store")?;
    headers.set("content-type", "text/plain; charset=utf-8")?;
    headers.set("content-length", &bytes.len().to_string())?;
    if let Some((key, value)) = extra {
        headers.set(key, value)?;
    }
    let builder = ResponseBuilder::new()
        .with_status(status)
        .with_headers(headers);
    Ok(if request.method() == Method::Head {
        builder.empty()
    } else {
        builder.fixed(bytes)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_ranges_distinguish_partial_unsatisfiable_and_ignored() {
        assert_eq!(
            parse_range("bytes=1-3", 6),
            RangeRequest::Satisfiable {
                offset: 1,
                length: 3
            }
        );
        assert_eq!(
            parse_range("bytes=-2", 6),
            RangeRequest::Satisfiable {
                offset: 4,
                length: 2
            }
        );
        assert_eq!(
            parse_range("bytes=4-", 6),
            RangeRequest::Satisfiable {
                offset: 4,
                length: 2
            }
        );
        assert_eq!(parse_range("bytes=7-8", 6), RangeRequest::Unsatisfiable);
        assert_eq!(parse_range("bytes=4-3", 6), RangeRequest::Ignore);
        assert_eq!(parse_range("bytes=-0", 6), RangeRequest::Unsatisfiable);
        assert_eq!(parse_range("bytes=0-1,3-4", 6), RangeRequest::Ignore);
        assert_eq!(parse_range("items=0-1", 6), RangeRequest::Ignore);
        assert_eq!(parse_range("bytes=wat", 6), RangeRequest::Ignore);
    }

    #[test]
    fn configured_chain_id_is_strict_positive_decimal() {
        assert_eq!(parse_configured_chain_id("8453"), Ok(8453));
        for invalid in ["", "0", "01", "0x2105", "-1", "18446744073709551616"] {
            assert_eq!(
                parse_configured_chain_id(invalid),
                Err(ArchiveError::Backend)
            );
        }
    }

    #[test]
    fn gateway_keys_and_rpc_ids_fail_closed() {
        let digest = "ab".repeat(32);
        let key = format!("objects/sha256/ab/{digest}");
        assert_eq!(decode_cas_key(&key).as_deref(), Some(key.as_str()));
        assert!(decode_cas_key(&format!("objects/sha256/ac/{digest}")).is_none());
        assert!(decode_cas_key(&format!("objects/sha256/AB/{}", digest.to_uppercase())).is_none());
        assert!(decode_cas_key("chains/0x2105/heads/finalized.bin").is_none());
        assert!(decode_cas_key("nested/data.bin").is_none());
        assert!(decode_cas_key("%2e%2e%2fsecret").is_none());
        assert!(valid_id(&json!(9_007_199_254_740_991_u64)));
        assert!(!valid_id(&json!(9_007_199_254_740_992_u64)));
        assert!(!valid_id(&json!(1.5)));
        assert!(!valid_id(&json!(true)));
    }
}
