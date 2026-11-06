//! The adapter against a scripted HTTP server, for the answers an emulator never gives: server
//! errors, a dropped connection, 409 and an object that changes during a range read.
//!
//! The server answers every request with what the test's script returns and counts requests, so
//! a test can also check that the client did not retry on its own. These tests need no
//! endpoint and run with the rest of the workspace.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use object_store::aws::AmazonS3Builder;
use seabed_adapter_objectstore_s3::S3ObjectStore;
use seabed_domain::{ETag, Key};
use seabed_ports::{DurabilityClass, ObjectStore, PutCondition, StoreError};

/// One request as the server saw it.
#[derive(Clone, Debug)]
struct Request {
    method: String,
    /// Path and query, as sent.
    target: String,
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// What the server does with a request.
enum Reply {
    Status {
        code: u16,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    },
    /// Reads the request, then closes the connection without answering.
    Drop,
}

fn status(code: u16) -> Reply {
    Reply::Status {
        code,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

const LAST_MODIFIED: &str = "Wed, 21 Oct 2026 07:28:00 GMT";

struct FakeS3 {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl FakeS3 {
    fn start(script: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let Some(req) = read_request(&stream) else {
                    continue;
                };
                seen.lock().unwrap().push(req.clone());
                if let Reply::Status {
                    code,
                    headers,
                    body,
                } = script(&req)
                {
                    write_reply(stream, code, &headers, &body);
                }
            }
        });
        Self { endpoint, requests }
    }

    fn store(&self) -> S3ObjectStore {
        let builder = AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_allow_http(true)
            .with_bucket_name("seabed-test")
            .with_region("us-east-1")
            .with_access_key_id("test")
            .with_secret_access_key("test");
        S3ObjectStore::new(builder, DurabilityClass::Ephemeral).unwrap()
    }

    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}

fn read_request(stream: &TcpStream) -> Option<Request> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).ok()?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        let (n, v) = h.split_once(':')?;
        headers.push((n.trim().to_string(), v.trim().to_string()));
    }
    let req = Request {
        method,
        target,
        headers,
    };
    let len: usize = req
        .header("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; len];
    reader.read_exact(&mut body).ok()?;
    Some(req)
}

fn write_reply(mut stream: TcpStream, code: u16, headers: &[(&str, String)], body: &[u8]) {
    let mut out = format!("HTTP/1.1 {code} Fake\r\nConnection: close\r\n");
    if !headers
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("content-length"))
    {
        out.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    for (n, v) in headers {
        out.push_str(&format!("{n}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes());
    let _ = stream.write_all(body);
}

fn key() -> Key {
    Key::new("k")
}

fn assert_unavailable(r: Result<impl std::fmt::Debug, StoreError>) {
    match r {
        Err(StoreError::Unavailable(_)) => {}
        other => panic!("expected Unavailable, got {other:?}"),
    }
}

/// A 5xx or a dropped connection may mean the request reached the store, so the adapter reports
/// it as `Unavailable` (never as a refusal or a missing key) and leaves retrying to the engine:
/// the server sees exactly one request.
#[test]
fn server_errors_and_dropped_connections_are_unavailable_and_not_retried() {
    for code in [500, 503] {
        let server = FakeS3::start(move |_| status(code));
        let store = server.store();
        assert_unavailable(store.get(&key()));
        assert_unavailable(store.put(&key(), b"x".to_vec(), PutCondition::Unconditional));
        assert_unavailable(store.put(&key(), b"x".to_vec(), PutCondition::IfNoneMatch));
        assert_unavailable(store.put(
            &key(),
            b"x".to_vec(),
            PutCondition::IfMatch(ETag::new("\"e\"")),
        ));
        assert_unavailable(store.list(""));
        assert_eq!(
            server.requests().len(),
            5,
            "status {code}: the client retried"
        );
    }

    let server = FakeS3::start(|_| Reply::Drop);
    let store = server.store();
    assert_unavailable(store.get(&key()));
    assert_unavailable(store.put(&key(), b"x".to_vec(), PutCondition::Unconditional));
    assert_unavailable(store.put(&key(), b"x".to_vec(), PutCondition::IfNoneMatch));
    assert_unavailable(store.put(
        &key(),
        b"x".to_vec(),
        PutCondition::IfMatch(ETag::new("\"e\"")),
    ));
    assert_eq!(server.requests().len(), 4, "dropped: the client retried");
}

/// Nothing listening: the request never reached a store, but the adapter cannot tell that
/// apart from a lost answer, so it is `Unavailable` too.
#[test]
fn a_refused_connection_is_unavailable() {
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let builder = AmazonS3Builder::new()
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_allow_http(true)
        .with_bucket_name("seabed-test")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test");
    let store = S3ObjectStore::new(builder, DurabilityClass::Ephemeral).unwrap();
    assert_unavailable(store.get(&key()));
    assert_unavailable(store.put(
        &key(),
        b"x".to_vec(),
        PutCondition::IfMatch(ETag::new("\"e\"")),
    ));
    assert_unavailable(store.list(""));
}

/// S3 answers 409 when a concurrent conditional write to the same key is in progress. Like a
/// 412, it is a refusal: the write did not happen.
#[test]
fn a_409_is_reported_as_a_refusal() {
    let server = FakeS3::start(|_| status(409));
    let store = server.store();
    assert_eq!(
        store.put(&key(), b"x".to_vec(), PutCondition::IfNoneMatch),
        Err(StoreError::PreconditionFailed)
    );
    assert_eq!(
        store.put(
            &key(),
            b"x".to_vec(),
            PutCondition::IfMatch(ETag::new("\"e\""))
        ),
        Err(StoreError::Conflict)
    );
    assert_eq!(server.requests().len(), 2, "the client retried a 409");
}

/// The range GET only accepts the object the HEAD saw. If it changed in between, S3 answers 412
/// and the read is `Unavailable` (try again), not `PreconditionFailed`, which a read has no
/// use for.
#[test]
fn a_range_read_of_an_object_that_changed_after_the_head_is_unavailable() {
    let server = FakeS3::start(|req| match req.method.as_str() {
        "HEAD" => Reply::Status {
            code: 200,
            headers: vec![
                ("Content-Length", "10".into()),
                ("ETag", "\"v1\"".into()),
                ("Last-Modified", LAST_MODIFIED.into()),
            ],
            body: Vec::new(),
        },
        _ => status(412),
    });
    let store = server.store();
    assert_unavailable(store.get_range(&key(), 2..5));

    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[1].method, "GET");
    assert_eq!(reqs[1].header("if-match"), Some("\"v1\""));
    assert_eq!(reqs[1].header("range"), Some("bytes=2-4"));
}

/// The happy path through the same server, so the tests above fail for the reason they name
/// and not because the fake speaks HTTP badly.
#[test]
fn the_fake_server_serves_a_range_read() {
    let server = FakeS3::start(|req| {
        let base = vec![
            ("ETag", "\"v1\"".to_string()),
            ("Last-Modified", LAST_MODIFIED.to_string()),
        ];
        match req.method.as_str() {
            "HEAD" => Reply::Status {
                code: 200,
                headers: [base, vec![("Content-Length", "10".into())]].concat(),
                body: Vec::new(),
            },
            _ => Reply::Status {
                code: 206,
                headers: [base, vec![("Content-Range", "bytes 2-4/10".into())]].concat(),
                body: b"234".to_vec(),
            },
        }
    });
    let store = server.store();
    assert_eq!(store.get_range(&key(), 2..5).unwrap(), b"234");
}

fn list_page(keys: &[&str], next: Option<&str>) -> Reply {
    let contents: String = keys
        .iter()
        .map(|k| {
            format!(
                "<Contents><Key>{k}</Key><LastModified>2026-10-21T07:28:00.000Z</LastModified>\
                 <ETag>&quot;e&quot;</ETag><Size>1</Size></Contents>"
            )
        })
        .collect();
    let more = match next {
        Some(t) => format!(
            "<IsTruncated>true</IsTruncated><NextContinuationToken>{t}</NextContinuationToken>"
        ),
        None => "<IsTruncated>false</IsTruncated>".to_string(),
    };
    Reply::Status {
        code: 200,
        headers: vec![("Content-Type", "application/xml".into())],
        body: format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>seabed-test</Name>\
             <Prefix>p/</Prefix><KeyCount>{}</KeyCount>{more}{contents}</ListBucketResult>",
            keys.len()
        )
        .into_bytes(),
    }
}

/// `list` asks for the configured page size and follows the continuation token to the end.
#[test]
fn list_asks_for_the_page_size_and_follows_the_token() {
    let server = FakeS3::start(|req| {
        if req.target.contains("continuation-token=t1") {
            list_page(&["p/2"], None)
        } else {
            list_page(&["p/1", "p/0"], Some("t1"))
        }
    });
    let store = server.store().with_list_page_size(2);
    let listed: Vec<String> = store
        .list("p/")
        .unwrap()
        .into_iter()
        .map(|m| m.key.as_str().to_string())
        .collect();
    assert_eq!(listed, ["p/0", "p/1", "p/2"]);

    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    for r in &reqs {
        assert!(r.target.contains("max-keys=2"), "{}", r.target);
        assert!(r.target.contains("prefix=p%2F"), "{}", r.target);
    }
}
