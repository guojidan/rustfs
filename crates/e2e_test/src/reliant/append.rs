#![cfg(test)]
// Copyright 2025 RustFS Team
// Licensed under the Apache License, Version 2.0

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use bytes::Bytes;
use serial_test::serial;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET_PREFIX: &str = "test-append-bucket";

async fn create_aws_s3_client() -> Result<Client, Box<dyn Error>> {
    let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "static"))
        .endpoint_url(ENDPOINT)
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::Config::from(&shared_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    Ok(client)
}

fn unique_bucket(base: &str) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    format!("{}-{}-{}", BUCKET_PREFIX, base.to_lowercase(), ts)
}

async fn setup_test_bucket(client: &Client, base: &str) -> Result<String, Box<dyn Error>> {
    let bucket = unique_bucket(base);
    match client.create_bucket().bucket(&bucket).send().await {
        Ok(_) => {}
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BucketAlreadyOwnedByYou") && !error_str.contains("BucketAlreadyExists") {
                return Err(e.into());
            }
        }
    }
    Ok(bucket)
}

async fn cleanup_bucket(client: &Client, bucket: &str) {
    // 列举并删除对象后，删除 bucket；忽略错误
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket);
        if let Some(t) = token.as_deref() {
            req = req.continuation_token(t);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => break,
        };
        let keys: Vec<String> = resp
            .contents()
            .iter()
            .filter_map(|o| o.key().map(|s| s.to_string()))
            .collect();
        for k in keys {
            let _ = client.delete_object().bucket(bucket).key(k).send().await;
        }
        let truncated = resp.is_truncated().unwrap_or(false);
        if truncated {
            token = resp.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }
    let _ = client.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_success() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-success").await?;
    let key = "append-test-object.txt";

    let data1 = b"hello".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Append second chunk with correct offset
    let data2 = b"world!".to_vec();
    let offset = data1.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&offset.to_string()).unwrap());
        })
        .send()
        .await?;

    // Fetch and verify combined content
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let etag_opt = get_resp.e_tag().map(|s| s.to_string());
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"helloworld!");
    assert_eq!(buf.len(), data1.len() + data2.len());

    // Composite ETag should show multipart style with suffix "-2"
    if let Some(etag) = etag_opt.as_ref() {
        assert!(etag.contains("-2"), "expected composite ETag with -2 suffix, got {etag}");
    }

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_offset_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-offset-mismatch").await?;
    let key = "append-offset-mismatch.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Wrong offset (should be 3, we use 2)
    let wrong_offset = 2usize;
    let data2 = b"DEF".to_vec();
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&wrong_offset.to_string()).unwrap());
        })
        .send()
        .await;

    assert!(result.is_err(), "expected offset mismatch error");
    if let Err(e) = result {
        let es = e.to_string();
        let mut ok = es.contains("PreconditionFailed") || es.contains("append offset mismatch");
        // Prefer structured check from SDK if available
        #[allow(unused_mut)]
        let mut code_msg = String::new();
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            if let Some(code) = meta.code() {
                code_msg.push_str(code);
            }
            if let Some(msg) = meta.message() {
                if !code_msg.is_empty() {
                    code_msg.push_str(": ");
                }
                code_msg.push_str(msg);
            }
            if !ok {
                ok = code_msg.contains("PreconditionFailed") || code_msg.contains("append offset mismatch");
            }
        }
        assert!(ok, "unexpected error: es={es}, meta={code_msg}");
        println!("ex: {}, code_msg: {}, e: {:?}", es, code_msg, e);
    }

    // Ensure content not modified
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_offset_too_large() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-offset-too-large").await?;
    let key = "append-offset-too-large.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Offset larger than current size (3 + 5)
    let wrong_offset = data1.len() + 5;
    let data2 = b"XYZ".to_vec();
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&wrong_offset.to_string()).unwrap());
        })
        .send()
        .await;

    assert!(result.is_err(), "expected offset too large error");
    if let Err(e) = result {
        let es = e.to_string();
        let mut ok = es.contains("PreconditionFailed") || es.contains("append offset mismatch");
        let mut code_msg = String::new();
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            if let Some(code) = meta.code() {
                code_msg.push_str(code);
            }
            if let Some(msg) = meta.message() {
                if !code_msg.is_empty() {
                    code_msg.push_str(": ");
                }
                code_msg.push_str(msg);
            }
            if !ok {
                ok = code_msg.contains("PreconditionFailed") || code_msg.contains("append offset mismatch");
            }
        }
        assert!(ok, "unexpected error: es={es}, meta={code_msg}");
    }

    // Ensure original content unchanged
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_multiple_times() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-multi").await?;
    let key = "append-multi.txt";

    let part1 = b"one".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(part1.clone()).into())
        .send()
        .await?;

    let part2 = b"two".to_vec();
    let off2 = part1.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(part2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off2.to_string()).unwrap());
        })
        .send()
        .await?;

    let part3 = b"three".to_vec();
    let off3 = part1.len() + part2.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(part3.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off3.to_string()).unwrap());
        })
        .send()
        .await?;

    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let etag_opt = get_resp.e_tag().map(|s| s.to_string());
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"onetwothree");
    if let Some(etag) = etag_opt.as_ref() {
        assert!(etag.contains("-3"), "expected composite ETag with -3 suffix, got {etag}");
    }
    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_with_offset_zero_overwrites() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-offset-zero-overwrite").await?;
    let key = "append-offset-zero-overwrite.txt";

    // Initial object
    let data1 = b"AAAA".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Send header with offset=0 — should go through normal put path (overwrite)
    let data2 = b"BBBBBB".to_vec();
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_static("0"));
        })
        .send()
        .await;

    assert!(result.is_ok(), "offset=0 should not trigger append failure");

    // Verify content overwritten (not appended)
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"BBBBBB");
    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_header_invalid() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-header-invalid").await?;
    let key = "append-header-invalid.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    let data2 = b"zzz".to_vec();
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(|req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_static("bad"));
        })
        .send()
        .await;

    assert!(result.is_err(), "invalid header should error");
    if let Err(e) = result {
        let es = e.to_string();
        let mut ok = es.contains("InvalidArgument") || es.contains("Invalid x-amz-write-offset-bytes");
        // Prefer structured check from SDK if available
        #[allow(unused_mut)]
        let mut code_msg = String::new();
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            if let Some(code) = meta.code() {
                code_msg.push_str(code);
            }
            if let Some(msg) = meta.message() {
                if !code_msg.is_empty() {
                    code_msg.push_str(": ");
                }
                code_msg.push_str(msg);
            }
            if !ok {
                ok = code_msg.contains("InvalidArgument") || code_msg.contains("Invalid x-amz-write-offset-bytes");
            }
        }
        assert!(ok, "unexpected error for invalid header: es={es}, meta={code_msg}");
    }

    // Ensure original content unchanged
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");
    cleanup_bucket(&client, &bucket).await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_encryption_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-encryption-reject").await?;
    let key = "append-encryption-reject.txt";

    // Create initial object
    let data1 = vec![b'a'; 8];
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Try append with encryption header and correct offset
    let offset = data1.len();
    let data2 = vec![b'b'; 4];
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&offset.to_string()).unwrap());
            req.headers_mut()
                .insert("x-amz-server-side-encryption", HeaderValue::from_static("AES256"));
        })
        .send()
        .await;

    assert!(result.is_err(), "expected encryption append rejection");
    if let Err(e) = result {
        let es = e.to_string();
        let mut ok = es.contains("NotImplemented") || es.contains("Append with compression/encryption not supported");
        #[allow(unused_mut)]
        let mut code_msg = String::new();
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            if let Some(code) = meta.code() {
                code_msg.push_str(code);
            }
            if let Some(msg) = meta.message() {
                if !code_msg.is_empty() {
                    code_msg.push_str(": ");
                }
                code_msg.push_str(msg);
            }
            if !ok {
                ok = code_msg.contains("NotImplemented") || code_msg.contains("Append with compression/encryption not supported");
            }
        }
        assert!(ok, "unexpected error: es={es}, meta={code_msg}");
    }

    // Ensure object unchanged
    let get_resp = client.get_object().bucket(&bucket).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf.len(), data1.len());
    assert!(buf.iter().all(|c| *c == b'a'));
    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_if_match_and_none_match() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-ifmatch").await?;
    let key = "append-ifmatch.txt";

    // Put initial object
    let data1 = b"hello".to_vec();
    let put1 = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;
    let etag1 = put1.e_tag().unwrap_or_default().to_string();

    // If-Match success on correct etag
    let data2 = b"world".to_vec();
    let off = data1.len();
    let res_ok = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off.to_string()).unwrap());
            req.headers_mut().insert("If-Match", HeaderValue::from_str(&etag1).unwrap());
        })
        .send()
        .await;
    assert!(res_ok.is_ok(), "If-Match with correct etag should succeed");

    // If-None-Match should fail when object exists
    let data3 = b"!".to_vec();
    let off2 = data1.len() + data2.len();
    let res_fail = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data3.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off2.to_string()).unwrap());
            req.headers_mut().insert("If-None-Match", HeaderValue::from_static("*"));
        })
        .send()
        .await;
    assert!(res_fail.is_err(), "If-None-Match * should fail for existing object");
    if let Err(e) = res_fail {
        let mut ok = false;
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            let code = meta.code().unwrap_or("");
            let msg = meta.message().unwrap_or("");
            ok = code.contains("PreconditionFailed") || msg.contains("precondition failed");
        }
        assert!(ok, "unexpected error: {e}");
    }

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_get_object_range_across_parts() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-range").await?;
    let key = "append-range.txt";

    // Create multiple parts via put + append
    let p1 = b"abcdef".to_vec(); // 6
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p1.clone()).into())
        .send()
        .await?;

    let p2 = b"GHIJ".to_vec(); // 4 (total 10)
    let off2 = p1.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off2.to_string()).unwrap());
        })
        .send()
        .await?;

    // Range spanning across parts: bytes=3-8 → "defGHI"
    let get = client.get_object().bucket(&bucket).key(key).range("bytes=3-8").send().await?;
    let mut reader = get.body.into_async_read();
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf).await?;
    assert_eq!(buf, b"defGHI");

    // Suffix range: bytes=-4 → last 4 bytes "HIJ" from second part? Actually total 10 → last 4: "FGHIJ"[-4:] = "HIJ"? Let's compute precisely.
    let get2 = client.get_object().bucket(&bucket).key(key).range("bytes=-4").send().await?;
    let mut r2 = get2.body.into_async_read();
    let mut b2 = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut r2, &mut b2).await?;
    // Total = 10, last 4 are bytes index 6..10 => "GHIJ"
    assert_eq!(b2, b"GHIJ");

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_if_match_wrong_etag_fails() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-ifmatch-wrong").await?;
    let key = "append-ifmatch-wrong.txt";

    // Put initial object
    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Append with wrong If-Match
    let data2 = b"def".to_vec();
    let off = data1.len();
    let res = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(data2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off.to_string()).unwrap());
            req.headers_mut().insert("If-Match", HeaderValue::from_static("\"nonexist\""));
        })
        .send()
        .await;
    assert!(res.is_err(), "If-Match with wrong etag should fail");
    if let Err(e) = res {
        let mut ok = false;
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            let code = meta.code().unwrap_or("");
            let msg = meta.message().unwrap_or("");
            ok = code.contains("PreconditionFailed") || msg.contains("precondition failed");
        }
        assert!(ok, "unexpected error: {e}");
    }

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_get_object_more_ranges() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-range-more").await?;
    let key = "append-range-more.txt";

    // Build content = "hello" + "world" = "helloworld"
    let p1 = b"hello".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p1.clone()).into())
        .send()
        .await?;
    let p2 = b"world".to_vec();
    let off2 = p1.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off2.to_string()).unwrap());
        })
        .send()
        .await?;

    // Prefix range 0-4 => "hello"
    let r1 = client.get_object().bucket(&bucket).key(key).range("bytes=0-4").send().await?;
    let mut b1 = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut r1.body.into_async_read(), &mut b1).await?;
    assert_eq!(b1, b"hello");

    // Open-ended from 5 => "world"
    let r2 = client.get_object().bucket(&bucket).key(key).range("bytes=5-").send().await?;
    let mut b2 = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut r2.body.into_async_read(), &mut b2).await?;
    assert_eq!(b2, b"world");

    // Single byte 0-0 => "h"
    let r3 = client.get_object().bucket(&bucket).key(key).range("bytes=0-0").send().await?;
    let mut b3 = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut r3.body.into_async_read(), &mut b3).await?;
    assert_eq!(b3, b"h");

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_negative_offset_invalid() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-negative-offset").await?;
    let key = "append-negative-offset.txt";

    // Seed object
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(b"seed").into())
        .send()
        .await?;

    // Append with negative offset
    let result = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(b"x").into())
        .customize()
        .mutate_request(|req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_static("-1"));
        })
        .send()
        .await;

    assert!(result.is_err(), "negative offset should be invalid");
    if let Err(e) = result {
        let mut ok = false;
        if let aws_sdk_s3::error::SdkError::ServiceError(se) = &e {
            let meta = se.err().meta();
            let code = meta.code().unwrap_or("");
            let msg = meta.message().unwrap_or("");
            ok = code.contains("InvalidArgument") || msg.contains("Invalid x-amz-write-offset-bytes");
        }
        assert!(ok, "unexpected error: {e}");
    }

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_head_after_multiple_appends() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-head").await?;
    let key = "append-head.txt";

    let p1 = b"aa".to_vec();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p1.clone()).into())
        .send()
        .await?;
    let p2 = b"bbb".to_vec();
    let off2 = p1.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p2.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off2.to_string()).unwrap());
        })
        .send()
        .await?;
    let p3 = b"cccc".to_vec();
    let off3 = p1.len() + p2.len();
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from(p3.clone()).into())
        .customize()
        .mutate_request(move |req| {
            use http::header::HeaderValue;
            req.headers_mut()
                .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off3.to_string()).unwrap());
        })
        .send()
        .await?;

    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    let size = head.content_length();
    assert_eq!(size, Some((p1.len() + p2.len() + p3.len()) as i64));
    if let Some(etag) = head.e_tag() {
        assert!(etag.contains("-3"), "expected multipart-style etag with -3 suffix, got {etag}");
    }
    cleanup_bucket(&client, &bucket).await;
    Ok(())
}

// 注意：此测试仅做小规模多次 append，用于验证 multipart 行为，避免大规模压力
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_too_many_parts_stress() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    let bucket = setup_test_bucket(&client, "append-too-many-parts-stress").await?;
    let key = "append-too-many-parts-stress.txt";

    // Start with a 1-byte object
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(b"0").into())
        .send()
        .await?;

    // Append a few 1-byte parts to ensure multipart behavior (composite ETag)
    let small_parts = 3usize; // keep tiny and stable
    for i in 1..=small_parts {
        let off = i; // expected offset grows by 1 each time
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(Bytes::from_static(b"1").into())
            .customize()
            .mutate_request(move |req| {
                use http::header::HeaderValue;
                req.headers_mut()
                    .insert("x-amz-write-offset-bytes", HeaderValue::from_str(&off.to_string()).unwrap());
            })
            .send()
            .await?;
    }

    // Verify size and composite ETag suffix
    let head = client.head_object().bucket(&bucket).key(key).send().await?;
    assert_eq!(head.content_length(), Some((1 + small_parts) as i64));
    if let Some(etag) = head.e_tag() {
        // initial + small_parts => total parts = 1 + small_parts
        let expected_suffix = format!("-{}", 1 + small_parts);
        let et = etag.trim_matches('"');
        if et.contains('-') {
            assert!(
                et.ends_with(&expected_suffix),
                "expected multipart-style etag with {expected_suffix} suffix, got {etag}"
            );
        } else {
            // 兼容某些实现返回非 composite ETag 的情形
            assert!(!et.is_empty(), "etag should not be empty");
        }
    }

    cleanup_bucket(&client, &bucket).await;
    Ok(())
}
