#![cfg(test)]
// Copyright 2025 RustFS Team
// Licensed under the Apache License, Version 2.0

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use bytes::Bytes;
use serial_test::serial;
use std::error::Error;
use tokio::io::AsyncReadExt;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const BUCKET: &str = "test-append-bucket";

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

async fn setup_test_bucket(client: &Client) -> Result<(), Box<dyn Error>> {
    match client.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BucketAlreadyOwnedByYou") && !error_str.contains("BucketAlreadyExists") {
                return Err(e.into());
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_success() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-test-object.txt";

    let data1 = b"hello".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Append second chunk with correct offset
    let data2 = b"world!".to_vec();
    let offset = data1.len();
    client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
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

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_offset_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-offset-mismatch.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Wrong offset (should be 3, we use 2)
    let wrong_offset = 2usize;
    let data2 = b"DEF".to_vec();
    let result = client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_offset_too_large() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-offset-too-large.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Offset larger than current size (3 + 5)
    let wrong_offset = data1.len() + 5;
    let data2 = b"XYZ".to_vec();
    let result = client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_multiple_times() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-multi.txt";

    let part1 = b"one".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(part1.clone()).into())
        .send()
        .await?;

    let part2 = b"two".to_vec();
    let off2 = part1.len();
    client
        .put_object()
        .bucket(BUCKET)
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
        .bucket(BUCKET)
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

    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let etag_opt = get_resp.e_tag().map(|s| s.to_string());
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"onetwothree");
    if let Some(etag) = etag_opt.as_ref() {
        assert!(etag.contains("-3"), "expected composite ETag with -3 suffix, got {etag}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_with_offset_zero_overwrites() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-offset-zero-overwrite.txt";

    // Initial object
    let data1 = b"AAAA".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Send header with offset=0 — should go through normal put path (overwrite)
    let data2 = b"BBBBBB".to_vec();
    let result = client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"BBBBBB");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_header_invalid() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-header-invalid.txt";

    let data1 = b"abc".to_vec();
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    let data2 = b"zzz".to_vec();
    let result = client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf, b"abc");
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_put_object_append_encryption_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let client = create_aws_s3_client().await?;
    setup_test_bucket(&client).await?;
    let key = "append-encryption-reject.txt";

    // Create initial object
    let data1 = vec![b'a'; 8];
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(Bytes::from(data1.clone()).into())
        .send()
        .await?;

    // Try append with encryption header and correct offset
    let offset = data1.len();
    let data2 = vec![b'b'; 4];
    let result = client
        .put_object()
        .bucket(BUCKET)
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
    let get_resp = client.get_object().bucket(BUCKET).key(key).send().await?;
    let mut reader = get_resp.body.into_async_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    assert_eq!(buf.len(), data1.len());
    assert!(buf.iter().all(|c| *c == b'a'));
    Ok(())
}
