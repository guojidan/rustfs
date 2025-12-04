# RustFS Append Upload Guide

RustFS supports continuously writing new data to the same object via append writes. This capability is intended for log collection, sequential writing of large files, or batch processing tasks requiring resume capability, avoiding frequent object splitting or temporary file management.

This guide covers append semantics, HTTP interface conventions, command-line and SDK examples, and troubleshooting points.

## Feature Overview

| Item | Description |
| --- | --- |
| Activation | Add `x-amz-write-offset-bytes` header to normal `PUT Object` request |
| Offset | Indicates the starting byte position of the data to be written in the object, must be a non-negative integer |
| First Write | When object doesn't exist, use `offset=0` or omit the header |
| Append Write | Offset must equal current object length (obtainable via `HEAD Object`) |
| Atomicity | RustFS validates offset length and rejects mismatched requests to prevent holes or overwrites |
| Cache Consistency | Successful writes automatically trigger cache invalidation, ensuring subsequent `GET` sees latest content |

## Request Flow

1. **Initialize Object**  
   Create object via normal `PUT`, or with `x-amz-write-offset-bytes: 0` (only if object doesn't exist).

2. **Calculate Next Offset**  
   Use `HEAD Object` to query `Content-Length`, which is the offset for the next chunk.  
   ```bash
   aws s3api head-object \
     --bucket my-bucket \
     --key logs/app.log \
     --endpoint-url http://127.0.0.1:9000 \
     --query 'ContentLength'
   ```

3. **Send Append Request**  
   Set `x-amz-write-offset-bytes` to the offset from previous step, `Content-Length` to the size of the current append chunk, and body with new content.

4. **Verify Result**  
   Server returns `200 OK` for success. Can `HEAD` or `GET` again to verify total length.

## Command-Line Example

Use `curl` (7.75+) with SigV4 support:

```bash
# 1. First write
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  --data-binary @chunk-0000.bin \
  http://127.0.0.1:9000/test-bucket/append-demo

# 2. Append 1 KiB (offset = 1024)
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "x-amz-write-offset-bytes: 1024" \
  --data-binary @chunk-0001.bin \
  http://127.0.0.1:9000/test-bucket/append-demo
```

If the passed offset doesn't match existing length, RustFS returns a `4xx` error and keeps original content. The client should re-read length and retry.

## Rust SDK Example

```rust
use aws_sdk_s3::{Client, types::ByteStream};

async fn append_part(client: &Client, bucket: &str, key: &str, offset: i64, data: Vec<u8>) -> anyhow::Result<()> {
    let mut op = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .customize()
        .await?;

    op.config_mut()
        .insert_header("x-amz-write-offset-bytes", offset.to_string());

    op.send().await?;
    Ok(())
}
```

Combine with `head_object()` to get `content_length()` to complete the "Calculate offset -> Call `append_part`" flow.

## Validation & Troubleshooting

- **Wrong offset**: If offset < 0 or inconsistent with existing size, receive `InvalidArgument` error. Re-`HEAD` and update offset to retry.
- **Object not found**: Specifying non-zero offset for non-existent object is rejected; for first write omit header or set to 0.
- **Concurrency**: When multiple clients write to same object concurrently, later requests detecting inconsistent offset will fail, prompting upper layer to re-read length.
- **Status Check**: `GET`/`HEAD` immediately sees size after append; background replication/consistency metadata is refreshed, no manual cache clear needed.

## Known Limitations

| Limitation | Description |
| --- | --- |
| SSE-S3 / SSE-C Objects | Append write currently doesn't support `x-amz-server-side-encryption: AES256` or customer-provided keys. Returns 4xx for such objects. |
| Compressed Objects | Objects with RustFS internal compression metadata (e.g., `x-rustfs-meta-x-rustfs-internal-compression`) forbid append, must use full rewrite. |
| Header Encoding | `x-amz-write-offset-bytes` must be valid UTF-8 and parseable as 64-bit non-negative integer. |
| Large Chunk Write | Single append still follows `PUT Object` size limits (max 5 GiB); larger traffic suggested to use normal multipart upload then append. |

## Regression Tests

End-to-end cases located in `crates/e2e_test/src/reliant/append.rs`, covering basic append, wrong offset, continuous multiple appends, compression/encryption limit validation, etc. During development run:

```bash
cargo test --package e2e_test --test reliant -- append
```

To quickly verify append functionality and compatibility with future changes.
