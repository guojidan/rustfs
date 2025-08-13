# Append Object API (Experimental)

RustFS provides an incremental append capability for existing objects to optimize sequential growth workloads (logs, time-series dumps) without rewriting prior data.

## Request Header

```
x-amz-write-offset-bytes: <N>
```

## Semantics

- When present and `N > 0`, a normal `PUT Object` is treated as an append.
- `N` must equal the current logical size in bytes of the object; otherwise the request fails (`PreconditionFailed`).
- Data is stored as a new part (multipart-style) and erasure coded independently.
- The object must already exist and contain at least one part (can't append to a non-existent or zero-length object created implicitly).
- Unsupported if the object is compressed or encrypted; returns `NotImplemented` or `InvalidRequest`.
- ETag after append is (or becomes) multipart-style with `-<partCount>` suffix.

## Error Mapping

| Condition | Error Code | Notes |
|-----------|------------|-------|
| Offset mismatch | PreconditionFailed | Provided offset != current size |
| Empty object (no parts) | InvalidRequest | First write must be normal PUT |
| Compressed/encrypted object | NotImplemented / InvalidRequest | Append disallowed |

## Example

```bash
# Initial write (size 11)
curl -X PUT "http://localhost:9000/demo/log.txt" \
  -H "Content-Type: text/plain" \
  --data-binary "hello world"

# Append 5 bytes at offset 11
echo -n "_TAIL" > part2
curl -X PUT "http://localhost:9000/demo/log.txt" \
  -H "x-amz-write-offset-bytes: 11" \
  --data-binary @part2
```

## Concurrency Guidance

Retrieve object size (e.g., `HEAD Object`) immediately before append; coordinate writers to avoid races. Future work may add conditional headers.

## Limitations

- Only end-of-object append; no random overwrite.
- Not a replacement for full multipart upload sessions.
- Versioning behaves like normal PUT (new version if bucket versioned).

## Use Cases

- Log aggregation
- Time-series incremental export
- Growing archival streams

> This API is experimental and may evolve.
