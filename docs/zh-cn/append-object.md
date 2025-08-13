# 追加写入接口 (实验特性)

RustFS 提供针对已存在对象的增量追加能力，用于优化日志、时间序列等顺序增长场景，避免重写历史数据。

## 请求头

```
x-amz-write-offset-bytes: <N>
```

## 语义

- 当该头存在且 `N > 0` 时，普通 `PUT Object` 进入追加模式。
- `N` 必须与对象当前逻辑大小(字节)完全相等，否则返回 `PreconditionFailed`。
- 新数据作为一个新的 part 写入并独立纠删码编码。
- 目标对象必须已存在且至少有一个 part（不能对真正空对象直接追加）。
- 对压缩或加密对象不支持追加，返回 `NotImplemented` 或 `InvalidRequest`。
- 追加成功后 ETag 为 multipart 风格（含 `-<part数>` 后缀）。

## 错误映射

| 条件 | 错误码 | 说明 |
|------|--------|------|
| 偏移不匹配 | PreconditionFailed | 提供 offset != 当前大小 |
| 空对象追加 | InvalidRequest | 需先普通 PUT 建立首个 part |
| 压缩/加密对象 | NotImplemented / InvalidRequest | 禁止追加 |

## 示例

```bash
# 初次写入 (大小 11)
curl -X PUT "http://localhost:9000/demo/log.txt" \
  -H "Content-Type: text/plain" \
  --data-binary "hello world"

# 在偏移 11 追加 5 字节
echo -n "_TAIL" > part2
curl -X PUT "http://localhost:9000/demo/log.txt" \
  -H "x-amz-write-offset-bytes: 11" \
  --data-binary @part2
```

## 并发建议

追加前使用 `HEAD Object` 获取长度并协调多写入者，避免竞态。后续可能加入条件头辅助。

## 限制

- 仅支持末尾顺序追加，不支持随机覆盖。
- 不是 multipart 上传会话的替代方案。
- 开启版本控制时行为与普通 PUT 一致（生成新版本）。

## 适用场景

- 日志聚合
- 时间序列增量导出
- 持续增长的归档流

> 本 API 为实验特性，后续可能调整。
