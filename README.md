# Hyperliquid WebSocket 广播服务

服务通过 `github.com/luanruisong/hyperliquid-collector` 读取 Hyperliquid 节点磁盘上的 JSONL 数据，并向 WebSocket 订阅者广播。

## 启动

1. 节点使用 `--write-fills --write-misc-events --stream-with-block-info`，让采集器读取带区块信息的输出。
2. 在 [config/config.yaml](config/config.yaml) 中设置 `collector.data_dir` 为节点数据根目录。服务需要读取源文件，并能向断点文件所在目录写入。
3. 使用 Go 1.26.11 或更新版本，在项目根目录运行 `go run .`。默认连接地址为 `ws://localhost:8080/ws`。

`collector.retention` 控制源文件保留时长，默认 `"0s"` 禁用清理。设为 `"72h"` 后，采集器会清理已完整采集、所属小时结束时间和文件修改时间都超过 72 小时的源文件。每个流最新小时的文件始终保留。启用清理后，服务需要有权删除这些源文件。

## WebSocket 协议

| 频道 | 节点目录 |
| --- | --- |
| `fills` | `node_fills_streaming` |
| `miscEvents` | `misc_events_streaming` |
| `twapStatuses` | `node_twap_statuses_streaming` |

连接后发送 JSON 文本帧。每个连接可独立订阅多个频道。

```json
{"method":"subscribe","channel":"fills"}
```

服务返回 `{"type":"subscribed","channel":"fills"}` 后开始向该连接推送这个频道的数据。退订时发送 `{"method":"unsubscribe","channel":"fills"}`，服务返回 `{"type":"unsubscribed","channel":"fills"}`。无效请求返回 `type: "error"` 和 `message`。

每条磁盘记录对应一条数据消息，时间为 RFC 3339 格式，`events` 保留节点输出的 JSON 结构。

```json
{
  "type": "data",
  "channel": "fills",
  "data": {
    "block_number": 123,
    "block_time": "2026-11-26T03:00:00.123456789Z",
    "local_time": "2026-11-26T03:00:00.123456999Z",
    "events": [["0xabc", {"coin": "ETH", "px": "3600", "sz": "0.2"}]]
  }
}
```

断点记录服务的磁盘采集进度。服务启动后会处理断点后的记录，有订阅者时才推送。客户端重连后需重新订阅，协议不提供历史补发，也不保证跨频道顺序。发送队列满时服务会断开慢连接。浏览器跨域访问需要通过同源代理接入，服务本身无认证。
