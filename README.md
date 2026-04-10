Use the patched DingTalk adapter here:
https://raw.githubusercontent.com/dingtalkwukong/dingtalk-hermes-connector/main/gateway/platforms/dingtalk.py

Replace ~/.hermes/hermes-agent/gateway/platforms/dingtalk.py with this file and restart hermes gateway to make DingTalk messaging work normally.


DingTalk platform adapter for hermes
Supports:
- Stream-mode WebSocket long connection via dingtalk-stream SDK
- Direct-message text receive/send with session context
- Inbound image/file/audio/video caching
- Outbound image via media upload + native image message (mediaId)
- Outbound document/file via Wiki workspace upload
- OpenAPI integration (access token, union ID resolution)
