Use the patched DingTalk adapter here:
https://raw.githubusercontent.com/dingtalkwukong/dingtalk-hermes-connector/main/gateway/platforms/dingtalk.py

Replace ~/.hermes/hermes-agent/gateway/platforms/dingtalk.py with this file and restart hermes gateway to make DingTalk messaging work normally.


DingTalk platform adapter for hermes
Supports:
- Outbound rich media (image, voice, video, file) sent via DingTalk Robot OpenAPI.
- Inbound media (image, audio, file) downloaded and cached for agent consumption.

