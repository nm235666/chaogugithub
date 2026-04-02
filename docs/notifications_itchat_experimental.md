# 个人微信通知通道（ItChat）实验说明

> 状态：实验性（默认不作为生产主链）

## 背景
- 当前生产主通知通道建议使用企业微信 webhook。
- 本文档说明如何启用个人微信通道，仅用于 PoC 或内部实验。

## 能力位置
- 适配器：`services/notifications/channels/wechat_personal.py`
- 服务入口：`services/notifications/service.py` 的 `notify_with_wechat_personal` / `notify(channel="itchat")`

## 启用步骤
1. 安装依赖（实验环境）：
```bash
pip install itchat-uos
```
2. 配置可选环境变量：
```bash
export WECHAT_PERSONAL_TO_USER=filehelper
export WECHAT_PERSONAL_STATUS_FILE=itchat.pkl
```
3. 首次发送时扫码登录（或终端二维码）。

## 使用说明
- 默认发送到 `filehelper`，建议先走文件传输助手验证。
- 若需要发给指定联系人/群，传 `to_user_name`。
- 该通道返回 `experimental=true` 标识，便于日志区分。

## 风险边界
- 个人微信登录状态可能失效，需要重扫二维码。
- 该链路受微信侧风控影响，稳定性不可与 webhook 等同。
- 生产环境默认应继续使用企业微信 webhook；ItChat 仅用于实验。
