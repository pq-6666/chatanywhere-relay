# chatanywhere-relay

10 个 ChatAnywhere 账号的 OpenAI 兼容中转网关：客户端只认一个中转密钥，
网关按轮换把请求发到 `https://api.chatanywhere.tech/v1`，每号独立并发上限 10，
429 / 5xx / 无响应时自动换号重试。

注意：每号免费额度为 7 天 50,000 点、24 小时 100 次，轮换只能分摊请求，
不能突破单号额度；以后新号注册后，补填对应环境变量即自动启用。

## 部署（Render，手动，和 cassidy-relay / hyxware-relay 同一套路）

- 新建 Web Service，仓库选本仓库，Region 选 Singapore，Plan 选 Free。
- Start Command（单 worker，绝不能改多 worker，否则每号并发上限翻倍）：
  `uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1`
- 环境变量（只填 Render 后台，绝不写进仓库）：
  - `RELAY_KEY`：客户端用的中转密钥。
  - `CA_KEY_QQ01`、`CA_KEY_QQ02`、`CA_KEY_QQ03`、`CA_KEY_16304`、
    `CA_KEY_16305`、`CA_KEY_GM01`、`CA_KEY_16301`、`CA_KEY_16302`、
    `CA_KEY_DWDBE`、`CA_KEY_MILINKEN2365`：10 个号位。
  - 可选：`CONCURRENCY_PER_KEY`（默认 10）、`UPSTREAM_URL`（默认
    `https://api.chatanywhere.tech/v1`）。

## 客户端用法

Base URL：`https://chatanywhere-relay.onrender.com/v1`，请求头
`Authorization: Bearer <RELAY_KEY>`，其余和 OpenAI 格式一样。
Free 层闲置约 15 分钟休眠、冷启动约 1 分钟。

## 自查接口（都要带 RELAY_KEY）

- `GET /health`：已配置号位数（无需鉴权）。
- `GET /admin/keys-check`：逐号调上游 `/v1/models` 探活，只报掩码与
  http 状态，不输出完整值。
