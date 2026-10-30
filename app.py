"""chatanywhere 多 key 中转网关。

把 10 个 ChatAnywhere 账号的 API key 聚到一个 OpenAI 兼容入口后面：
客户端只认一个中转密钥（RELAY_KEY），网关按轮换把请求发到
https://api.chatanywhere.tech/v1，每号独立并发上限（默认 10），429/5xx/无响应
时自动换号重试。注意：每号免费额度为 7 天 50,000 点、24 小时 100 次，轮换只能分摊请求，不能突破单号额度。

关键安全规则：
- 13 个 key 只走 Render 环境变量，绝不写进仓库、文件或日志。
- 对外回显只用掩码（前 6 + 后 4），绝不输出完整值。
- 单 worker 运行：多 worker 会让每号并发上限翻倍。

流式转发注意：上游 httpx 流和每号信号量必须由响应生成器持有、
在流结束时才释放——绝不能包在 async with 里提前关闭，否则客户端
收到的是空 body（2026-10-25 实测：/v1/models 因此返回空体，
客户端报 Upstream model list response was not valid JSON）。
"""
import asyncio
import itertools
import logging
import os
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chatanywhere-relay")

UPSTREAM = os.environ.get("UPSTREAM_URL", "https://api.chatanywhere.tech/v1").rstrip("/")
RELAY_KEY = os.environ.get("RELAY_KEY", "")
PER_KEY_CONCURRENCY = int(os.environ.get("CONCURRENCY_PER_KEY", "10"))

# 10 个号位：第一批 10 个号全部有值。
# 环境变量缺失即视为未配置号位，以后新号注册后补填对应环境变量即自动启用。
SLOT_ENV = [
    ("qq01", "CA_KEY_QQ01"),
    ("qq02", "CA_KEY_QQ02"),
    ("qq03", "CA_KEY_QQ03"),
    ("16304", "CA_KEY_16304"),
    ("16305", "CA_KEY_16305"),
    ("gm01", "CA_KEY_GM01"),
    ("16301", "CA_KEY_16301"),
    ("16302", "CA_KEY_16302"),
    ("dwdbe", "CA_KEY_DWDBE"),
    ("milinken2365", "CA_KEY_MILINKEN2365"),
]

HOP_HEADERS = {
    "host", "authorization", "content-length", "connection", "keep-alive",
    "proxy-authenticate", "proxy-authorization", "te", "trailer",
    "transfer-encoding", "upgrade",
}


def mask(value: str) -> str:
    if len(value) <= 10:
        return "****"
    return value[:6] + "**********" + value[-4:]


def load_keys() -> dict:
    keys = {}
    for name, env in SLOT_ENV:
        v = os.environ.get(env, "").strip()
        if v:
            keys[name] = v
    return keys


KEYS = load_keys()
SEMAPHORES = {name: asyncio.Semaphore(PER_KEY_CONCURRENCY) for name in KEYS}
COOLDOWN_UNTIL = {name: 0.0 for name in KEYS}
_rr = itertools.cycle(list(KEYS.keys())) if KEYS else itertools.cycle([])


def ordered_slots() -> list:
    """轮换顺序：优先未冷却号位，其次按轮换指针。"""
    names = list(KEYS.keys())
    if not names:
        return []
    try:
        start = next(_rr)
    except StopIteration:
        start = names[0]
    idx = names.index(start) if start in names else 0
    rotated = names[idx:] + names[:idx]
    now = time.time()
    fresh = [n for n in rotated if COOLDOWN_UNTIL.get(n, 0) <= now]
    cooled = [n for n in rotated if COOLDOWN_UNTIL.get(n, 0) > now]
    return fresh + cooled


def check_auth(request: Request) -> bool:
    if not RELAY_KEY:
        return False
    return request.headers.get("authorization", "") == f"Bearer {RELAY_KEY}"


app = FastAPI(title="hyxware-relay")


@app.get("/")
async def root():
    return {"ok": True, "service": "chatanywhere-relay"}


@app.get("/health")
async def health():
    return {"ok": True, "slots_configured": len(KEYS), "slots": list(KEYS.keys())}


@app.get("/admin/keys-check")
async def keys_check(request: Request):
    """逐号探活：调上游 /v1/models，只报掩码与 http 状态，不输出完整值。"""
    if not check_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    results = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0),
                                 trust_env=False) as client:
        for name, key in KEYS.items():
            try:
                r = await client.get(
                    f"{UPSTREAM}/models",
                    headers={"Authorization": f"Bearer {key}"},
                )
                results[name] = {"mask": mask(key), "ok": r.status_code < 400,
                                 "http": r.status_code}
            except Exception as e:  # noqa: BLE001 - 探活只记失败
                results[name] = {"mask": mask(key), "ok": False,
                                 "http": 0, "error": type(e).__name__}
    return {"slots_configured": len(KEYS), "results": results}


@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    if not check_auth(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    if not KEYS:
        return JSONResponse({"error": "no keys configured"}, status_code=503)

    body = await request.body()
    query = str(request.url.query)
    fwd_headers = {k: v for k, v in request.headers.items()
                   if k.lower() not in HOP_HEADERS
                   and k.lower() != "accept-encoding"}
    url = f"{UPSTREAM}/{path}" + (f"?{query}" if query else "")

    last_status, last_body = 502, b"all keys failed"
    for name in ordered_slots():
        key = KEYS[name]
        sem = SEMAPHORES[name]
        # 信号量手动持有：流式响应要在整个 body 送完后才释放，
        # 不能用 async with 在 return 时提前释放。
        await sem.acquire()
        # trust_env=False：不读本地代理环境变量（本机代理变量格式会被
        # httpx 严格解析拒绝，致 AsyncClient 构造即 500；生产环境直连上游）。
        client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0),
                                   trust_env=False)
        try:
            headers = dict(fwd_headers)
            headers["Authorization"] = f"Bearer {key}"
            # 强制上游返回明文：避免 gzip/br 编解码在网关处错位。
            headers["Accept-Encoding"] = "identity"
            req = client.build_request(request.method, url, headers=headers,
                                       content=body)
            t0 = time.time()
            # 手动管理流生命周期：不在 async with 里打开，否则生成器
            # 执行时上游连接已关闭，客户端只能收到空 body。
            r = await client.send(req, stream=True)
            if r.status_code >= 400:
                last_status = r.status_code
                last_body = await r.aread()
                wait = 30.0 if r.status_code == 429 else 10.0
                COOLDOWN_UNTIL[name] = time.time() + wait
                log.info("slot %s -> %s (%.1fs), retry next",
                         name, r.status_code, time.time() - t0)
                await r.aclose()
                await client.aclose()
                sem.release()
                continue
            # 注意：httpx 已把上游 gzip/br 解码成明文再交出来，所以下游
            # 绝不能再带 content-encoding，否则浏览器会把明文当 gzip 解而失败。
            resp_headers = {
                k: v for k, v in r.headers.items()
                if k.lower() not in HOP_HEADERS
                and k.lower() not in ("content-length", "content-encoding")
            }
            log.info("slot %s -> %s (%.1fs)",
                     name, r.status_code, time.time() - t0)

            async def gen(r=r, client=client, sem=sem):
                try:
                    async for chunk in r.aiter_bytes():
                        yield chunk
                finally:
                    await r.aclose()
                    await client.aclose()
                    sem.release()

            return StreamingResponse(
                gen(), status_code=r.status_code,
                headers=resp_headers,
                media_type=r.headers.get("content-type"),
            )
        except Exception as e:  # noqa: BLE001 - 无响应也换号重试
            COOLDOWN_UNTIL[name] = time.time() + 10.0
            last_status, last_body = 502, str(e).encode()[:200]
            log.info("slot %s -> no response (%s), retry next",
                     name, type(e).__name__)
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - 清理失败不影响换号
                pass
            sem.release()
            continue
    # 全号失败：返回 JSON 错误体，客户端至少能解析出原因，
    # 而不是收到一段纯文本导致 not valid JSON。
    try:
        detail = last_body.decode("utf-8", "replace")[:500]
    except Exception:  # noqa: BLE001 - 解码失败时给通用文案
        detail = "upstream failed"
    return JSONResponse(
        {"error": {"message": detail, "type": "upstream_failed"}},
        status_code=last_status if 400 <= last_status <= 599 else 502,
    )
