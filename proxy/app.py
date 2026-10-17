#!/usr/bin/env python3
"""fnmedia-ext 主代理（FastAPI）。

链路：nginx(/v) -> 本代理(UDS) -> trim-media 官方后端(upstream UDS)
策略：
  - 搜索 GET /v/api/v1/search/list   透传本地结果 + 追加在线资源
  - 详情 GET /v/api/v1/item/<guid>   在线 guid 返回伪造详情
  - 播放 POST /play/info、GET /stream/list、POST /stream、POST /play/play
                                     在线 guid/media_guid 返回伪造链路
  - 流   GET /v/media/online/<mg>/*  HLS 代理（重写 m3u8 分片/密钥）
  - 海报 GET /v/online/poster        外站海报同源代理
  - 其余全部原样透传官方后端
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import time
from pathlib import Path
import urllib.parse
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import (JSONResponse, RedirectResponse,
                               StreamingResponse)

import providers
import health
import inject as inj
import config as cfgmod
from config import config
import history as histmod
from history import history
from store import (store, make_guid, media_guid, video_guid, is_online_guid,
                   season_guid, episode_guid, ep_media_guid, ep_video_guid)

UPSTREAM_SOCK = os.environ.get("FNMEDIA_UPSTREAM_SOCK",
                               "/var/run/trim_media_upstream.sock")
ONLINE_PREFIX = "/v/media/online"
_search_cache: dict[str, tuple[float, list, bool]] = {}
# 线路速度统计：provider -> {ewma_ms, n, fails, last}（分片实测，卡顿自动换线用）
_SEG_STATE = Path(__file__).resolve().parent.parent / "state" / "seg_stats.json"
_seg_stats: dict[str, dict] = {}
_seg_dirty = False
_seg_last_save = 0.0


def _seg_load() -> None:
    global _seg_stats
    try:
        _seg_stats = json.loads(_SEG_STATE.read_text())
    except Exception:
        _seg_stats = {}


def _seg_flush() -> None:
    global _seg_dirty, _seg_last_save
    if not _seg_dirty:
        return
    try:
        _SEG_STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SEG_STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_seg_stats, ensure_ascii=False))
        tmp.replace(_SEG_STATE)
        _seg_dirty = False
        _seg_last_save = time.time()
    except Exception:
        pass


def _seg_record(provider: str, ms: float, ok: bool) -> None:
    global _seg_dirty
    if not provider:
        return
    st = _seg_stats.setdefault(
        provider, {"ewma_ms": float(ms), "n": 0, "fails": 0, "last": 0.0})
    st["ewma_ms"] = round(st["ewma_ms"] * 0.7 + ms * 0.3, 1) if st["n"] else round(ms, 1)
    st["n"] = int(st["n"]) + 1
    st["fails"] = 0 if ok else int(st.get("fails", 0)) + 1
    st["last"] = time.time()
    _seg_dirty = True
    if time.time() - _seg_last_save > 60:
        _seg_flush()


def _speed_rank(provider: str) -> float:
    """线路排序键：越小越快。没实测过的给中性值，排在中游。"""
    st = _seg_stats.get(provider)
    if not st:
        return 2000.0
    return float(st.get("ewma_ms", 2000)) + int(st.get("fails", 0)) * 800
_PARTIAL_TTL = 15.0   # 只抓了部分源的结果缓存时间
_SEARCH_BUDGET = 2.0  # 在线搜索总预算（秒），留 1s 给本地响应和收尾（总时限 3s）
CACHE_TTL = 120


def b64e(u: str) -> str:
    return base64.urlsafe_b64encode(u.encode()).decode().rstrip("=")


def b64d(s: str) -> str:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.up = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(uds=UPSTREAM_SOCK),
        timeout=httpx.Timeout(120), trust_env=False)
    app.state.ext = httpx.AsyncClient(
        timeout=httpx.Timeout(30), trust_env=False, follow_redirects=True)
    _seg_load()
    tasks = [asyncio.create_task(health.loop())]
    yield
    for t in tasks:
        t.cancel()
    _seg_flush()
    await app.state.up.aclose()
    await app.state.ext.aclose()


app = FastAPI(lifespan=lifespan)

import logging
_log = logging.getLogger("fnmedia")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@app.middleware("http")
async def _access(request: Request, call_next):
    resp = await call_next(request)
    p = request.url.path
    if "/api/" in p or p.startswith(ONLINE_PREFIX) or p.startswith("/v/media"):
        _log.info("%s %s -> %s", request.method,
                  (p + ("?" + request.url.query if request.url.query else ""))[:120],
                  resp.status_code)
    return resp


def up_client(req: Request) -> httpx.AsyncClient:
    return req.app.state.up


def ext_client(req: Request) -> httpx.AsyncClient:
    return req.app.state.ext


async def forward_upstream(request: Request) -> Response:
    """原样透传官方后端（走 unix socket）。"""
    client = up_client(request)
    url = "http://localhost" + request.url.path
    if request.url.query:
        url += "?" + request.url.query
    body = await request.body()
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("connection", "content-length", "host")}
    headers["Host"] = "localhost"
    try:
        r = await client.request(request.method, url, headers=headers, content=body)
    except Exception as exc:
        return JSONResponse({"code": -1, "msg": f"upstream {type(exc).__name__}", "data": None},
                            status_code=502)
    resp_headers = {k: v for k, v in r.headers.items()
                    if k.lower() not in ("content-length", "content-encoding",
                                         "transfer-encoding", "connection")}
    return Response(content=r.content, status_code=r.status_code,
                    headers=resp_headers,
                    media_type=r.headers.get("content-type"))


# ------------------------------------------------------------- 在线搜索

def _merge_items(titles: list) -> list:
    """跨源同名合并 + 噪音过滤，生成注入用的搜索结果条目。"""
    maxn = int(config.get().get("max_per_keyword", 12))
    # 跨源同名合并：同 规范化片名+年份 聚成一个条目，每个来源各成一条
    # 播放线路（供播放器「切换线路」）；过滤解说/预告/无直链。
    groups: dict[tuple, dict] = {}
    for t in titles:
        if config.is_junk(t.title, t.remarks):
            continue
        playable = [e for line in t.sources.values() for e in line
                    if e.direct or e.resolve]
        if not playable:
            continue
        key = (_norm_title(t.title), t.year)
        g = groups.get(key)
        if g is None:
            g = {"rep": t, "members": [], "lines": {}}
            groups[key] = g
        g["members"].append(t)
        for lname, eps in t.sources.items():
            play = [e for e in eps if e.direct or e.resolve]
            if not play:
                continue
            lkey = f"{t.provider}::{lname}"
            # 同 key 保留直链更多（更优）者；否则集数更多者
            cur = g["lines"].get(lkey)
            nd_new = sum(1 for e in play if e.direct)
            nd_cur = sum(1 for e in (cur or []) if e.get("direct"))
            if cur is None or nd_new > nd_cur or \
                    (nd_new == nd_cur and len(play) > len(cur)):
                g["lines"][lkey] = [e.__dict__ for e in play]
    # 排序：按可播直链总数（跨线路）降序
    ordered = sorted(groups.values(),
                     key=lambda g: sum(len(v) for v in g["lines"].values()),
                     reverse=True)[:maxn]
    briefs = []
    for g in ordered:
        rep = g["rep"]
        brief = inj.brief_of(rep)
        brief["lines"] = g["lines"]
        briefs.append(brief)
    briefs = store.remember_merged(briefs)
    return [inj.search_item(b) for b in briefs]


async def online_search(keyword: str, budget: float = _SEARCH_BUDGET) -> list:
    """在线搜索：最多花 budget 秒，查到多少返回多少，慢源直接放弃。"""
    now = time.time()
    hit = _search_cache.get(keyword)
    if hit:
        ts, items, complete = hit
        if now - ts < (CACHE_TTL if complete else _PARTIAL_TTL):
            return items
    sources = config.enabled_sources()
    titles, complete = await providers.search_all(keyword, sources,
                                                  timeout=budget)
    items = _merge_items(titles)
    if complete or items:
        _search_cache[keyword] = (time.time(), items, complete)
    if not complete:
        _schedule_fill(keyword)
    return items


_bg_fill: set = set()


def _schedule_fill(keyword: str) -> None:
    """这次没抓全：后台把剩下的源跑完，下次搜索直接吃完整缓存。"""
    if keyword in _bg_fill:
        return
    _bg_fill.add(keyword)

    async def work():
        try:
            titles, complete = await providers.search_all(
                keyword, config.enabled_sources(), timeout=15.0)
            if complete:
                items = _merge_items(titles)
                _search_cache[keyword] = (time.time(), items, True)
        except Exception:
            pass
        finally:
            _bg_fill.discard(keyword)

    try:
        task = asyncio.create_task(work())
    except RuntimeError:
        _bg_fill.discard(keyword)
        return
    _bg_fill.add(task)  # 持有引用，避免任务被 GC
    task.add_done_callback(_bg_fill.discard)


def _norm_title(t: str) -> str:
    """跨源同名归并的片名规范化：去空白/标点/常见后缀，便于把不同站的同一部片聚到一起。"""
    t = (t or "").strip().lower()
    # 去掉空格与常见分隔标点
    t = re.sub(r"[\s·・:：,，.。!！?？'’\"\-—_()（）\[\]【】]+", "", t)
    # 去掉年份尾缀（年份单独作为合并键）
    t = re.sub(r"(19|20)\d{2}$", "", t)
    return t


_CJK_RE = re.compile(r"[一-鿿]")
# 拼音输入碎片：含音节分隔符 ' 或 ’（gan、gang't、gang'tie'xia）
_PINYIN_SEP_RE = re.compile(r"['’]")


def _should_online(q: str) -> bool:
    """只对"整词"触发在线搜索：
       - 含中文（输入法上屏后）→ 触发
       - 拼音碎片（含 ' 分隔符，或纯小写短音节 ≤6 字符）→ 不触发
       - 英文整词：含空格的多词，或长度 ≥7 的单词（Inception）→ 触发
    """
    if _CJK_RE.search(q):
        return True
    if _PINYIN_SEP_RE.search(q):
        return False
    if " " in q.strip() and len(q.strip()) >= 4:
        return True
    # 纯单词：拼音音节最长 6（zhuang），用 7 作为英文词门槛
    return len(q) >= 7


@app.get("/v/api/v1/search/list")
async def search_list(request: Request):
    t0 = time.time()
    local = await forward_upstream(request)
    q = request.query_params.get("q", "").strip()
    try:
        import json
        data = json.loads(local.body)
    except Exception:
        return local
    if not isinstance(data, dict):
        return local
    items = data.get("data")
    if not isinstance(items, list):
        items = []
    online = []
    if q and _should_online(q):
        # 硬性时间盒：本地 + 在线合计不超过 3 秒，到点查到什么返回什么
        budget = max(0.3, _SEARCH_BUDGET - (time.time() - t0))
        try:
            # 超时由 search_all 内部兜住，这里不再套 wait_for，避免把
            # 已经抓到一半的结果整个取消掉
            online = await online_search(q, budget)
        except Exception:
            online = []
    # 在线片库更齐全，默认把在线结果排到前面；可在管理页关闭
    if config.get().get("online_first", True):
        data["data"] = online + items
    else:
        data["data"] = items + online
    return JSONResponse(data, status_code=200,
                        headers={"Content-Type": "application/json; charset=utf-8"})


# ------------------------------------------------------------- 详情 / 播放

@app.get("/v/api/v1/item/{guid}")
async def item_detail(guid: str, request: Request):
    # 伪造的 Season / Episode 节点
    if store.get_episode(guid):
        ep = store.get_episode(guid)
        data = inj._base_item(
            guid=guid, type="Episode", title=ep["ep_title"],
            parent_guid=ep["season_guid"], parent_title="第 1 季",
            tv_title=ep["title"], season_number=1, episode_number=ep["index"] + 1,
            number_of_seasons=1, can_play=1,
            media_guid=ep_media_guid(guid), video_guid=ep_video_guid(guid))
        return JSONResponse({"code": 0, "msg": "", "data": data})
    from store import season_guid as _sg
    # 反查 season guid
    tv = None
    for g in store._titles:
        if _sg(g) == guid:
            tv = g
            break
    if tv:
        brief = store.get_title_by_guid(tv)
        data = inj._base_item(
            guid=guid, type="Season", title="第 1 季", parent_guid=tv,
            parent_title=brief["title"], tv_title=brief["title"],
            season_number=1, number_of_seasons=1,
            number_of_episodes=store.episode_count(tv), can_play=None,
            poster=inj._poster(brief.get("poster")))
        return JSONResponse({"code": 0, "msg": "", "data": data})
    brief = store.get_title_by_guid(guid)
    if not brief:
        return await forward_upstream(request)
    return JSONResponse({"code": 0, "msg": "", "data": inj.detail_item(brief)})


@app.get("/v/api/v1/season/list/{guid}")
async def season_list_route(guid: str, request: Request):
    brief = store.get_title_by_guid(guid)
    if not brief or not inj.is_tv(brief):
        return await forward_upstream(request)
    return JSONResponse({"code": 0, "msg": "", "data": inj.season_list(brief)})


@app.get("/v/api/v1/episode/list/{guid}")
async def episode_list_route(guid: str, request: Request):
    # guid 可能是伪造的 season guid，也可能是 tv guid
    tv = None
    from store import season_guid as _sg
    for g in store._titles:
        if _sg(g) == guid:
            tv = g
            break
    if not tv:
        brief = store.get_title_by_guid(guid)
        if brief and inj.is_tv(brief):
            tv = guid
    if not tv:
        return await forward_upstream(request)
    return JSONResponse({"code": 0, "msg": "",
                         "data": inj.episode_list(store.get_title_by_guid(tv))})


@app.get("/v/api/v1/stream/list/{guid}")
async def stream_list(guid: str, request: Request):
    ep = store.get_episode(guid)
    target = None
    if ep:
        sl = inj.episode_stream_list(ep)
        target = store.get_episode_target(guid)
    else:
        brief = store.get_title_by_guid(guid)
        if not brief:
            return await forward_upstream(request)
        sl = inj.stream_list(brief)
        if inj.is_tv(brief):
            # 标题播放键实际播放智能续播集，线路菜单也以该集为准
            egs = store.tv_episode_guids(guid)
            if egs:
                target = store.get_episode_target(egs[0])
        else:
            target = store.resolve_media_guid(media_guid(guid))
    quals = _qualities(target) if target else []
    if quals:
        sl["video_streams"][0]["resolution_type"] = quals[0]["resolution"]
        sl["video_streams"][0]["bps"] = quals[0]["bitrate"]
        sl["qualities"] = quals
        sl["cloud_storage_info"] = None
    return JSONResponse({"code": 0, "msg": "", "data": sl})


@app.post("/v/api/v1/play/info")
async def play_info(request: Request):
    import json
    body = json.loads(await request.body() or b"{}")
    guid = body.get("item_guid", "")
    ep = store.get_episode(guid)
    rec = history.get(guid)
    if ep:
        info = inj.episode_play_info(ep)
        if rec:
            info["ts"] = rec.get("ts", 0)
        return JSONResponse({"code": 0, "msg": "", "data": info})
    brief = store.get_title_by_guid(guid)
    if not brief:
        # 季 guid 也可能被当播放键：映射回所属剧集
        for _og in list(store._titles):
            if season_guid(_og) == guid:
                brief = store.get_title_by_guid(_og)
                guid = _og
                break
    if not brief:
        return await forward_upstream(request)
    # 多集（TV）：标题播放键 → 智能续播（没看过=第1集；看过未看完=该集+进度；
    # 已看完=下一集）。返回该集的 play info，后续 stream/play 都按集走。
    if inj.is_tv(brief):
        ep_guids = store.tv_episode_guids(guid)
        if ep_guids:
            target_eg, resume_ts = history.resume_episode(ep_guids)
            if target_eg:
                ep = store.get_episode(target_eg)
                if ep:
                    info = inj.episode_play_info(ep)
                    info["ts"] = resume_ts
                    # 客户端是拿「点进来的 guid」去校验的：保持 guid/type/item 与请求一致，
                    # 只把 media/video 指向要播的那一集（续播逻辑不变）。
                    # 否则 iOS 会认为条目已被移动/删除，弹「文件不存在」。
                    info["guid"] = guid
                    info["type"] = "TV"
                    info["parent_guid"] = ""
                    info["grand_guid"] = guid
                    info["item"] = inj.detail_item(brief)
                    return JSONResponse({"code": 0, "msg": "", "data": info})
    info = inj.play_info(brief)
    if rec:
        info["ts"] = rec.get("ts", 0)  # 续播位置（秒），播放器据此恢复
    return JSONResponse({"code": 0, "msg": "", "data": info})


def _line_choices(target: dict) -> list[dict]:
    """该播放目标可切换的线路：按资源站去重（同一站直链优先于 share 页），
    当前播放线路排第一。"""
    raw = store.target_lines(target)
    cur = target.get("line")
    # 当前线路置顶，其余直链在前、可解析页在后
    raw.sort(key=lambda t: (t["line"] != cur, not t["m3u8"]
                            and not re.search(r"\.mp4(\?|$)", t["url"], re.I),
                            t.get("resolve", False)))
    out, seen_provider, seen_name = [], set(), set()
    for t in raw:
        provider = t["line"].split("::", 1)[0]
        name = _PROVIDER_CN.get(provider, provider)
        if provider in seen_provider:
            continue
        # 极端情况下不同 provider 映射到同名，加序号区分
        if name in seen_name:
            k = 2
            while f"{name}{k}" in seen_name:
                k += 1
            name = f"{name}{k}"
        seen_provider.add(provider)
        seen_name.add(name)
        out.append({**t, "label": name})
    # 保持当前线路第一
    out.sort(key=lambda t: t["line"] != cur)
    return out


def _line_bitrate(line: str, lines: list[str]) -> int:
    # 用 9000 万段的稳定整数编码线路，避免与真实码率（通常<3000万）冲突
    try:
        idx = lines.index(line)
    except ValueError:
        idx = 0
    return 90_000_000 + idx * 10001


def _line_of_bitrate(br, lines: list[str]) -> str | None:
    try:
        b = int(br or 0)
    except (TypeError, ValueError):
        return None
    if 90_000_000 <= b < 91_000_000:
        idx = (b - 90_000_000) // 10001
        if 0 <= idx < len(lines):
            return lines[idx]
    return None


def _qualities(target: dict) -> list[dict]:
    """把每条播放线路表达成播放器「清晰度」菜单里的一个可选项。"""
    lines = _line_choices(target)
    if len(lines) <= 1:
        return []
    out = []
    for i, t in enumerate(lines):
        out.append({
            "resolution": t.get("label") or _friendly_line(t["line"]),
            "bitrate": 90_000_000 + i * 10001,
            "progressive": True,
            "is_m3u8": False,  # 置 False 才不会被菜单项置灰
        })
    return out


@app.post("/v/api/v1/stream")
async def stream_post(request: Request):
    import json
    body = json.loads(await request.body() or b"{}")
    # 播放器有时只带 video_guid（如剧集标题页的「第 1 集」按钮）
    mg = body.get("media_guid") or body.get("video_guid") or ""
    target = store.resolve_media_guid(mg)
    if not target:
        return await forward_upstream(request)
    if target.get("kind") == "episode":
        sl = inj.episode_stream_list(target)
    else:
        brief = store.get_title_by_guid(target["online_guid"])
        sl = inj.stream_list(brief)
    f = sl["files"][0]
    v = sl["video_streams"][0]
    quals = _qualities(target)
    if quals:
        # 默认选中第一条线路，让播放器初始分辨率/码率与之一致
        v["resolution_type"] = quals[0]["resolution"]
        v["bps"] = quals[0]["bitrate"]
    return JSONResponse({"code": 0, "msg": "", "data": {
        "file_stream": f, "video_stream": v,
        "audio_streams": [], "subtitle_streams": [],
        "qualities": quals, "cloud_storage_info": None}})


@app.post("/v/api/v1/play/play")
async def play_play(request: Request):
    import json
    body = json.loads(await request.body() or b"{}")
    # 只在 media_guid 为空时用 video_guid 兜底（标题页大播放键走这条）
    mg = body.get("media_guid") or body.get("video_guid") or ""
    base_target = store.resolve_media_guid(mg)
    if not base_target:
        return await forward_upstream(request)
    # 播放器「清晰度/线路」菜单切换时会带我们编码的 bitrate，据此选线路
    choices = _line_choices(base_target)
    chosen_line = None
    if len(choices) > 1:
        chosen_line = _line_of_bitrate(body.get("bitrate"), [t["line"] for t in choices])
        if chosen_line is None:
            # 播放器没指定线路：选实测最快的源（无数据时保持原线路）
            ranked = sorted(
                choices, key=lambda t: _speed_rank(t["line"].split("::", 1)[0]))
            if ranked and ranked[0]["line"] != base_target.get("line") \
                    and _speed_rank(ranked[0]["line"].split("::", 1)[0]) < \
                    _speed_rank(base_target.get("line", "").split("::", 1)[0]):
                chosen_line = ranked[0]["line"]
    if chosen_line and chosen_line != base_target.get("line"):
        if base_target.get("kind") == "episode" and base_target.get("ep_guid"):
            target = store.get_episode_target(base_target["ep_guid"], chosen_line) or base_target
        else:
            target = store.resolve_episode(
                base_target["online_guid"], chosen_line,
                int(base_target.get("index", 0) or 0))
            if target:
                target = dict(target, online_guid=base_target["online_guid"],
                              ep_guid="", kind="title")
            else:
                target = base_target
    else:
        target = base_target
    await _resolve_if_needed(target, request)
    # mg 已与"标题第一集"或"具体某集"绑定；按源类型返回 HLS 或直链代理
    src_url = target.get("url", "")
    if re.search(r"\.m3u8(\?|$)", src_url, re.I):
        q = urllib.parse.urlencode({"line": target["line"], "index": target["index"]})
        play_link = f"{ONLINE_PREFIX}/{mg}/preset.m3u8?{q}"
        wrapper = "HLS"
    else:
        q = urllib.parse.urlencode({"line": target["line"], "index": target["index"]})
        play_link = f"{ONLINE_PREFIX}/{mg}/direct?{q}"
        wrapper = "MP4"
    return JSONResponse({"code": 0, "msg": "", "data": {
        "audio_guid": "", "audio_index": -1, "hls_time": 6,
        "is_subtitle_external": 0, "media_guid": mg,
        "play_link": play_link, "subtitle_guid": "", "subtitle_index": -1,
        "subtitle_link": "", "video_guid": body.get("video_guid", vg_for(mg)),
        "video_index": 0, "wrapper": wrapper,
        "non_fatal_errno": None, "video_encoder": "",
        "not_supported_resolution": "", "supported_highest_resolution": ""}})


@app.get(f"{ONLINE_PREFIX}/{{mg}}/direct")
async def direct_proxy(mg: str, request: Request):
    """mp4 等非 HLS 直链代理：同源、带 Range 支持（播放器 seek）。"""
    target = store.resolve_media_guid(mg)
    if not target:
        return Response("not found", status_code=404)
    await _resolve_if_needed(target, request)
    src_url = target["url"]
    if re.search(r"\.m3u8(\?|$)", src_url, re.I):
        # 源是 HLS 却走到直链：重定向到 preset
        return RedirectResponse(f"{ONLINE_PREFIX}/{mg}/preset.m3u8", status_code=302)
    fwd = dict(target.get("headers") or {})
    if request.headers.get("range"):
        fwd["Range"] = request.headers["range"]
    try:
        req = ext_client(request).build_request("GET", src_url, headers=fwd)
        r = await ext_client(request).send(req, stream=True)
    except Exception as exc:
        return Response(f"direct fail {type(exc).__name__}", status_code=502)
    drop = {"content-length", "content-encoding", "transfer-encoding",
            "connection", "content-security-policy"}
    h = {k: v for k, v in r.headers.items() if k.lower() not in drop}
    h["Access-Control-Allow-Origin"] = "*"

    async def pump():
        try:
            async for chunk in r.aiter_raw():
                yield chunk
        finally:
            await r.aclose()

    return StreamingResponse(pump(), status_code=r.status_code, headers=h,
                             media_type=r.headers.get("content-type", "video/mp4"))


@app.get("/v/api/v1/media/range/{mg}")
async def media_range(mg: str, request: Request):
    """iOS/移动端播放器直接拉取 media_guid 字节时走这里：
    在线条目无本地文件，改为返回本代理的 HLS 播放列表（绝对路径）。"""
    target = store.resolve_media_guid(mg)
    if not target:
        return await forward_upstream(request)
    await _resolve_if_needed(target, request)
    src_url = target.get("url", "")
    q = urllib.parse.urlencode({"line": target.get("line", ""),
                                "index": target.get("index", 0)})
    if re.search(r"\.m3u8(\?|$)", src_url, re.I):
        master = ("#EXTM3U\n#EXT-X-VERSION:3\n"
                  "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080\n"
                  f"{ONLINE_PREFIX}/{mg}/main.m3u8?{q}\n")
        return Response(master, media_type="application/vnd.apple.mpegurl",
                        headers={"Cache-Control": "no-cache",
                                 "Access-Control-Allow-Origin": "*"})
    return RedirectResponse(f"{ONLINE_PREFIX}/{mg}/direct?{q}", status_code=302)


@app.get("/v/api/v1/media/range/{mg}/{sub:path}")
async def media_range_sub(mg: str, request: Request):
    """同上的子路径（播放列表相对请求）兜底。"""
    if not store.resolve_media_guid(mg):
        return await forward_upstream(request)
    url = f"{ONLINE_PREFIX}/{mg}/{sub}"
    if request.url.query:
        url += "?" + request.url.query
    return RedirectResponse(url, status_code=302)


def vg_for(mg: str) -> str:
    t = store.resolve_media_guid(mg)
    if not t:
        return ""
    if t.get("kind") == "episode":
        return ep_video_guid(t["ep_guid"])
    return video_guid(t["online_guid"])


@app.post("/v/api/v1/person/list/{guid}")
async def person_list(guid: str, request: Request):
    if not store.get_title_by_guid(guid) and not store.get_episode(guid):
        return await forward_upstream(request)
    return JSONResponse({"code": 0, "msg": "", "data": {"total": 0, "list": []}})


@app.post("/v/api/v1/play/record")
async def play_record(request: Request):
    import json
    try:
        body = json.loads(await request.body() or b"{}")
    except Exception:
        body = {}
    ig = body.get("item_guid", "")
    if store.get_title_by_guid(ig) or store.get_episode(ig):
        # 记录在线播放进度到本地（继续观看），不上报官方库
        history.record(body)
        return JSONResponse({"code": 0, "msg": "", "data": True})
    return await forward_upstream(request)


@app.delete("/v/api/v1/play/record")
async def play_record_delete(request: Request):
    import json
    ids = []
    try:
        body = await request.body()
        if body:
            j = json.loads(body)
            if isinstance(j, dict):
                g = j.get("item_guid") or j.get("guid")
                ids = g if isinstance(g, list) else ([g] if g else [])
            elif isinstance(j, list):
                ids = [x.get("item_guid") if isinstance(x, dict) else x for x in j]
    except Exception:
        ids = []
    ids = [g for g in ids if g and (store.get_title_by_guid(g) or store.get_episode(g))]
    if ids:
        history.delete(ids)
        return JSONResponse({"code": 0, "msg": "", "data": True})
    return await forward_upstream(request)


@app.get("/v/api/v1/play/list")
async def play_list(request: Request):
    upstream = await forward_upstream(request)
    try:
        import json
        data = json.loads(upstream.body)
    except Exception:
        return upstream
    local_items = data.get("data") if isinstance(data, dict) else None
    if not isinstance(local_items, list):
        local_items = []

    online_items = []
    for rec in history.all():
        ig = rec.get("item_guid", "")
        ep = store.get_episode(ig)
        brief = store.get_title_by_guid(ig)
        if ep:
            brief = store.get_title_by_guid(ep["online_guid"])
        if not brief:
            continue  # 缓存已淘汰的标题不再展示
        online_items.append(inj.history_item(brief, rec, ep))

    # 在线历史按最近播放排序，置顶；按 guid 与官方去重
    online_items.sort(key=lambda x: x.get("_updated_at", 0), reverse=True)
    seen = {x.get("guid") for x in local_items}
    merged = online_items + [x for x in local_items if x.get("guid") not in
                             {o["guid"] for o in online_items}]
    # 剥掉私有字段
    for o in online_items:
        o.pop("_online", None)
        o.pop("_updated_at", None)
        o.pop("_year", None)
    return JSONResponse({"code": 0, "msg": "", "data": merged},
                        headers={"Content-Type": "application/json; charset=utf-8"})


# media/p 的 RPC（quit/checkPlayLink 等）对在线 playLink 直接回 succ
@app.post("/v/api/v1/media/p")
async def media_p(request: Request):
    import json
    try:
        body = json.loads(await request.body() or b"{}")
    except Exception:
        body = {}
    link = body.get("playLink", "")
    if isinstance(link, str) and link.startswith(ONLINE_PREFIX):
        # 在线播放的清晰度/线路切换（resetQuality）等无官方媒体任务，直接成功
        return JSONResponse({"code": 0, "msg": "",
                             "data": {"reqid": body.get("reqid", ""), "result": "succ"}})
    return await forward_upstream(request)


# ------------------------------------------------------------- HLS 代理

def _pick_episode(mg: str, request: Request) -> dict | None:
    # 优先：mg 已绑定具体某集（TV 选集）或标题第一集
    target = store.resolve_media_guid(mg)
    if target:
        # query 可显式覆盖集序/线路（master 变体、STRM/调试用）
        if request.query_params.get("index") is not None or request.query_params.get("line"):
            try:
                idx = int(request.query_params.get("index", target.get("index", 0)))
            except ValueError:
                idx = target.get("index", 0)
            r = store.resolve_episode(
                target["online_guid"], request.query_params.get("line"), idx)
            if r:
                return dict(r, online_guid=target["online_guid"],
                            ep_guid=target.get("ep_guid", ""), kind=target.get("kind"))
        return target
    # 兼容：未在索引中的旧 mg，退回标题解析
    og = store.online_guid_by_media(mg)
    if not og:
        return None
    try:
        index = int(request.query_params.get("index", "0"))
    except ValueError:
        index = 0
    return store.resolve_episode(og, request.query_params.get("line"), index)


def _seg_url(mg: str, absolute: str, qs: str = "") -> str:
    suffix = f"&{qs}" if qs else ""
    return f"{ONLINE_PREFIX}/{mg}/seg?u={b64e(absolute)}{suffix}"


async def _resolve_if_needed(picked: dict, request: Request) -> dict:
    """播放页(/share/<id>)在播放时解析出真实媒体地址（原地更新 picked）。"""
    if picked and picked.get("resolve"):
        real = await providers.resolve_share(
            ext_client(request), picked["url"],
            (picked.get("headers") or {}).get("Referer", ""))
        if real:
            picked["url"] = real
            picked["resolve"] = False
    return picked


@app.get(f"{ONLINE_PREFIX}/{{mg}}/preset.m3u8")
async def hls_preset(mg: str, request: Request):
    picked = _pick_episode(mg, request)
    if not picked:
        return Response("not found", status_code=404)
    await _resolve_if_needed(picked, request)
    # play/play 选定线路后会带 line/index；此时只输出该线路，避免与清晰度菜单重复
    explicit_line = request.query_params.get("line")
    if explicit_line:
        targets = [{"line": picked["line"], "url": picked["url"],
                    "m3u8": bool(re.search(r"\.m3u8(\?|$)", picked["url"], re.I)),
                    "resolve": False}]
    else:
        targets = store.target_lines(picked)
    # 枚举该集/正片在各播放线路下的地址；可解析页先解析
    hls_targets = []
    for t in targets:
        if t.get("resolve"):
            real = await providers.resolve_share(
                ext_client(request), t["url"],
                (t.get("headers") or {}).get("Referer", ""))
            if real:
                t["url"] = real
                t["m3u8"] = bool(re.search(r"\.m3u8(\?|$)", real, re.I))
        if t["m3u8"]:
            hls_targets.append(t)
    if not hls_targets:
        return Response("no hls line", status_code=404)

    idx = int(picked.get("index", 0) or 0)

    def variant_uri(t: dict) -> str:
        q = {"index": idx, "line": t["line"]}
        # 透传可能存在的额外选集参数
        return "main.m3u8?" + urllib.parse.urlencode(q)

    if len(hls_targets) == 1:
        # 单线路：保持原有清晰度写法
        master = ("#EXTM3U\n#EXT-X-VERSION:3\n"
                  "#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080\n"
                  f"{variant_uri(hls_targets[0])}\n")
    else:
        # 多线路：无 RESOLUTION 的变体，飞牛 HLS 播放器呈现为可切换「线路」
        out = ["#EXTM3U", "#EXT-X-VERSION:3"]
        # 按实测速度排序（快在前），快的线路给更大的 BANDWIDTH，
        # 播放器 ABR 会优先选带宽最大的变体 → 默认走快线
        hls_targets = sorted(
            hls_targets,
            key=lambda t: _speed_rank(t["line"].split("::", 1)[0]))
        for i, t in enumerate(hls_targets):
            name = _friendly_line(t["line"])
            # NAME 供支持的播放器显示线路名；BANDWIDTH 差异化避免被合并
            out.append(f'#EXT-X-STREAM-INF:BANDWIDTH={3000000 - i * 100000},'
                       f'PROGRAM-ID=1,NAME="{name}"')
            out.append(variant_uri(t))
        master = "\n".join(out) + "\n"
    return Response(master, media_type="application/vnd.apple.mpegurl")


_PROVIDER_CN = {
    "baofeng": "暴风", "ffzy": "非凡", "lzi": "量子", "wujin": "无尽",
    "youku": "优酷", "guangsu": "光速", "ruyi": "如意", "jisu": "极速",
    "xinlang": "新狼", "dytt": "电影天堂", "s360": "360",
    "fh": "飞花", "lj": "蓝鲸",
}
_GROUP_CN = {"m3u8": "主线路", "yun": "云播", "ff": "非凡", "uk": "优酷",
             "gs": "光速", "bf": "暴风", "lz": "量子"}


def _friendly_line(key: str) -> str:
    """把 'provider::组名' 转成好认的线路名，如 baofeng::bfzym3u8 -> 暴风。"""
    provider, _, group = key.partition("::")
    pcn = _PROVIDER_CN.get(provider, provider)
    if not group:
        return pcn
    if group in _GROUP_CN:
        return f"{pcn}·{_GROUP_CN[group]}"
    stem = group
    for suf in ("zym3u8", "m3u8", "yun"):
        if stem.endswith(suf):
            stem = stem[:-len(suf)]
            break
    stem = stem.upper() if stem.isascii() and stem else ""
    return f"{pcn}·{stem}" if stem else pcn


def _is_media_playlist(text: str) -> bool:
    return "#EXTINF" in text or "#EXT-X-TARGETDURATION" in text


def _first_variant(text: str, base: str) -> str | None:
    """从 master 播放列表里取第一个变体（子 m3u8）的绝对地址。"""
    uri = None
    for raw in text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        uri = s
        break
    if not uri:
        return None
    return urllib.parse.urljoin(base, uri)


@app.get(f"{ONLINE_PREFIX}/{{mg}}/main.m3u8")
async def hls_main(mg: str, request: Request):
    picked = _pick_episode(mg, request)
    if not picked:
        return Response("not found", status_code=404)
    await _resolve_if_needed(picked, request)
    src_url, headers = picked["url"], picked["headers"]
    # 跟随嵌套 master（如 index.m3u8 -> 3000k/hls/mixed.m3u8），最多 4 层
    cur, text = src_url, None
    for _ in range(4):
        try:
            r = await ext_client(request).get(cur, headers=headers)
            if r.status_code != 200:
                return Response(f"upstream {r.status_code}", status_code=502)
            text = r.text
        except Exception as exc:
            return Response(f"fetch fail {type(exc).__name__}", status_code=502)
        if _is_media_playlist(text):
            break
        nxt = _first_variant(text, cur)
        if not nxt or nxt == cur:
            break
        cur = nxt
    base_url = cur  # 分片相对地址以最终媒体列表为基准

    sel_qs = ""
    if picked.get("line"):
        sel_qs = f"line={urllib.parse.quote(picked['line'])}&index={picked['index']}"
    out_lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("#EXT-X-KEY") or line.startswith("#EXT-X-MAP"):
            def repl(m):
                uri = m.group(1)
                absu = urllib.parse.urljoin(base_url, uri)
                return f'URI="{_seg_url(mg, absu, sel_qs)}"'
            raw = re.sub(r'URI="([^"]+)"', repl, raw)
            out_lines.append(raw)
        elif line.startswith("#") or not line:
            out_lines.append(raw)
        else:
            out_lines.append(_seg_url(mg, urllib.parse.urljoin(base_url, line), sel_qs))
    body = "\n".join(out_lines) + "\n"
    return Response(body, media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-cache",
                             "Access-Control-Allow-Origin": "*"})


@app.get(f"{ONLINE_PREFIX}/{{mg}}/seg")
async def hls_seg(mg: str, request: Request):
    u = request.query_params.get("u", "")
    try:
        src_url = b64d(u)
    except Exception:
        return Response("bad u", status_code=400)
    picked = _pick_episode(mg, request)
    headers = picked["headers"] if picked else {}
    # 透传 Range（播放器 seek）
    fwd = dict(headers)
    if "range" in request.headers:
        fwd["Range"] = request.headers["range"]
    t0 = time.monotonic()
    try:
        r = await ext_client(request).get(src_url, headers=fwd)
    except Exception as exc:
        _seg_record(picked["line"].split("::", 1)[0] if picked else "",
                    (time.monotonic() - t0) * 1000, False)
        return Response(f"seg fail {type(exc).__name__}", status_code=502)
    _seg_record(picked["line"].split("::", 1)[0] if picked else "",
                (time.monotonic() - t0) * 1000,
                r.status_code in (200, 206))
    drop = ("content-length", "content-encoding", "transfer-encoding",
            "connection", "content-security-policy")
    h = {k: v for k, v in r.headers.items() if k.lower() not in drop}
    h["Access-Control-Allow-Origin"] = "*"
    return Response(content=r.content, status_code=r.status_code, headers=h)


# ------------------------------------------------------------- 海报代理

def _decode_tok(tok: str) -> str:
    tok = tok.split(".", 1)[0]
    return b64d(tok)


@app.get("/v/api/v1/sys/img/online/{tok_path:path}")
async def online_poster(tok_path: str, request: Request):
    try:
        u = _decode_tok(tok_path)
    except Exception:
        return Response("", status_code=400)
    if not u.startswith("http"):
        return Response("", status_code=400)
    try:
        r = await ext_client(request).get(u, headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return Response("", status_code=502)
    return Response(content=r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type", "image/jpeg"),
                    headers={"Cache-Control": "max-age=86400",
                             "Access-Control-Allow-Origin": "*"})


@app.get("/_ext/healthz")
async def healthz():
    return {"ok": True, "service": "fnmedia-ext", "upstream": _up_status()}


def _up_status() -> str:
    return "unknown"


# ------------------------------------------------------------- 管理 API

@app.get("/v/api/ext/status")
async def ext_status(request: Request):
    return {"ok": True,
            "config": {k: v for k, v in config.get().items() if k != "admin_token"},
            "cached_titles": len(store._titles)}


@app.get("/v/api/ext/online")
async def ext_online(q: str = "", limit: int = 200):
    items = []
    for g, b in store._titles.items():
        items.append({
            "guid": g, "title": b.get("title"), "year": b.get("year"),
            "area": b.get("area"), "remarks": b.get("remarks"),
            "score": b.get("score"), "provider": b.get("online_id", "").split(":")[0],
            "episodes": sum(len(e) for e in b.get("lines", {}).values()),
            "poster": b.get("poster"),
        })
    items.sort(key=lambda x: x.get("year") or "", reverse=True)
    if q:
        items = [x for x in items if q.lower() in (x["title"] or "").lower()]
    return {"code": 0, "data": items[:limit]}


@app.post("/v/api/ext/config")
async def ext_set_config(request: Request):
    body = await request.json()
    allowed = {"max_per_keyword", "filter_junk", "junk_keywords", "online_first"}
    patch = {k: v for k, v in body.items() if k in allowed}
    if "sources" in body and isinstance(body["sources"], list):
        cur = {s["id"]: s for s in config.all_sources()}
        for ch in body["sources"]:
            sid = ch.get("id")
            if sid in cur and isinstance(ch.get("enabled"), bool):
                cur[sid]["enabled"] = ch["enabled"]
        patch["sources"] = list(cur.values())
    config.save(patch)
    _search_cache.clear()
    return {"code": 0, "msg": "saved"}


@app.post("/v/api/ext/junk-keywords")
async def ext_junk_keywords(request: Request):
    body = await request.json()
    if body.get("reset"):
        kws = config.reset_junk_keywords()
    else:
        kws = config.set_junk_keywords(body.get("keywords", []))
    _search_cache.clear()
    return {"code": 0, "msg": "saved", "data": {"junk_keywords": kws}}


@app.post("/v/api/ext/source/add")
async def ext_source_add(request: Request):
    body = await request.json()
    ok, info = config.add_source(body.get("name", ""), body.get("api", ""),
                                 bool(body.get("enabled", True)))
    if not ok:
        return JSONResponse({"code": 400, "msg": info}, status_code=400)
    _search_cache.clear()
    return {"code": 0, "msg": "added", "data": {"id": info}}


@app.post("/v/api/ext/source/delete")
async def ext_source_delete(request: Request):
    body = await request.json()
    ok, info = config.remove_source(body.get("id", ""))
    if not ok:
        return JSONResponse({"code": 404, "msg": info}, status_code=404)
    _search_cache.clear()
    return {"code": 0, "msg": "deleted"}


@app.post("/v/api/ext/source/restore")
async def ext_source_restore(request: Request):
    config.restore_builtin()
    _search_cache.clear()
    return {"code": 0, "msg": "restored"}


@app.post("/v/api/ext/source/test")
async def ext_source_test(request: Request):
    body = await request.json()
    api = (body.get("api") or "").strip()
    sid = body.get("id")
    if not api and sid:
        src = next((s for s in config.all_sources() if s["id"] == sid), None)
        if not src:
            return JSONResponse({"code": 404, "msg": "源不存在"}, status_code=404)
        api = src["api"]
    keyword = (body.get("keyword") or "庆余年").strip()
    result = await providers.probe_source(api, keyword=keyword)
    return {"code": 0, "msg": "", "data": result}


@app.post("/v/api/ext/source/test-all")
async def ext_source_test_all(request: Request):
    """并发探测全部源，返回 [{id,name,...result}]。"""
    async def one(s):
        r = await providers.probe_source(s["api"])
        r.update(id=s["id"], name=s["name"], enabled=bool(s.get("enabled")),
                 builtin=bool(s.get("builtin")))
        return r
    results = await asyncio.gather(*[one(s) for s in config.all_sources()],
                                   return_exceptions=True)
    out = [r for r in results if isinstance(r, dict)]
    return {"code": 0, "msg": "", "data": out}


@app.post("/v/api/ext/health/run")
async def ext_health_run(request: Request):
    """立即体检一次（可选推送），返回报告。"""
    import json as _json
    body = _json.loads(await request.body() or b"{}")
    report = await health.run_check(push=bool(body.get("push", False)))
    return {"code": 0, "msg": "", "data": report}


@app.get("/v/api/ext/health/report")
async def ext_health_report():
    st = health._load_state()
    return {"code": 0, "msg": "", "data": st.get("last_report")}


@app.get("/v/api/ext/seg-stats")
async def ext_seg_stats():
    """各源分片实测速度（EWMA 毫秒），供管理页展示/调试。"""
    return {"code": 0, "msg": "", "data": _seg_stats}


@app.get("/v/online-admin")
async def admin_page():
    from admin_ui import ADMIN_HTML
    return Response(ADMIN_HTML, media_type="text/html; charset=utf-8")


# ------------------------------------------------------------- 兜底透传

@app.api_route("/{full_path:path}",
               methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def catch_all(request: Request, full_path: str):
    return await forward_upstream(request)
