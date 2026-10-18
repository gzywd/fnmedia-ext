#!/usr/bin/env python3
"""豆瓣榜单：每周抓取并本地缓存，供在线搜索注入与管理页展示。

抓取源实测结论（2026-10-18，本机 NAS 直连，httpx trust_env=False）：
  Top250
    https://movie.douban.com/top250?start=N（N=0,25,...,225，共 10 页）
    → 200，text/html ~60KB。只需正常浏览器 UA（无需 Referer）。
  ｜ 走 HTML 解析（stdlib re），字段最全（中文名/原名/年份/评分/海报）。
  热门电影 / 剧集 / 综艺
    a) https://m.douban.com/rexxar/api/v2/subject_collection/<id>/items?start=0&count=50
       → 200，application/json。必须带 Referer: https://m.douban.com/ + 浏览器 UA
         （缺 Referer 会 400/rexxar 拒绝）。✅ 采用
       collection id 实测：
         movie_hot     200 total=30   热门电影（movie_hot_growing 已 404，弃用）
         movie_hot_gaia 200 total=311 备选（返回 50 条，评分较低的新片流）
         tv_hot        200 total=240
         show_hot      200 total=60
         movie_top250  200 total=250（可作 Top250 的 JSON 兜底）
    b) https://movie.douban.com/j/search_subjects?type=...&tag=热门|综艺
       → 200，需 Referer: https://movie.douban.com/ + X-Requested-With: XMLHttpRequest。
         能通但只有 title/rate/cover，无年份、无 total 分页，作为兜底。
    c) movie.douban.com/chart 等 HTML 页：反爬更严且结构易变，仅作最后兜底（未启用）。

缓存：state/charts.json，结构 {"charts": {key: {"fetched_at": ts, "items": [...]}}}。
刷新由外部定时任务触发（本模块不内置定时器）；数据超过 3 天仍可返回旧数据。
"""
from __future__ import annotations

import asyncio
import html as _html
import json
import logging
import re
import time
from pathlib import Path

import httpx

_log = logging.getLogger("fnmedia")

CHARTS = {
    "douban_top250":      {"name": "豆瓣电影Top250", "aliases": ["top250", "豆瓣top250", "豆瓣电影top250"], "subject_type": "Movie"},
    "douban_hot_movie":   {"name": "豆瓣热门电影",   "aliases": ["热门电影", "豆瓣热门电影"],   "subject_type": "Movie"},
    "douban_hot_tv":      {"name": "豆瓣热门剧集",   "aliases": ["热门剧集", "热门电视剧", "豆瓣热门剧集"], "subject_type": "TV"},
    "douban_hot_variety": {"name": "豆瓣热门综艺",   "aliases": ["热门综艺", "豆瓣热门综艺"],   "subject_type": "TV"},
}

# 热门榜单对应的 rexxar collection id（实测可用的）
_HOT_COLLECTIONS = {
    "douban_hot_movie":   "movie_hot",
    "douban_hot_tv":      "tv_hot",
    "douban_hot_variety": "show_hot",
}

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
_H_HTML = {"User-Agent": _UA, "Accept-Language": "zh-CN,zh;q=0.9"}
_H_MOBILE = {"User-Agent": _UA, "Referer": "https://m.douban.com/",
             "Accept": "application/json"}
_H_JSON = {"User-Agent": _UA, "Referer": "https://movie.douban.com/",
           "X-Requested-With": "XMLHttpRequest"}

_STATE = Path(__file__).resolve().parent.parent / "state" / "charts.json"
_CACHE_TTL = 3 * 24 * 3600      # 缓存容忍 3 天
_PAGE_SLEEP = 1.0

_lock = asyncio.Lock()
_cached: dict | None = None
_cached_mtime: float = 0.0

# ── 成人内容硬过滤（绝对红线，与 app.py 的 NSFW_RE 同思路）──────────────
NSFW_RE = re.compile(
    r"伦理|三级|福利|自拍|偷拍|写真|激情|成人|无码|巨乳|丝袜|萝莉|群交|淫|裸聊|"
    r"欲奴|情色|色情|换爱|女优|做爱|床戏|援交|约炮|人妻|少妇|性爱|高潮|内射|"
    r"勃起|榨精|紫薇|露出|强暴|轮奸|卖淫|嫖|妓|风月|一夜情|口交|肛交|自慰", re.I)


# 关键词子串误伤白名单（如《千年女优》是今敏的正经动画电影）
_NSFW_ALLOW = ("千年女优",)


def _is_nsfw(*texts: str) -> bool:
    joined = " ".join(t for t in texts if t)
    if any(w in joined for w in _NSFW_ALLOW):
        return False
    return bool(NSFW_RE.search(joined))


# ── 缓存读写 ────────────────────────────────────────────────────────────
def _load() -> dict:
    global _cached, _cached_mtime
    try:
        mtime = _STATE.stat().st_mtime
    except OSError:
        return {"charts": {}}
    if _cached is not None and mtime == _cached_mtime:
        return _cached
    try:
        data = json.loads(_STATE.read_text())
        if not isinstance(data, dict):
            data = {}
    except Exception:
        _log.exception("charts 缓存解析失败")
        return {"charts": {}}
    data.setdefault("charts", {})
    _cached, _cached_mtime = data, mtime
    return data


def _save(data: dict) -> None:
    global _cached, _cached_mtime
    try:
        _STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        tmp.replace(_STATE)
        _cached_mtime = _STATE.stat().st_mtime
        _cached = data
    except Exception:
        _log.exception("charts 缓存保存失败")


# ── 抓取 ────────────────────────────────────────────────────────────────
def _rating_str(v) -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    return f"{f:.1f}" if f > 0 else ""


def _year_of(text: str) -> str:
    m = re.search(r"(19|20)\d{2}", text or "")
    return m.group(0) if m else ""


async def _fetch_top250(client: httpx.AsyncClient) -> list[dict]:
    """HTML 解析 movie.douban.com/top250，共 10 页，单页失败跳过。"""
    items: list[dict] = []
    for start in range(0, 250, 25):
        try:
            r = await client.get(f"https://movie.douban.com/top250?start={start}",
                                 headers=_H_HTML)
            r.raise_for_status()
        except Exception as exc:
            _log.warning(f"charts top250 start={start} 抓取失败：{exc}")
            await asyncio.sleep(_PAGE_SLEEP)
            continue
        page = 0
        for block in r.text.split('<div class="item">')[1:]:
            m_rank = re.search(r"<em>(\d+)</em>", block)
            m_titles = re.findall(r'<span class="title">([^<]+)</span>', block)
            m_pic = re.search(r'<img[^>]+src="([^"]+)"', block)
            m_rate = re.search(r'<span class="rating_num"[^>]*>([\d.]+)</span>', block)
            if not (m_rank and m_titles):
                continue
            cn = _html.unescape(m_titles[0]).strip()
            en = _html.unescape(m_titles[1]).strip() if len(m_titles) > 1 else ""
            en = re.sub(r"^[/\s]+", "", en)
            year = _year_of(re.search(r"<p[^>]*>(.*?)</p>", block, re.S).group(1)
                            if re.search(r"<p[^>]*>(.*?)</p>", block, re.S) else "")
            items.append({
                "rank": int(m_rank.group(1)),
                "title": cn,
                "raw_title": f"{cn} {en}".strip(),
                "year": year,
                "rating": m_rate.group(1) if m_rate else "",
                "poster": _html.unescape(m_pic.group(1)) if m_pic else "",
                "type": "Movie",
            })
            page += 1
        _log.info(f"charts top250 start={start} 解析 {page} 条")
        if page == 0:
            _log.warning(f"charts top250 start={start} 解析为空，可能被反爬")
        await asyncio.sleep(_PAGE_SLEEP)
    return items


def _rexxar_items(data: dict, stype: str) -> list[dict]:
    out: list[dict] = []
    for idx, x in enumerate(data.get("subject_collection_items") or [], 1):
        title = (x.get("title") or "").strip()
        if not title:
            continue
        pic = x.get("pic") or {}
        cover = x.get("cover") or {}
        poster = pic.get("normal") or pic.get("large") or cover.get("url") or ""
        orig = (x.get("original_title") or "").strip()
        raw = f"{title} {orig}".strip() if orig and orig != title else title
        year = (x.get("year") or "").strip() or _year_of(x.get("card_subtitle") or "")
        out.append({
            "rank": idx,
            "title": title,
            "raw_title": raw,
            "year": year,
            "rating": _rating_str((x.get("rating") or {}).get("value")),
            "poster": poster,
            "type": stype,
        })
    return out


async def _fetch_collection(client: httpx.AsyncClient, cid: str, stype: str) -> list[dict]:
    url = (f"https://m.douban.com/rexxar/api/v2/subject_collection/{cid}"
           f"/items?start=0&count=50")
    r = await client.get(url, headers=_H_MOBILE)
    r.raise_for_status()
    data = r.json()
    items = _rexxar_items(data, stype)
    _log.info(f"charts rexxar {cid} 取到 {len(items)} 条 / total={data.get('total')}")
    return items


async def _fetch_j_subjects(client: httpx.AsyncClient, typ: str, tag: str,
                            stype: str) -> list[dict]:
    """兜底源 b)：movie.douban.com/j/search_subjects。"""
    url = (f"https://movie.douban.com/j/search_subjects?type={typ}"
           f"&tag={tag}&sort=recommend&page_limit=50&page_start=0")
    r = await client.get(url, headers=_H_JSON)
    r.raise_for_status()
    items: list[dict] = []
    for idx, x in enumerate(r.json().get("subjects") or [], 1):
        title = (x.get("title") or "").strip()
        if not title:
            continue
        items.append({
            "rank": idx,
            "title": title,
            "raw_title": title,
            "year": "",
            "rating": _rating_str(x.get("rate")),
            "poster": x.get("cover") or "",
            "type": stype,
        })
    return items


async def _fetch_hot(client: httpx.AsyncClient, key: str) -> list[dict]:
    meta = CHARTS[key]
    cid = _HOT_COLLECTIONS[key]
    try:
        items = await _fetch_collection(client, cid, meta["subject_type"])
        if items:
            return items
        _log.warning(f"charts {key} rexxar 空结果，转兜底源")
    except Exception as exc:
        _log.warning(f"charts {key} rexxar 失败（{exc}），转兜底源")

    typ = "movie" if key == "douban_hot_movie" else "tv"
    tag = "%E7%BB%BC%E8%89%BA" if key == "douban_hot_variety" else "%E7%83%AD%E9%97%A8"
    return await _fetch_j_subjects(client, typ, tag, meta["subject_type"])


# ── 对外接口 ────────────────────────────────────────────────────────────
async def _fetch_chart(client: httpx.AsyncClient, key: str) -> list[dict]:
    if key == "douban_top250":
        raw = await _fetch_top250(client)
    else:
        raw = await _fetch_hot(client, key)
    stype = CHARTS[key]["subject_type"]
    out: list[dict] = []
    for it in raw:
        if _is_nsfw(it.get("title", ""), it.get("raw_title", "")):
            _log.warning(f"[nsfw] charts {key} 剔除成人条目：{it.get('title')}")
            continue
        it["type"] = stype
        out.append(it)
    for i, it in enumerate(out, 1):
        it["rank"] = i
    return out


async def refresh_all(force: bool = False) -> dict:
    """抓全部榜单并写缓存；force=False 时 3 天内且非空的榜单直接跳过。"""
    st = _load()
    charts: dict = st.setdefault("charts", {})
    now = time.time()
    stats: dict = {}
    ok_any = False

    async with _lock:
        async with httpx.AsyncClient(timeout=15, trust_env=False,
                                     follow_redirects=True) as client:
            for key in CHARTS:
                old = charts.get(key) or {}
                age = now - float(old.get("fetched_at") or 0)
                if not force and old.get("items") and age < _CACHE_TTL:
                    stats[key] = {"count": len(old["items"]), "error": None}
                    ok_any = True
                    continue
                try:
                    items = await _fetch_chart(client, key)
                except Exception as exc:
                    _log.exception(f"charts {key} 抓取异常")
                    stats[key] = {"count": len(old.get("items") or []),
                                  "error": f"{type(exc).__name__}: {exc}"[:200]}
                    continue
                if not items:
                    stats[key] = {"count": len(old.get("items") or []),
                                  "error": "empty"}
                    continue
                charts[key] = {"fetched_at": now, "items": items}
                stats[key] = {"count": len(items), "error": None}
                ok_any = True
                _log.info(f"charts {key} 刷新完成：{len(items)} 条")

    _save(st)
    result = {"ok": ok_any, "stats": stats}
    _log.info(f"charts refresh_all force={force} → {result}")
    return result


async def get_chart(key: str) -> list[dict]:
    """取榜单条目：缓存优先；缓存缺失/为空时现场抓一次。"""
    if key not in CHARTS:
        return []
    old = (_load().get("charts") or {}).get(key) or {}
    items = old.get("items") or []
    if items:
        return items
    async with httpx.AsyncClient(timeout=15, trust_env=False,
                                 follow_redirects=True) as client:
        try:
            items = await _fetch_chart(client, key)
        except Exception:
            _log.exception(f"charts {key} 现场抓取失败")
            return []
    if items:
        st = _load()
        st.setdefault("charts", {})[key] = {"fetched_at": time.time(), "items": items}
        _save(st)
    return items


def charts_state() -> dict:
    """{key: {"fetched_at": ts, "count": n}}，供管理页展示。"""
    charts = _load().get("charts") or {}
    return {k: {"fetched_at": float((charts.get(k) or {}).get("fetched_at") or 0),
                "count": len((charts.get(k) or {}).get("items") or [])}
            for k in CHARTS}


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return re.sub(r"[\s\-_·.。，,！!？?：:、/\\]+", "", s)


def match_chart(q: str) -> str | None:
    """搜索词命中榜单名/别名则返回 chart key，否则 None。"""
    n = _norm(q)
    if not n:
        return None
    best: str | None = None
    best_len = 0
    for key, meta in CHARTS.items():
        for name in [meta["name"], *meta["aliases"]]:
            nn = _norm(name)
            if nn and nn in n and len(nn) > best_len:
                best, best_len = key, len(nn)
    return best


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "match":
        print(match_chart(" ".join(sys.argv[2:])))
    else:
        print(json.dumps(asyncio.run(refresh_all("--force" in sys.argv)),
                         ensure_ascii=False, indent=2))
