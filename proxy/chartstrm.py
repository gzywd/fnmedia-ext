#!/usr/bin/env python3
"""豆瓣榜单 → strm 文件同步（含本地 NFO + 豆瓣海报）。

飞牛影视 v0.9+ 支持"优先读取本地 NFO 和图片"（媒体库默认开启），
所以每部片都配 .nfo 元数据 + 豆瓣海报图片，封面不再依赖 TMDB 刮削
（解决《伟大的长征》这类 TMDB 匹配不到的片）。

目录结构：
  电影/<榜单名>/<标题> (年份).strm            单文件（智能续播）
  电影/<榜单名>/<标题> (年份).nfo             本地元数据
  电影/<榜单名>/<标题> (年份)-poster.jpg      豆瓣海报
  剧集/<榜单名>/<标题> (年份)/                已解析的剧集：每集一个 strm
      tvshow.nfo / poster.jpg
      <标题> - S01E01.strm ... S01E{N}.strm   点谁播谁（/chartplay/<guid>/ep<N>）
  未解析/单集的剧集退化为单文件结构（解析后下次同步自动升级为每集结构）
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import urllib.request
from pathlib import Path
import xml.sax.saxutils as _sx

_log = logging.getLogger("fnmedia")

PROXY_DIR = Path(__file__).resolve().parent
CHARTS_JSON = PROXY_DIR.parent / "state" / "charts.json"
STRM_DIR = Path("/vol1/1000/存储空间1/豆瓣仓库")
# strm 里写的直连地址（fnmedia-ext TCP 端口，见 tcp_relay.py）
BASE_URL = "http://192.168.31.95:18930"

CHARTS = {
    "douban_top250":      {"name": "豆瓣电影Top250", "subject_type": "Movie"},
    "douban_hot_movie":   {"name": "豆瓣热门电影",   "subject_type": "Movie"},
    "douban_hot_tv":      {"name": "豆瓣热门剧集",   "subject_type": "TV"},
    "douban_hot_variety": {"name": "豆瓣热门综艺",   "subject_type": "TV"},
}


def _safe(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', " ", name).strip() or "untitled"


def _guid_of(key: str, ent: dict) -> str:
    """与 app._chart_guid 完全一致，保证 strm 链接能被反查。"""
    raw = (ent.get("raw_title") or ent.get("title") or "").strip()
    h = hashlib.md5(f"{key}:{raw}:{ent.get('year', '')}".encode()).hexdigest()[:24]
    return "chart_" + h


def _esc(s) -> str:
    return _sx.escape(str(s or ""))


def _nfo(root: str, title: str, raw: str, year: str, rating: str) -> str:
    return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
            f'<{root}>\n'
            f'  <title>{_esc(title)}</title>\n'
            f'  <originaltitle>{_esc(raw)}</originaltitle>\n'
            f'  <year>{_esc(year)}</year>\n'
            + (f'  <rating>{_esc(rating)}</rating>\n' if rating else "")
            + f'  <id type="douban"></id>\n'
            f'</{root}>\n')


def _save_poster(path: Path, url: str) -> None:
    """下载豆瓣海报到本地（已存在则跳过）。豆瓣图床需带 Referer。"""
    if not url or path.exists():
        return
    try:
        req = urllib.request.Request(url, headers={
            "Referer": "https://movie.douban.com/",
            "User-Agent": "Mozilla/5.0"})
        data = urllib.request.urlopen(req, timeout=10).read()
        if data:
            path.write_bytes(data)
    except Exception as e:
        _log.warning(f"[chartstrm] 海报下载失败: {type(e).__name__} {url[:60]}")


def _resolved_tv(og_key: str, guid: str):
    """返回 (og, 集guid列表)：该条目已解析为多集剧集；否则 (None, [])。"""
    try:
        import app as _app
        from store import store
    except Exception:
        return None, []
    og = _app._chart_map.get(guid)
    brief = store.get_title_by_guid(og) if og else None
    if not brief or not brief.get("lines"):
        return None, []
    return og, store.tv_episode_guids(og)


def _expected_eps(ent: dict) -> int:
    """豆瓣标注的应有集数："更新至8集"/"47集全" -> 8/47；无法解析返回 0。"""
    m = re.search(r"(\d+)\s*集", ent.get("episodes_info") or "")
    return int(m.group(1)) if m else 0


def sync_strm() -> dict:
    """按当前榜单缓存重建 strm/nfo/海报（幂等，多余文件清理）。"""
    try:
        data = json.loads(CHARTS_JSON.read_text()).get("charts", {})
    except Exception:
        data = {}
    stats = {}
    for key, info in CHARTS.items():
        is_tv = info.get("subject_type") == "TV"
        group = "剧集" if is_tv else "电影"
        sub = STRM_DIR / group / _safe(info.get("name", key))
        sub.mkdir(parents=True, exist_ok=True)
        items = (data.get(key) or {}).get("items", [])
        want_files: set[str] = set()
        want_dirs: set[str] = set()
        n_titles = 0
        for ent in items:
            title = _safe(ent.get("title", ""))
            year = (ent.get("year") or "").strip()
            guid = _guid_of(key, ent)
            og, egs = _resolved_tv(key, guid) if is_tv else (None, [])
            if is_tv and len(egs) <= 1:
                # 用户指定：剧集/综艺没解析出多集的（缺集少集）不上墙，
                # 等每周重搜拿到完整资源后自动出现
                continue
            want_eps = _expected_eps(ent)
            if is_tv and want_eps and len(egs) < want_eps:
                # 豆瓣标注应有 N 集、资源站集数不够 -> 缺集，不上墙
                _log.info(f"[chartstrm] 缺集不上墙: {ent.get('title')} "
                          f"应有{want_eps}集 资源站{len(egs)}集")
                continue
            n_titles += 1
            stem = f"{title} ({year})" if year else title
            if is_tv and len(egs) > 1:
                # ---- 剧集结构：每集一个 strm ----
                sd = sub / stem
                want_dirs.add(sd.name)
                sd.mkdir(parents=True, exist_ok=True)
                (sd / "tvshow.nfo").write_text(_nfo(
                    "tvshow", ent.get("title", ""), ent.get("raw_title", ""),
                    year, ent.get("rating", "")))
                _save_poster(sd / "poster.jpg", ent.get("poster", ""))
                ep_files: set[str] = set()
                for i, _eg in enumerate(egs):
                    fn = f"{title} - S01E{i + 1:02d}.strm"
                    ep_files.add(fn)
                    url = f"{BASE_URL}/chartplay/{guid}/ep{i}.m3u8\n"
                    f = sd / fn
                    if not f.exists() or f.read_text() != url:
                        f.write_text(url)
                for old in sd.glob("*.strm"):
                    if old.name not in ep_files:
                        old.unlink()
                # 迁移：清掉旧的平铺单文件结构
                for legacy in (sub / f"{stem}.strm", sub / f"{stem}.nfo",
                               sub / f"{stem}-poster.jpg"):
                    if legacy.exists():
                        legacy.unlink()
            else:
                # ---- 单文件结构（电影 / 未解析剧集）----
                fn = f"{stem}.strm"
                want_files.add(fn)
                url = f"{BASE_URL}/chartplay/{guid}.m3u8\n"
                f = sub / fn
                if not f.exists() or f.read_text() != url:
                    f.write_text(url)
                base = fn[:-5]
                (sub / f"{base}.nfo").write_text(_nfo(
                    "movie", ent.get("title", ""), ent.get("raw_title", ""),
                    year, ent.get("rating", "")))
                want_files.add(f"{base}.nfo")
                pf = f"{base}-poster.jpg"
                want_files.add(pf)
                _save_poster(sub / pf, ent.get("poster", ""))
        # 清理不再在榜上的文件/目录（仅顶层；剧集子目录内部已各自清理）
        try:
            for old in sub.iterdir():
                if old.is_file() and old.suffix in (".strm", ".nfo", ".jpg") \
                        and old.name not in want_files:
                    old.unlink()
                elif old.is_dir() and old.name not in want_dirs:
                    shutil.rmtree(old, ignore_errors=True)
        except Exception:
            pass
        stats[key] = {"dir": str(sub), "count": n_titles}
    return stats
