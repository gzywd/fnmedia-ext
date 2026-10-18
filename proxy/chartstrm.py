#!/usr/bin/env python3
"""豆瓣榜单 → strm 文件同步：把 state/charts.json 的榜单写成
飞牛影视可扫描的 strm（影视墙），点海报即走 /chartplay 解析播放。

目录结构（电影库/剧集库分开建，避免综艺被当电影刮削）：
  /vol1/1000/存储空间1/豆瓣仓库/
    电影/豆瓣电影Top250/肖申克的救赎 (1994).strm
    电影/豆瓣热门电影/....strm
    剧集/豆瓣热门剧集/....strm
    剧集/豆瓣热门综艺/....strm
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

PROXY_DIR = Path(__file__).resolve().parent
CHARTS_JSON = PROXY_DIR.parent / "state" / "charts.json"
STRM_DIR = Path("/vol1/1000/存储空间1/豆瓣仓库")
# strm 里写的直连地址（fnmedia-ext TCP 端口，见 supervise.py --port）
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


def sync_strm() -> dict:
    """按当前榜单缓存重建 strm 文件（幂等，多余文件删除）。返回统计。"""
    try:
        data = json.loads(CHARTS_JSON.read_text()).get("charts", {})
    except Exception:
        data = {}
    stats = {}
    for key, info in CHARTS.items():
        group = "剧集" if info.get("subject_type") == "TV" else "电影"
        sub = STRM_DIR / group / _safe(info.get("name", key))
        items = (data.get(key) or {}).get("items", [])
        sub.mkdir(parents=True, exist_ok=True)
        wanted = set()
        for ent in items:
            title = _safe(ent.get("title", ""))
            year = (ent.get("year") or "").strip()
            fname = f"{title} ({year}).strm" if year else f"{title}.strm"
            wanted.add(fname)
            url = f"{BASE_URL}/chartplay/{_guid_of(key, ent)}.m3u8\n"
            f = sub / fname
            try:
                if not f.exists() or f.read_text() != url:
                    f.write_text(url)
            except Exception:
                pass
        # 清理不再在榜上的旧文件
        try:
            for old in sub.glob("*.strm"):
                if old.name not in wanted:
                    old.unlink()
        except Exception:
            pass
        stats[key] = {"dir": str(sub), "count": len(wanted)}
    return stats
