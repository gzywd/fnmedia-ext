#!/usr/bin/env python3
"""资源站自动体检：每日定时测速、连续失败自动停用、pushplus 推送报告。

配置（state/config.json 的 "health" 键，可在管理页 API 修改）：
    enabled        是否启用每日体检（默认 True）
    hour / minute  每天体检时刻（本地时间，默认 04:10）
    fail_threshold 连续失败多少次后自动停用该源（默认 2）
    push           是否推送报告（默认 True）

状态（state/health.json）：每个源的连续失败计数、最近一次报告。
管理 API（在 app.py 里挂载）：
    POST /v/api/ext/health/run    立即体检一次
    GET  /v/api/ext/health/report 最近一次报告
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx

import providers
from config import config

_log = logging.getLogger("fnmedia")

PUSHPLUS_URL = "http://www.pushplus.plus/send"
PUSHPLUS_TOKEN = "497749699db34cea8f6a1ede1a700db3"

_STATE = Path(__file__).resolve().parent.parent / "state" / "health.json"
_lock = asyncio.Lock()


def _load_state() -> dict:
    try:
        return json.loads(_STATE.read_text())
    except Exception:
        return {"fails": {}, "last_report": None, "last_run": 0}


def _save_state(st: dict) -> None:
    try:
        _STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2))
        tmp.replace(_STATE)
    except Exception:
        _log.exception("health state 保存失败")


def _hcfg() -> dict:
    h = config.get().get("health") or {}
    return {
        "enabled": bool(h.get("enabled", True)),
        "hour": int(h.get("hour", 4)),
        "minute": int(h.get("minute", 10)),
        "fail_threshold": max(1, int(h.get("fail_threshold", 2))),
        "push": bool(h.get("push", True)),
    }


async def _push(title: str, html: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=15, trust_env=False) as c:
            await c.post(PUSHPLUS_URL, json={
                "token": PUSHPLUS_TOKEN, "title": title,
                "content": html, "template": "html"})
    except Exception:
        _log.exception("pushplus 推送失败")


async def run_check(push: bool | None = None,
                    auto_disable: bool = True) -> dict:
    """立即体检一轮：并发探测全部源，返回报告 dict。"""
    h = _hcfg()
    if push is None:
        push = h["push"]
    st = _load_state()
    fails: dict = st.get("fails", {})
    threshold = h["fail_threshold"]

    sources = config.all_sources()
    async def one(s: dict) -> dict:
        t0 = time.monotonic()
        try:
            r = await asyncio.wait_for(
                providers.probe_source(s["api"], keyword="庆余年", timeout=12.0),
                timeout=15.0)
        except Exception as exc:
            r = {"ok": False, "count": 0, "elapsed_ms": int((time.monotonic()-t0)*1000),
                 "error": type(exc).__name__}
        r.update({"id": s["id"], "name": s.get("name", s["id"]),
                  "enabled": bool(s.get("enabled"))})
        return r

    results = await asyncio.gather(*[one(s) for s in sources])
    disabled_names: list[str] = []
    for r in results:
        ok = bool(r.get("ok")) and int(r.get("count") or 0) > 0
        r["ok"] = ok
        if ok:
            fails[r["id"]] = 0
        elif r["enabled"]:
            n = int(fails.get(r["id"], 0)) + 1
            fails[r["id"]] = n
            r["fail_streak"] = n
            if auto_disable and n >= threshold:
                config.save({"sources": [
                    dict(x, enabled=False) if x["id"] == r["id"] else dict(x)
                    for x in config.all_sources()]})
                disabled_names.append(r["name"])
                r["auto_disabled"] = True

    report = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "results": results,
        "auto_disabled": disabled_names,
    }
    st["fails"] = fails
    st["last_report"] = report
    st["last_run"] = time.time()
    _save_state(st)

    if push:
        rows = "".join(
            f"<tr><td>{r['name']}</td>"
            f"<td>{'✅' if r['ok'] else '❌'}</td>"
            f"<td>{r.get('elapsed_ms', 0)}ms</td>"
            f"<td>{r.get('count', 0)}</td>"
            f"<td>{'已自动停用' if r.get('auto_disabled') else ('' if r['enabled'] else '(未启用)')}</td>"
            f"<td>{(r.get('error') or '')[:40]}</td></tr>"
            for r in results)
        html = (f"<h3>fnmedia-ext 资源站体检 {report['time']}</h3>"
                f"<table border=1 cellpadding=4 style='border-collapse:collapse'>"
                f"<tr><th>源</th><th>状态</th><th>耗时</th><th>命中</th><th>动作</th><th>错误</th></tr>"
                f"{rows}</table>"
                + (f"<p>🚫 自动停用：{'、'.join(disabled_names)}</p>" if disabled_names else ""))
        await _push("fnmedia-ext 资源站体检报告", html)
    return report


async def loop() -> None:
    """每日定时体检（本地时间 hour:minute）。"""
    while True:
        try:
            h = _hcfg()
            now = datetime.now()
            run_at = now.replace(hour=h["hour"], minute=h["minute"],
                                 second=0, microsecond=0)
            if run_at <= now:
                run_at += timedelta(days=1)
            await asyncio.sleep((run_at - now).total_seconds())
            if not _hcfg()["enabled"]:
                continue
            await run_check()
        except asyncio.CancelledError:
            return
        except Exception:
            _log.exception("health loop 异常")
            await asyncio.sleep(300)
