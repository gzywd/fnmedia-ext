#!/usr/bin/env python3
"""fnmedia-ext TCP 中继：0.0.0.0:<port> -> /var/run/trim_media.sock。

豆瓣仓库 strm 直连播放入口（/chartplay/<guid>.m3u8 + HLS 代理）。
字节级转发，不解析 HTTP；路径白名单（仅播放相关）由 app 中间件负责。
"""
from __future__ import annotations

import asyncio
import os
import sys

PORT = int(os.environ.get("FNMEDIA_TCP_PORT", "18930"))
TARGET = sys.argv[1] if len(sys.argv) > 1 else "/var/run/trim_media.sock"


async def pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    try:
        while True:
            data = await r.read(65536)
            if not data:
                break
            w.write(data)
            await w.drain()
    except Exception:
        pass
    finally:
        try:
            w.close()
        except Exception:
            pass


async def handle(cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
    try:
        ur, uw = await asyncio.open_unix_connection(TARGET)
    except Exception:
        try:
            cw.close()
        except Exception:
            pass
        return
    t1 = asyncio.create_task(pipe(cr, uw))
    t2 = asyncio.create_task(pipe(ur, cw))
    await asyncio.gather(t1, t2, return_exceptions=True)
    try:
        uw.close()
    except Exception:
        pass


async def main():
    srv = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print(f"[fnmedia-tcp] listening 0.0.0.0:{PORT} -> {TARGET}", flush=True)
    async with srv:
        await srv.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"[fnmedia-tcp] exit {type(e).__name__}: {e}", flush=True)
