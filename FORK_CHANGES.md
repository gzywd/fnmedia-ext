# 本地改动说明（gzywd fork）

本仓库是 `yujian0815/fnmedia-ext` 的 fork，用于记录在 NAS 上做的兼容性修补。
上游原版代码保持原样，本文件只说明额外改动。

## 2026-10-17 · 修复 iOS App 无法播放 + 搜索首屏慢

### 1. iOS / 移动端播放报「文件权限不足」

**现象**：iPhone 飞牛 App 搜索到在线影片，点播放立即报「文件权限不足」。
网页端正常。

**原因**：移动端播放器不走 `play/play` 返回的 `play_link`，而是直接请求

```
GET /v/api/v1/media/range/<media_guid>
```

在线条目没有本地文件，该请求被透传给官方影视后端，返回 403，App 把它翻译成「文件权限不足」。

**改动** `proxy/app.py`：

- 新增 `GET /v/api/v1/media/range/{mg}`：在线条目返回同源 HLS 播放列表（绝对路径），
  分片/密钥继续走 `/v/media/online/...` 代理；非 HLS 直链 302 到 `/direct`。
- 新增 `GET /v/api/v1/media/range/{mg}/{sub}`：播放列表相对路径兜底，重定向到对应的在线代理地址。

### 2. 搜索首屏等待过久

**现象**：搜索要等 5-6 秒才出结果，期间页面空白；源多时（8 个）甚至直接搜不到在线资源，
例如「极限挑战」。

**原因**：

1. `search_list` 串行等待在线聚合（`timeout=6.0`），本地结果被一起卡住。
2. 聚合要等**全部源**返回；只要有一个慢源（暴风/极速常 9 秒+）就会整体超时，
   已经抓到的结果被整包丢弃 → 表现为「明明有资源却搜不到」。

**改动**（硬性 3 秒上限，查到多少返回多少）：

- `providers.search_all` 增加 `timeout` 参数：到点立即返回已完成源的结果，
  未完成的请求取消，并返回 `complete` 标记；不再整包丢弃。
- `search_list` 用总预算 `_SEARCH_BUDGET = 2.0s`（再算上本地查询开销，实测 2.4-2.9s）。
- 结果按「完整 / 部分」分别缓存：完整 120s、部分 15s；命中缓存 13ms 返回。
- 部分结果的同时后台再跑一次完整抓取，跑完直接覆盖成完整缓存，
  下次搜索就能拿到全部线路（用户无感）。

> 注意：飞牛原生搜索接口是一次性响应，无法真正流式追加；所以只能在时间盒内
> 返回已有结果，剩余源靠后台补全。

### 3. 搜索偶发 500（InvalidStateError）

**原因**：同一关键词的并发请求共享同一个 `future`，`asyncio.wait_for` 超时会取消该
future；后台任务随后 `set_result` 抛 `InvalidStateError` → 500 → 客户端重试，进一步变慢。

**改动**：

- 用 `asyncio.shield(task)` 保护后台任务，超时不再取消。
- `set_result` 前检查 `fut.done()`。

### 4. 标题页「第 1 集」大按钮提示「文件不存在」

**现象**：剧集/综艺详情页点顶部大播放键报「文件不存在」，点下方选集里的同一集却能播。

**原因**（两处，都在 `proxy/app.py`）：

1. `stream/list` 对非 TV 条目调用 `store.media_guid(guid)`，而 `media_guid` 是模块级函数，
   `Store` 上没有这个方法 → AttributeError → 500 → 客户端报「文件不存在」。
   改成 `media_guid(guid)`。
2. 播放器有时请求 `play/play`、`stream` 时只带 `video_guid`、`media_guid` 留空；
   原逻辑只看 `media_guid`，空值直接透传官方 → 官方找不到该文件。
   现改为 `media_guid or video_guid` 兜底。

### 5. 剧集页大按钮修好后仍报「文件不存在」（play/info 条目一致性）

**原因**：客户端会拿「点进来的 guid」校验播放信息。
- 旧逻辑对 TV 返回的是 `Episode` 的 guid/type/item，与请求的 TV guid 对不上，
  App 判定条目被移动/删除，直接弹「文件不存在」，后续播放请求根本不发生。
- 部分场景还会拿「季 guid」当播放键，旧逻辑不认识季 guid，直接透传官方 → 失败。

**改动**：

- `play/info` 对 TV：保持 `guid/type/item` 与请求一致（TV），只把 `media/video guid`
  指向要播的那一集（智能续播/进度不变）。
- `play/info` 支持季 guid：映射回所属剧集再走智能续播。

### 6. 新功能：资源站自动体检（feature 3）

- 新增 `proxy/health.py`：每日定时（默认 04:10，可配）并发探测全部源，
  pushplus 推送报告；启用的源**连续 2 次**失败自动停用（可在管理页再启用）。
- 配置：`state/config.json` 的 `health` 键（enabled/hour/minute/fail_threshold/push）。
- API：`POST /v/api/ext/health/run`（立即体检，body `{"push":true}` 可推送）、
  `GET /v/api/ext/health/report`（最近报告）。

### 7. 新功能：卡顿自动换线（feature 6）

- 播放分片时实测各源速度（EWMA，持久化在 `state/seg_stats.json`）。
- `play/play` 在播放器未指定线路时，自动选实测最快的源；无数据保持原线路。
- master 播放列表按速度排序，快线给最大 BANDWIDTH（ABR 优先选中快线）。
- API：`GET /v/api/ext/seg-stats` 查看各源实测速度。

### 8. 弹幕匹配：虚拟文件名标准化（配合 danmu-api 容器）

- 在线条目的 `file_name/path` 从 `在线/HD国语.m3u8` 改为
  电影 `剧名.年份.备注.m3u8`、剧集 `剧名.S01E01.集标题.m3u8`，
  使 dandanplay 兼容弹幕服务能按文件名正确匹配。

## 注意

- 只修改 `proxy/app.py`，不动官方二进制 / nginx / 数据库，卸载扩展仍可完整还原。
- 应用中心升级或重装扩展会覆盖这些改动，升级后需要重新应用本仓库的补丁。
