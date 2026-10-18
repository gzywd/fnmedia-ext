#!/bin/bash
# fnmedia-ext 内存守护：任一相关进程 RSS 超过 700MB →
#   1. 写诊断日志（供后续排查）  2. pushplus 微信推送  3. 重启服务
# 由 fnmedia-memguard.timer 每分钟触发。
# 环境变量可覆盖（测试用）：FNME_MEM_THRESHOLD_MB / FNME_MEM_COOLDOWN_SEC
set -u

THRESHOLD_MB="${FNME_MEM_THRESHOLD_MB:-700}"
COOLDOWN_SEC="${FNME_MEM_COOLDOWN_SEC:-600}"
UNIT="fnmedia-ext.service"
REPO="/vol1/@appcenter/fnmedia-ext/repo"
LOG_DIR="$REPO/logs/memguard"
STATE_DIR="$REPO/state"
LAST_FILE="$STATE_DIR/memguard_last_action"
PUSH_TOKEN="497749699db34cea8f6a1ede1a700db3"

mkdir -p "$LOG_DIR" "$STATE_DIR"

# ---- 找 RSS 最大的 fnmedia-ext 相关进程（uvicorn 子进程才是内存大头）----
MAX_RSS_KB=0
MAX_PID=""
MAX_CMD=""
for pid in $(pgrep -f "fnmedia-ext" 2>/dev/null); do
    [ -r "/proc/$pid/status" ] || continue
    rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null)
    [ -n "${rss:-}" ] || continue
    if [ "$rss" -gt "$MAX_RSS_KB" ]; then
        MAX_RSS_KB=$rss
        MAX_PID=$pid
        MAX_CMD=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | head -c 120)
    fi
done

[ -n "$MAX_PID" ] || exit 0
MAX_RSS_MB=$((MAX_RSS_KB / 1024))
[ "$MAX_RSS_MB" -ge "$THRESHOLD_MB" ] || exit 0

# ---- 冷却：10 分钟内只记录不动作，防重启循环 ----
NOW=$(date +%s)
ACTION="full"
if [ -f "$LAST_FILE" ]; then
    LAST=$(cat "$LAST_FILE" 2>/dev/null || echo 0)
    if [ $((NOW - LAST)) -lt "$COOLDOWN_SEC" ]; then
        ACTION="log_only"
    fi
fi

STAMP=$(date +%Y%m%d_%H%M%S)
LOG="$LOG_DIR/mem_${STAMP}.log"

{
    echo "=== fnmedia-ext 内存守护触发 $STAMP ==="
    echo "触发进程: pid=$MAX_PID RSS=${MAX_RSS_MB}MB (阈值 ${THRESHOLD_MB}MB)"
    echo "cmdline: $MAX_CMD"
    echo "动作: $ACTION（冷却 ${COOLDOWN_SEC}s）"
    echo
    echo "--- /proc/$MAX_PID/status 摘要 ---"
    grep -E "^(Name|State|Threads|VmPeak|VmSize|VmRSS|VmSwap|voluntary)" "/proc/$MAX_PID/status" 2>/dev/null
    echo
    echo "--- 全部 fnmedia 进程 ---"
    ps -eo pid,rss,etime,nlwp,cmd | grep -E "fnmedia|uvicorn" | grep -v grep
    echo
    echo "--- 系统内存 ---"
    free -m
    echo
    echo "--- 内存 Top15 ---"
    ps -eo pid,rss,comm --sort=-rss | head -16
    echo
    echo "--- 网络连接（ESTABLISHED 计数）---"
    ss -tnp 2>/dev/null | grep -c ESTAB
    ss -tnp 2>/dev/null | grep python | head -10
    echo
    echo "--- journalctl 最近 300 行 ---"
    journalctl -u "$UNIT" -n 300 --no-pager 2>/dev/null | tail -300
    echo
    echo "--- 系统 OOM 记录（若有）---"
    journalctl --no-pager 2>/dev/null | grep -i "killed process\|out of memory" | tail -5
} > "$LOG" 2>&1

# 日志滚动：只留最近 20 份
ls -1t "$LOG_DIR"/mem_*.log 2>/dev/null | tail -n +21 | xargs -r rm -f

TITLE="fnmedia-ext 内存超限 ${MAX_RSS_MB}MB"
CONTENT="时间: $STAMP<br>进程: pid=$MAX_PID (${MAX_CMD:0:60})<br>RSS: ${MAX_RSS_MB}MB / 阈值 ${THRESHOLD_MB}MB<br>诊断日志: $LOG<br>"
if [ "$ACTION" = "full" ]; then
    CONTENT="${CONTENT}已执行: systemctl restart fnmedia-ext<br>请播放测试观察是否复现。"
else
    CONTENT="${CONTENT}冷却期内(10min)，仅记录未重启。"
fi

curl -s -m 15 -X POST "http://www.pushplus.plus/send" \
    -H "Content-Type: application/json" \
    -d "{\"token\":\"$PUSH_TOKEN\",\"title\":\"$TITLE\",\"content\":\"$CONTENT\",\"template\":\"html\"}" \
    >/dev/null 2>&1

echo "$(date '+%F %T') RSS=${MAX_RSS_MB}MB action=$ACTION log=$LOG" >> "$LOG_DIR/memguard_history.log"

if [ "$ACTION" = "full" ]; then
    if systemctl restart "$UNIT"; then
        echo "$NOW" > "$LAST_FILE"
        echo "$(date '+%F %T') 重启成功" >> "$LOG_DIR/memguard_history.log"
    else
        echo "$(date '+%F %T') 重启失败！需人工介入" >> "$LOG_DIR/memguard_history.log"
    fi
fi
