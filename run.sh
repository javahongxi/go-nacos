#!/usr/bin/env bash
# go-nacos 本地启动脚本（开发用，非生产部署）。端口对齐 Java Nacos：
# 主端口 8848 → gRPC +1000=9848（SDK + 节点间内部 RPC 复用），raft -1000=7848；控制台 8080（Nacos 3.x）。
#
#   ./run.sh              单机，默认端口：gRPC 9848 / raft 7848 / 控制台 8080
#   ./run.sh cluster [n]  本机 n 节点集群（默认 3），每节点全部端口 +10：
#                         node1 控制台 8080 / gRPC 9848，node2 8090 / 9858，node3 8100 / 9868 ...
#
# 集群 Ctrl-C 一次性优雅停止全部节点；数据在 data-cluster/nodeN 持久化，
# 重跑即原身份恢复（节点身份 = gRPC 地址，端口固定）。
# 注意：单机与集群不要同时跑（端口完全重叠）。
set -euo pipefail
cd "$(dirname "$0")"

BIN=bin/gonacos
HOST=127.0.0.1

usage() {
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

build() {
    mkdir -p bin
    go build -o "$BIN" ./cmd/gonacos
}

# wait_admin URL：轮询控制台直到节点就绪（最多 15s）。
wait_admin() {
    for _ in $(seq 1 150); do
        if curl -sf -o /dev/null "$1/api/overview"; then
            return 0
        fi
        sleep 0.1
    done
    echo "节点未就绪: $1" >&2
    return 1
}

single() {
    build
    echo "单机启动：SDK $HOST:9848  控制台 http://$HOST:8080  （Ctrl-C 停止）"
    exec "$BIN" -data-dir ./data
}

cluster() {
    local n=${1:-3}
    [[ "$n" =~ ^[0-9]+$ ]] && [ "$n" -ge 1 ] && [ "$n" -le 9 ] || usage
    build
    mkdir -p logs

    local pids=()
    cleanup() {
        trap - INT TERM
        echo ""
        echo "停止集群..."
        if [ "${#pids[@]}" -gt 0 ]; then
            for pid in "${pids[@]}"; do
                kill "$pid" 2>/dev/null || true
            done
        fi
        wait 2>/dev/null || true
    }
    trap cleanup INT TERM

    local i off join
    for i in $(seq 1 "$n"); do
        off=$((10 * (i - 1))) # 每节点全部端口 +10，保持 Nacos 偏移关系
        local dir="data-cluster/node$i"
        local log="logs/node$i.log"
        join=""
        if [ "$i" -gt 1 ]; then
            join="127.0.0.1:9848" # 节点1 的 gRPC 地址
        fi
        mkdir -p "$dir"
        "$BIN" \
            -listen "$HOST:$((9848 + off))" \
            -raft-listen "$HOST:$((7848 + off))" \
            -admin-listen "$HOST:$((8080 + off))" \
            -data-dir "$dir" \
            ${join:+-join "$join"} \
            >"$log" 2>&1 &
        pids+=($!)
        echo "node$i 启动：控制台 http://$HOST:$((8080 + off))  gRPC $HOST:$((9848 + off))  日志 $log"
        if [ "$i" -eq 1 ]; then
            # 首节点就绪后再拉其余节点（join 本身也会重试，这里只是让日志干净）
            wait_admin "http://$HOST:$((8080 + off))" || cleanup
        fi
    done

    echo "集群就绪（$n 节点），Ctrl-C 停止全部；数据目录 data-cluster/，重跑即恢复。"
    wait
}

case "${1:-single}" in
    single) single ;;
    cluster) cluster "${2:-3}" ;;
    *) usage ;;
esac