#!/usr/bin/env bash
# fork-update.sh — 从 CZtheHusky/sub2api 拉取最新 fork release（上游 sub2api main + 降智检测），
# 更新本机实例；失败自动回滚。配合 fork-update.timer 每日运行。
#
# 实例清单: /etc/sub2api-fork-instances.conf，每行一个实例：
#   <安装目录>:<systemd服务名>:<回环端口>
#   例: /opt/sub2api:sub2api.service:8080
set -euo pipefail

REPO="CZtheHusky/sub2api"
CONF="${FORK_UPDATE_CONF:-/etc/sub2api-fork-instances.conf}"

tag=$(curl -sf --connect-timeout 15 --max-time 30 "https://api.github.com/repos/${REPO}/releases/latest" \
  | grep -oE '"tag_name": *"[^"]+"' | head -1 | sed -E 's/.*"([^"]+)"$/\1/' || true)
if [ -z "${tag:-}" ]; then echo "无法获取最新 release，跳过"; exit 0; fi
ver="${tag#v}"
echo "最新 fork release: ${tag}"

fail=0
while IFS=: read -r dir svc port; do
  case "${dir:-}" in ''|\#*) continue;; esac
  bin="${dir}/sub2api"
  if [ ! -x "${bin}" ]; then echo "[${svc}] ${bin} 不存在，跳过"; fail=1; continue; fi

  cur=$("${bin}" -version 2>/dev/null | grep -oE 'Sub2API [0-9][^ ]*' | head -1 | awk '{print $2}' || echo unknown)
  echo "[${svc}] 当前 ${cur:-unknown} / 最新 ${ver}"
  if [ "${cur:-}" = "${ver}" ]; then echo "[${svc}] 已是最新，无需更新"; continue; fi

  tmp=$(mktemp -d)
  asset="sub2api_${ver}_linux_amd64.tar.gz"
  if ! curl -sfL --retry 5 --retry-delay 3 --retry-all-errors -o "${tmp}/${asset}" "https://github.com/${REPO}/releases/download/${tag}/${asset}" \
    || ! curl -sfL --retry 3 -o "${tmp}/checksums.txt" "https://github.com/${REPO}/releases/download/${tag}/checksums.txt" \
    || ! (cd "${tmp}" && grep "${asset}\$" checksums.txt | sha256sum -c - >/dev/null) \
    || ! tar -xzf "${tmp}/${asset}" -C "${tmp}"; then
    echo "[${svc}] 下载/校验失败，跳过该实例"
    rm -rf "${tmp}"; fail=1; continue
  fi

  systemctl stop "${svc}"
  cp -f "${bin}" "${bin}.bak"
  install -m 755 -o sub2api -g sub2api "${tmp}/sub2api" "${bin}"
  rm -rf "${tmp}"
  systemctl start "${svc}"
  sleep 3
  if systemctl is-active --quiet "${svc}" && curl -sf --max-time 10 -o /dev/null "http://127.0.0.1:${port}/"; then
    echo "[${svc}] 升级完成: ${cur} -> ${ver}（备份: ${bin}.bak）"
  else
    echo "[${svc}] 升级后服务异常，自动回滚到 ${cur}"
    install -m 755 -o sub2api -g sub2api "${bin}.bak" "${bin}"
    systemctl restart "${svc}"
    fail=1
  fi
done < "${CONF}"

exit "${fail}"
