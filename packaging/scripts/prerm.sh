#!/bin/sh
set -e

# Stop only on removal; an upgrade keeps the running ntlb (see postinst.sh)
if [ "$1" = "remove" ] && [ -d /run/systemd/system ]; then
  systemctl stop ntlb.service >/dev/null 2>&1 || true
  systemctl disable ntlb.service >/dev/null 2>&1 || true
fi
