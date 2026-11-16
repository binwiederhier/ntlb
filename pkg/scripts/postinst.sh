#!/bin/sh
set -e

# No restart on upgrade: a restart drains every stream and has all clients of this load balancer
# reconnect at once, so the operator restarts when it suits (systemctl restart ntlb)
if [ "$1" = "configure" ] && [ -d /run/systemd/system ]; then
  systemctl --system daemon-reload >/dev/null || true
fi
