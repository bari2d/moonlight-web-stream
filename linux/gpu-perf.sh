#!/usr/bin/env bash
# Sunshine global prep command: pin AMD GPU clocks high while a stream runs
# (clock ramp-up between frames adds capture/encode latency jitter), and
# return them to "auto" afterwards.
# Usage: gpu-perf.sh on|off
set -u
level=auto
[[ "${1:-}" == "on" ]] && level=high

for f in /sys/class/drm/card*/device/power_dpm_force_performance_level; do
    [[ -e "$f" ]] || continue
    echo "$level" | sudo -n tee "$f" >/dev/null || true
done
