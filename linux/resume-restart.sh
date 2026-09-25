#!/usr/bin/env bash
# Restart the tunnel the moment the machine resumes from suspend, instead of
# waiting for the watchdog. A quick tunnel never survives a suspend, and
# clients kept hitting the dead hostname while DNS caches expired.
set -u
log() { echo "[moonlight-resume] $*" >&2; }
log "watching logind for resume"
gdbus monitor --system --dest org.freedesktop.login1 --object-path /org/freedesktop/login1 2>/dev/null \
| while IFS= read -r line; do
    # PrepareForSleep (true) = going to sleep, (false) = resumed
    if [[ $line == *PrepareForSleep* && $line == *false* ]]; then
        log "resume detected; restarting tunnel"
        sleep 3
        systemctl --user restart moonlight-tunnel.service
    fi
done
log "gdbus monitor exited"
exit 1
