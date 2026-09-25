#!/usr/bin/env bash
# Starts a Cloudflare quick tunnel for Moonlight Web and points the
# redirect.bari2d.dev CNAME at the generated *.trycloudflare.com hostname.
#
# Credentials live in ~/.config/moonlight-web/cloudflare.env:
#   CF_API_TOKEN=...          (Zone:DNS:Edit on bari2d.dev)
#   CF_ZONE_NAME=bari2d.dev   (optional)
#   CF_RECORD_NAME=redirect.bari2d.dev (optional)
#   TUNNEL_TARGET=http://127.0.0.1:8080 (optional)
#   CF_TUNNEL_TOKEN=...       (optional) token of a NAMED tunnel created in the
#                             Cloudflare dashboard (Zero Trust -> Networks ->
#                             Tunnels). With it the hostname never changes, no
#                             DNS/redirect juggling happens, and cloudflared
#                             reconnects by itself after suspend or an outage.
set -uo pipefail

ENV_FILE="${MOONLIGHT_TUNNEL_ENV:-$HOME/.config/moonlight-web/cloudflare.env}"
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"

CF_ZONE_NAME="${CF_ZONE_NAME:-bari2d.dev}"
CF_RECORD_NAME="${CF_RECORD_NAME:-redirect.bari2d.dev}"
TUNNEL_TARGET="${TUNNEL_TARGET:-http://127.0.0.1:8080}"
CF_API="https://api.cloudflare.com/client/v4"

log() { echo "[moonlight-tunnel] $*" >&2; }

cf() {
    local method="$1" path="$2"; shift 2
    curl -sS -X "$method" "$CF_API$path" \
        -H "Authorization: Bearer $CF_API_TOKEN" \
        -H "Content-Type: application/json" "$@"
}

update_dns() {
    local tunnel_host="$1"
    if [[ -z "${CF_API_TOKEN:-}" ]]; then
        log "CF_API_TOKEN not set in $ENV_FILE; skipping DNS update"
        return
    fi

    local zone_id record body resp
    zone_id=$(cf GET "/zones?name=$CF_ZONE_NAME" | jq -r '.result[0].id // empty')
    if [[ -z "$zone_id" ]]; then
        log "could not find zone $CF_ZONE_NAME"
        return
    fi

    record=$(cf GET "/zones/$zone_id/dns_records?name=$CF_RECORD_NAME" | jq -c '.result[0] // empty')
    body=$(jq -nc --arg n "$CF_RECORD_NAME" --arg c "$tunnel_host" \
        '{type:"CNAME", name:$n, content:$c, ttl:60, proxied:false}')

    if [[ -z "$record" ]]; then
        resp=$(cf POST "/zones/$zone_id/dns_records" --data "$body")
    else
        resp=$(cf PUT "/zones/$zone_id/dns_records/$(jq -r .id <<<"$record")" --data "$body")
    fi

    if [[ $(jq -r .success <<<"$resp") == "true" ]]; then
        log "$CF_RECORD_NAME -> $tunnel_host"
    else
        log "DNS update failed: $(jq -c .errors <<<"$resp")"
    fi

    update_redirect_rule "$zone_id" "$tunnel_host"
    update_wt_ip "$zone_id"
}

# WebTransport connects directly (UDP 443) to wt.bari2d.dev, so keep that
# DNS-only record on the current public IP.
update_wt_ip() {
    local zone_id="$1" name="${WT_PUBLIC_RECORD:-wt.bari2d.dev}" ip record body resp
    ip=$(curl -s -4 --max-time 10 https://api.ipify.org) || return
    [[ $ip =~ ^[0-9.]+$ ]] || return

    record=$(cf GET "/zones/$zone_id/dns_records?name=$name" | jq -c '.result[0] // empty')
    [[ -n "$record" && $(jq -r .content <<<"$record") == "$ip" ]] && return

    body=$(jq -nc --arg n "$name" --arg c "$ip" '{type:"A", name:$n, content:$c, ttl:60, proxied:false}')
    if [[ -z "$record" ]]; then
        resp=$(cf POST "/zones/$zone_id/dns_records" --data "$body")
    else
        resp=$(cf PUT "/zones/$zone_id/dns_records/$(jq -r .id <<<"$record")" --data "$body")
    fi
    [[ $(jq -r .success <<<"$resp") == "true" ]] && log "$name -> $ip"
}

# The zone's Single Redirect rule for $CF_RECORD_NAME is what visitors actually
# hit; point its target at the new tunnel. Needs "Single Redirect: Edit".
update_redirect_rule() {
    local zone_id="$1" tunnel_host="$2" phase="http_request_dynamic_redirect" ruleset rule resp
    ruleset=$(cf GET "/zones/$zone_id/rulesets/phases/$phase/entrypoint")
    if [[ $(jq -r .success <<<"$ruleset") != "true" ]]; then
        log "redirect rule not updated: $(jq -c .errors <<<"$ruleset")"
        return
    fi

    rule=$(jq -c --arg h "$CF_RECORD_NAME" \
        '[.result.rules[]? | select(.expression | contains($h))][0] // empty' <<<"$ruleset")
    if [[ -z "$rule" ]]; then
        log "no redirect rule mentions $CF_RECORD_NAME"
        return
    fi

    rule=$(jq -c --arg u "https://$tunnel_host" '
        del(.version, .last_updated, .ref)
        | .action_parameters.from_value.target_url = {value: $u}' <<<"$rule")
    resp=$(cf PATCH "/zones/$zone_id/rulesets/$(jq -r .result.id <<<"$ruleset")/rules/$(jq -r .id <<<"$rule")" --data "$rule")

    if [[ $(jq -r .success <<<"$resp") == "true" ]]; then
        log "redirect rule -> https://$tunnel_host"
    else
        log "redirect rule update failed: $(jq -c .errors <<<"$resp")"
    fi
}

URL_FILE="${XDG_RUNTIME_DIR:-/tmp}/moonlight-tunnel-url"
rm -f "$URL_FILE"

# Named tunnel: stable hostname, cloudflared handles every reconnect itself.
if [[ -n "${CF_TUNNEL_TOKEN:-}" ]]; then
    log "running named tunnel (stable hostname)"
    echo "https://$CF_RECORD_NAME" > "$URL_FILE"
    exec cloudflared tunnel --no-autoupdate run --token "$CF_TUNNEL_TOKEN"
fi

# A quick tunnel can stay "running" after the edge has dropped it. Every 10s,
# probe the public URL; after 3 straight failures (~30 s) while the local
# server is healthy, kill this service so systemd restarts it with a fresh
# tunnel. If the local server itself is unresponsive, restart that instead.
watchdog() {
    local public_fails=0 local_fails=0 url
    sleep 30
    while sleep 10; do
        if ! curl -sf -o /dev/null --max-time 5 "$TUNNEL_TARGET/"; then
            public_fails=0
            if (( ++local_fails >= 6 )); then
                log "moonlight-web unresponsive; restarting it"
                systemctl --user restart moonlight-web.service
                local_fails=0
            fi
            continue
        fi
        local_fails=0

        url=$(cat "$URL_FILE" 2>/dev/null) || continue
        if curl -sf -o /dev/null --max-time 8 "$url/"; then
            public_fails=0
        elif (( ++public_fails >= 3 )); then
            log "tunnel $url unreachable; restarting"
            kill -TERM 0
        fi
    done
}
watchdog &

# cloudflared prints the quick-tunnel URL to stderr; watch for it and update
# DNS each time a new one appears (cloudflared may reconnect with a new host).
cloudflared tunnel --no-autoupdate --url "$TUNNEL_TARGET" 2>&1 | while IFS= read -r line; do
    echo "$line"
    if [[ $line =~ https://([a-z0-9-]+\.trycloudflare\.com) && ${BASH_REMATCH[1]} != api.trycloudflare.com ]]; then
        host="${BASH_REMATCH[1]}"
        echo "https://$host" > "$URL_FILE"
        log "tunnel up: https://$host"
        update_dns "$host"
    fi
done

# cloudflared exited; take the watchdog down too and let systemd restart us.
log "cloudflared exited; restarting"
kill -TERM 0
