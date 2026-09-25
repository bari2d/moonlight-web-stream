#!/usr/bin/env bash
# Issues/renews the Let's Encrypt cert for the WebTransport hostnames via a
# Cloudflare DNS-01 challenge, then restarts moonlight-web if it changed.
# Uses CF_API_TOKEN from ~/.config/moonlight-web/cloudflare.env.
set -euo pipefail

ENV_FILE="${MOONLIGHT_TUNNEL_ENV:-$HOME/.config/moonlight-web/cloudflare.env}"
source "$ENV_FILE"

WT_DOMAINS="${WT_DOMAINS:-wt.bari2d.dev,wt-lan.bari2d.dev}"
LEGO_DIR="$HOME/.config/moonlight-web/lego"
CERT="$LEGO_DIR/certificates/${WT_DOMAINS%%,*}.crt"

before=$(sha256sum "$CERT" 2>/dev/null || true)

CF_DNS_API_TOKEN="$CF_API_TOKEN" lego run \
    --accept-tos --dns cloudflare --domains "$WT_DOMAINS" \
    --path "$LEGO_DIR" --renew-days 30 \
    --dns.resolvers 1.1.1.1:53,1.0.0.1:53

if [[ "$(sha256sum "$CERT")" != "$before" ]]; then
    echo "certificate changed; restarting moonlight-web"
    systemctl --user restart moonlight-web.service || true
fi
