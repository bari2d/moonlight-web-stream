# WebTransport endpoint alternatives

The page URL can stay on a Cloudflare Tunnel. Native WebTransport connects
separately to directly reachable UDP/HTTP3 listeners; Quick Tunnel hostnames
are not interchangeable with those listeners.

`web_transport.public_url` remains the primary URL, preserving existing configs
and clients. Add up to five alternatives (six distinct URLs total):

```json
{
  "web_transport": {
    "enabled": true,
    "bind_address": "192.168.1.85:443",
    "additional_bind_addresses": ["192.168.1.85:8443", "192.168.1.85:4443"],
    "public_url": "https://primary.example.com:443/moonlight-web-transport",
    "alternate_public_urls": [
      "https://alternate.example.com:443/moonlight-web-transport",
      "https://primary.example.com:8443/moonlight-web-transport",
      "https://alternate.example.com:8443/moonlight-web-transport",
      "https://primary.example.com:4443/moonlight-web-transport",
      "https://alternate.example.com:4443/moonlight-web-transport"
    ],
    "certificate_pem": "primary.crt",
    "private_key_pem": "primary.key",
    "additional_tls_identities": [{
      "hostnames": ["alternate.example.com"],
      "certificate_pem": "alternate.crt",
      "private_key_pem": "alternate.key"
    }]
  }
}
```

- Every URL must use HTTPS and specify its port explicitly. DNS must resolve
  to the native server/router, not a normal Cloudflare proxy. If using
  Cloudflare DNS, use a separate **DNS-only** record.
- Each hostname needs a browser-trusted certificate. Alternatives use the
  primary certificate unless assigned another identity. A SAN certificate
  covering all hostnames needs no additional identities. Certificate renewals
  require a server restart to reload the files.
- Open the listed UDP ports in the host firewall and forward them on the router.
  TCP port forwarding alone does not expose WebTransport. The example uses
  the same internal and external ports; explicit NAT translations also work.
- All listeners share one origin-bound, expiring, one-use bridge token.
  The first valid CONNECT claims the session; no second media session is made.
- The browser tries the primary immediately, then an alternative every 250 ms.
  Each whole connection attempt has a 3-second deadline, so six blocked
  endpoints add at most approximately 4.25 seconds before existing fallback.
  An early success cancels pending timers and closes losing connections.
  Stopping or replacing a control session aborts the race too.
- Browser logs show each hostname/port and its outcome, without the token.
  Server acceptance logs include the selected authority.

Different ports help only with port-specific filtering; alternate hostnames
can help with name-specific filtering. Neither bypasses blanket UDP/QUIC
blocking. WebSocket fallback remains available through the existing tunnel.

Run `npm run test:transport` for the endpoint-racing tests and
`cargo test -p web-server web_transport` for server tests. Regenerate the API
types with `npm run generate-bindings` before building the browser frontend.

## QUIC tuning (`web_transport.quic`)

The browser-facing QUIC connection can be tuned in the config. Defaults are
chosen for a game stream on a jittery link such as Wi-Fi:

```json
{
  "web_transport": {
    "quic": {
      "congestion_controller": "cubic",
      "initial_window_bytes": 524288,
      "initial_rtt_ms": 50,
      "requested_max_ack_delay_ms": 5,
      "stream_fairness": false
    }
  }
}
```

- `congestion_controller`: `cubic` (default), `bbr` (Quinn marks it
  experimental; keeps pacing through random wireless loss instead of shrinking
  the window), `new_reno`, or `fixed`: a constant window of
  `initial_window_bytes` (1 MiB when `0`) that ignores loss, like native
  Moonlight's uncontrolled RTP. Best on a LAN: an app-limited video stream
  never regrows a window that Wi-Fi loss shrank, so later large frames wait
  extra round trips for ACKs. The encoder bitrate still bounds the send rate.
- `initial_window_bytes`: initial congestion window. Large enough for one key
  frame so it is not paced out over several round trips; Quinn still shrinks
  it on real congestion.
- `initial_rtt_ms`: RTT estimate used before the first measurement.
- `requested_max_ack_delay_ms`: asks the browser (QUIC ACK-frequency
  extension) to acknowledge within this delay so loss is detected sooner.
  Browsers without the extension ignore it. `0` disables the request.
- `stream_fairness`: `false` (default) drains each video frame's stream to
  completion before starting the next one. Delta frames additionally get a
  strictly decreasing priority so in-flight frames are always sent in
  sequence order.

The stream statistics overlay shows the QUIC path RTT next to the
application-level "streamer to browser" RTT. If only the latter jumps, the
browser's main thread is the bottleneck rather than the network.

### Browser-side smoothing

With the canvas renderer enabled, the `Canvas Frame Pacing` setting adds a
small adaptive jitter buffer between decoder and canvas (`balanced`: up to two
frame intervals, `smooth`: up to four). Frames are shown at their original
cadence on the display refresh and a burst after a stall is caught up rather
than replayed. On browsers with a worker `VideoDecoder` and `OffscreenCanvas`
(Chromium, recent Safari and Firefox), the canvas renderer now decodes, paces
and draws inside a worker so a busy main thread cannot delay presentation.
