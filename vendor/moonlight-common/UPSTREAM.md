# moonlight-common local override

This directory contains the `moonlight-common` Rust crate from upstream commit
`df9f1e3003fb4834dbb17a4bd4d3cf25d2fea3d9`.

It is kept as a narrow local Cargo override because the upstream C video
adapter converts `presentationTimeUs` through a nanosecond/90 kHz expression
that overflows `u64` after roughly five hours. The local change constructs the
timestamp directly with `Duration::from_micros`, preserving the raw C value.

The `moonlight-common-sys` dependency remains pinned to the same upstream Git
revision, so the C sources are not duplicated here.

When upstream fixes the adapter, remove this directory, restore the workspace
dependency to the Git revision containing the fix, and retain the boundary
regression test upstream.
