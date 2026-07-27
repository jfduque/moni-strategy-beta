#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
VERSION=${1:-$(awk -F '"' '/^version =/ { print $2; exit }' "$REPO_ROOT/Cargo.toml")}
ARCH=${MONI_DEB_ARCH:-$(dpkg --print-architecture)}
OUT_DIR=${MONI_DEB_OUT_DIR:-"$REPO_ROOT/target/debian"}
PACKAGE_ROOT="$OUT_DIR/moni-strategy-beta_${VERSION}_${ARCH}"

cargo build --workspace --release
rm -rf "$PACKAGE_ROOT"
install -d "$PACKAGE_ROOT/DEBIAN"
sed -e "s/@VERSION@/$VERSION/g" -e "s/@ARCH@/$ARCH/g" \
    "$SCRIPT_DIR/control" > "$PACKAGE_ROOT/DEBIAN/control"
install -m 0755 "$SCRIPT_DIR/postinst" "$PACKAGE_ROOT/DEBIAN/postinst"
install -m 0755 "$SCRIPT_DIR/postrm" "$PACKAGE_ROOT/DEBIAN/postrm"
install -Dm0755 "$REPO_ROOT/target/release/moni-strategy-beta" \
    "$PACKAGE_ROOT/usr/bin/moni-strategy-beta"
install -Dm0644 "$REPO_ROOT/packaging/systemd/moni-strategy-beta.service" \
    "$PACKAGE_ROOT/lib/systemd/system/moni-strategy-beta.service"
install -Dm0644 "$REPO_ROOT/config.example.toml" \
    "$PACKAGE_ROOT/usr/share/doc/moni-strategy-beta/examples/config.example.toml"
install -Dm0644 "$REPO_ROOT/SPEC.md" \
    "$PACKAGE_ROOT/usr/share/doc/moni-strategy-beta/SPEC.md"
dpkg-deb --build --root-owner-group "$PACKAGE_ROOT" "$OUT_DIR"
