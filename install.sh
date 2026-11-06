#!/bin/sh
set -u
case "$(uname -s)" in
Darwin) os=darwin ;;
Linux) os=linux ;;
*) echo "unsupported system: $(uname -s)"; exit 1 ;;
esac
case "$(uname -m)" in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*) echo "unsupported cpu: $(uname -m)"; exit 1 ;;
esac

tmp=$(mktemp) || exit 1
if ! curl -fsL "https://github.com/ochaton/hermes/releases/download/v0.9.0-ux.1/hermes-$os-$arch" -o "$tmp"; then
	rm -f "$tmp"
	echo "download failed. go install github.com/ochaton/hermes@experiment/ux-send"
	exit 1
fi
chmod +x "$tmp"

dest=
for d in /opt/homebrew/bin /usr/local/bin; do
	case ":$PATH:" in
	*":$d:"*) if [ -w "$d" ] && mv "$tmp" "$d/hermes"; then dest=$d; break; fi ;;
	esac
done
if [ -z "$dest" ] && command -v sudo >/dev/null 2>&1 &&
	sudo mkdir -p /usr/local/bin && sudo mv "$tmp" /usr/local/bin/hermes; then
	dest=/usr/local/bin
fi
if [ -z "$dest" ]; then
	dest=$HOME/.local/bin
	mkdir -p "$dest" && mv "$tmp" "$dest/hermes" || exit 1
	case ":$PATH:" in
	*":$dest:"*) ;;
	*) echo 'export PATH="$HOME/.local/bin:$PATH"' ;;
	esac
fi
echo installed
echo "hermes send <file>"
