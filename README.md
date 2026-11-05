# hermes

Fast peer-to-peer file transfer over HTTP/3 (QUIC) with multistream parallel downloads.

## Install

**From source** (requires Go 1.26.2+):
```sh
go install github.com/ochaton/hermes@latest
```

**Pre-built binaries** are available on the [releases page](https://github.com/ochaton/hermes/releases).

## Usage

### Share a file

```sh
hermes share --addr <ip:port> <file>
```

Prints a token like `192.168.1.5:4443/a1b2c3d4e5f60718`. Anyone on the network can use it to download the file.

By default the address is `0.0.0.0:4443` and hermes auto-detects the outbound IP to display the correct token.

### Download a file

```sh
hermes download [--streams N] [--conns M] <ip:port/token>
```

Downloads the file using `N` parallel QUIC streams (default 8) spread over `M` QUIC connections (default 2). Each chunk is verified with xxHash64. The file is written atomically — saved only after a full-file checksum passes.

hermes runs with `GOMAXPROCS=1` unless the `GOMAXPROCS` environment variable is set: quic-go's per-packet goroutine handoffs are much cheaper on a single P, which matters most on macOS.

## Example

```sh
# sender
hermes share --addr 0.0.0.0:4443 video.mp4
# prints: 192.168.1.5:4443/a1b2c3d4e5f60718

# receiver
hermes download 192.168.1.5:4443/a1b2c3d4e5f60718
```
