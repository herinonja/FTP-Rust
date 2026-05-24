# TROOZN Radxa Proxy

Single Rust proxy for the Radxa.

Kodi still talks to the Radxa through a standard FTP endpoint:

```text
Kodi -> ftp://radxa:2120/ -> TROOZN Radxa Proxy
```

Phones no longer expose FTP to the Radxa. They connect to the Radxa with
WebTransport and stay registered while the TROOZN app is alive:

```text
TROOZN phone app -> WebTransport https://radxa:4433/ -> TROOZN Radxa Proxy
```

## Runtime

Environment variables:

- `TROOZN_FTP_BIND`, default `0.0.0.0:2120`
- `TROOZN_WEBTRANSPORT_BIND`, default `0.0.0.0:4433`
- `TROOZN_KODI_HOST`, default `127.0.0.1`
- `TROOZN_KODI_PORT`, default `8080`

Start:

```sh
cargo run
```

The server prints the WebTransport certificate hash. Phones must pin this hash.

## WebTransport Protocol

Phone registration, phone to Radxa:

```json
{"type":"phone.register","deviceId":"phone-1","displayName":"Pixel TROOZN"}
```

Radxa to phone directory listing:

```json
{"type":"media.list","path":"/"}
```

Phone response:

```json
{
  "ok": true,
  "entries": [
    {"name":"Movies","isDirectory":true,"size":0},
    {"name":"song.flac","isDirectory":false,"size":12345678}
  ]
}
```

Radxa to phone metadata:

```json
{"type":"media.stat","path":"/song.flac"}
```

Phone response:

```json
{"ok":true,"isDirectory":false,"size":12345678}
```

Radxa to phone file streaming:

```json
{"type":"media.get","path":"/song.flac","start":0}
```

For `media.get`, the phone writes raw file bytes to the same bidirectional
stream and then finishes it. Errors should be sent as:

```json
{"ok":false,"error":"file not found"}
```

Controller requests for Kodi are also accepted on WebTransport:

```json
{"type":"kodi.jsonrpc","method":"Input.Up"}
```
