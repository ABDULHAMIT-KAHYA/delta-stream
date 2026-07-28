$ErrorActionPreference = "Stop"
if (-not (Test-Path ".\target\debug\delta-stream.exe")) {
    cargo build --features pubnub-transport
}
.\target\debug\delta-stream.exe --pubnub-publish
