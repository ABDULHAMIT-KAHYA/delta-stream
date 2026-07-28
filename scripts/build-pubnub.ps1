$ErrorActionPreference = "Stop"
cargo build --features pubnub-transport
Write-Host "Built. Start listener and publisher in separate terminals:"
Write-Host ".\target\debug\delta-stream.exe --pubnub-listen"
Write-Host ".\target\debug\delta-stream.exe --pubnub-publish"
