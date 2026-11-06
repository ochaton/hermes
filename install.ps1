$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$arch = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'arm64' } else { 'amd64' }
$dir = Join-Path $env:LOCALAPPDATA 'hermes'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
try {
	Invoke-WebRequest -UseBasicParsing -OutFile (Join-Path $dir 'hermes.exe') `
		-Uri "https://github.com/ochaton/hermes/releases/download/v0.9.0-ticket.1/hermes-windows-$arch.exe"
} catch {
	Write-Host 'download failed. go install github.com/ochaton/hermes@experiment/ux-ticket'
	return
}
$path = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not (($path -split ';') -contains $dir)) {
	[Environment]::SetEnvironmentVariable('Path', ((@($path, $dir) | Where-Object { $_ }) -join ';'), 'User')
}
Write-Host 'installed'
Write-Host 'open a new terminal, then: hermes send <file>'
