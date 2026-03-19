param(
    [Parameter(Mandatory = $true)]
    [string]$PythonBin,

    [string]$ScriptDir = "",

    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$PythonBin = $PythonBin.Trim('"')
if ([string]::IsNullOrWhiteSpace($ScriptDir)) {
    $ScriptDir = Split-Path -Parent $PSCommandPath
}
$ScriptDir = $ScriptDir.Trim('"')
$ScriptDir = $ScriptDir.TrimEnd('\', '/')

$scriptPath = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir "app.py"))
$pidFile = Join-Path $ScriptDir "server.pid"
$stdoutLog = Join-Path $ScriptDir "server.stdout.log"
$stderrLog = Join-Path $ScriptDir "server.stderr.log"
$startupTimeoutSeconds = 120

if (Test-Path $pidFile) {
    $savedPid = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ($savedPid -match "^\d+$") {
        Stop-Process -Id ([int]$savedPid) -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($listener) {
    $listenerProc = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)" -ErrorAction SilentlyContinue
    if ($listenerProc -and $listenerProc.Name -ieq "python.exe") {
        Stop-Process -Id $listener.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}

$targets = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and (
        $_.CommandLine -like "*$scriptPath*" -or
        $_.CommandLine -like "*analysis_system\\app.py*" -or
        $_.CommandLine -like "*analysis_system/app.py*" -or
        $_.CommandLine -like "* app.py*"
    )
}

foreach ($proc in $targets) {
    if ($proc.ProcessId -ne $PID) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

Start-Sleep -Seconds 1

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($listener) {
    throw "Port $Port is still in use by PID $($listener.OwningProcess)."
}

Remove-Item $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

$process = Start-Process `
    -FilePath $PythonBin `
    -ArgumentList "app.py" `
    -WorkingDirectory $ScriptDir `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $pidFile -Value $process.Id -Encoding ASCII

for ($i = 0; $i -lt $startupTimeoutSeconds; $i++) {
    Start-Sleep -Seconds 1

    $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listener) {
        exit 0
    }

    $running = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if (-not $running) {
        $stderrPreview = ""
        if (Test-Path $stderrLog) {
            $stderrPreview = (Get-Content $stderrLog -ErrorAction SilentlyContinue | Select-Object -Last 20) -join [Environment]::NewLine
        }
        if ([string]::IsNullOrWhiteSpace($stderrPreview)) {
            throw "Server process exited before port $Port became ready."
        }
        throw "Server process exited before port $Port became ready.`n$stderrPreview"
    }
}

Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
throw "Server startup timed out. Check $stdoutLog and $stderrLog for details."
