param(
    [Parameter(Mandatory = $true)]
    [string]$PythonBin,

    [string]$ScriptDir = "",

    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"

$PythonBin = $PythonBin.Trim('"')
if ([string]::IsNullOrWhiteSpace($ScriptDir)) {
    $ScriptDir = Split-Path -Parent $PSCommandPath
}
$ScriptDir = $ScriptDir.Trim('"')
$ScriptDir = $ScriptDir.TrimEnd('\', '/')

$scriptPath = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir "app.py"))
$pidFile = Join-Path $ScriptDir "server.pid"

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

$process = Start-Process -FilePath $PythonBin -ArgumentList "app.py" -WorkingDirectory $ScriptDir -PassThru
Set-Content -Path $pidFile -Value $process.Id -Encoding ASCII
