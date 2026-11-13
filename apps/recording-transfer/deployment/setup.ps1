<# Install a stable per-user launcher. No tasks or real transfers start by default. #>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$MachineConfig,
    [string]$InstallRoot = "$env:LOCALAPPDATA\BrainCOGS\U19RecordingTransfer",
    [string]$UvPath,
    [string]$PythonVersion = '3.12',
    [switch]$Initialize,
    [switch]$InstallTasks,
    [switch]$DesktopShortcuts
)
$ErrorActionPreference = 'Stop'
if (-not $UvPath) { $UvPath = (Get-Command uv -ErrorAction Stop).Source }
foreach ($path in @($MachineConfig, $InstallRoot, $UvPath)) {
    if ($path.Contains('"') -or -not [IO.Path]::IsPathRooted($path)) { throw 'Use absolute paths without double quotes.' }
}
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot)
$config = Get-Content -LiteralPath $MachineConfig -Raw | ConvertFrom-Json
if ($config.schema_version -ne 1) { throw 'Expected machine config schema_version 1.' }
New-Item -ItemType Directory -Force -Path $InstallRoot | Out-Null
if (Test-Path -LiteralPath (Join-Path $InstallRoot 'machine.json')) {
    throw 'An installation already exists. Edit its machine.json and use Update; setup never overwrites machine settings.'
}
& $UvPath python install $PythonVersion
if ($LASTEXITCODE -ne 0) { throw 'uv could not prepare Python.' }
$pythonOutput = & $UvPath python find $PythonVersion
if ($LASTEXITCODE -ne 0) { throw 'uv could not locate Python.' }
$python = ($pythonOutput | Select-Object -Last 1).Trim()
foreach ($name in @('bootstrap.py', 'launch.ps1', 'tasks.ps1')) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination (Join-Path $InstallRoot $name)
}
[IO.File]::WriteAllText((Join-Path $InstallRoot 'bootstrap-python.txt'), $python)
$config | Add-Member -NotePropertyName state_dir -NotePropertyValue (Join-Path $InstallRoot 'state') -Force
$config | Add-Member -NotePropertyName uv_executable -NotePropertyValue $UvPath -Force
$config | Add-Member -NotePropertyName python -NotePropertyValue $PythonVersion -Force
$serialized = $config | ConvertTo-Json -Depth 12
[IO.File]::WriteAllText((Join-Path $InstallRoot 'machine.json'), $serialized, (New-Object Text.UTF8Encoding($false)))
& $python (Join-Path $InstallRoot 'bootstrap.py') --root $InstallRoot status
if ($LASTEXITCODE -ne 0) { throw 'Machine configuration validation failed. Correct machine.json before initializing.' }
if ($Initialize) {
    & $python (Join-Path $InstallRoot 'bootstrap.py') --root $InstallRoot update
    if ($LASTEXITCODE -ne 0) { throw 'Initial release install failed; see logs. Retry with launch.ps1 -Mode update.' }
}
if ($DesktopShortcuts) {
    $shell = New-Object -ComObject WScript.Shell
    $desktop = [Environment]::GetFolderPath('Desktop')
    foreach ($item in @(@('U19 Recording Transfer', 'run'), @('Update U19 Recording Transfer', 'update'), @('Rollback U19 Recording Transfer', 'rollback'))) {
        $shortcut = $shell.CreateShortcut((Join-Path $desktop ($item[0] + '.lnk')))
        $shortcut.TargetPath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
        $shortcut.Arguments = '-NoProfile -ExecutionPolicy Bypass -File "{0}" -Mode {1}' -f (Join-Path $InstallRoot 'launch.ps1'), $item[1]
        $shortcut.WorkingDirectory = $InstallRoot
        $shortcut.Save()
    }
}
& (Join-Path $InstallRoot 'tasks.ps1') -Install:$InstallTasks
Write-Output "Installed stable launcher at $InstallRoot. No data transfers or cleanup were started."
