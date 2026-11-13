[CmdletBinding()]
param(
    [ValidateSet('run', 'copy', 'cleanup', 'cleanup-preview', 'update', 'rollback', 'status')]
    [string]$Mode = 'run',
    [switch]$Demo
)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$python = [IO.File]::ReadAllText((Join-Path $root 'bootstrap-python.txt')).Trim()
$arguments = @((Join-Path $root 'bootstrap.py'), '--root', $root, $Mode)
if ($Demo) { $arguments += '--demo' }
& $python @arguments
$result = $LASTEXITCODE
if ($result -ne 0 -and $Mode -eq 'run') {
    Add-Type -AssemblyName System.Windows.Forms
    [System.Windows.Forms.MessageBox]::Show("Recording Transfer could not start. See $root\logs\launcher.log and last_error.txt.", 'U19 Recording Transfer') | Out-Null
}
exit $result
