<# Preview by default; -Install registers per-user tasks that call the stable launcher. #>
[CmdletBinding()]
param([switch]$Install, [string]$TaskPrefix = 'U19 Recording Transfer')
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$launcher = Join-Path $root 'launch.ps1'
foreach ($item in @(@('Update', '22:30', 'update'), @('Copy', '23:00', 'copy'), @('Cleanup preview', '06:00', 'cleanup-preview'))) {
    $name = "$TaskPrefix - $($item[0])"
    $arguments = '-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -Mode {1}' -f $launcher, $item[2]
    Write-Output "$name at $($item[1]): powershell.exe $arguments"
    if ($Install) {
        $action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument $arguments -WorkingDirectory $root
        $trigger = New-ScheduledTaskTrigger -Daily -At $item[1]
        $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
        $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Days 1)
        Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Description 'Uses stable U19 launcher; updates skip while application is active. Cleanup is preview only.' -Force | Out-Null
    }
}
