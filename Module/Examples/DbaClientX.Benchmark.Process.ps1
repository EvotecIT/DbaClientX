function Set-DbaClientXBenchmarkProcess {
    [CmdletBinding()]
    param(
        [string] $ProcessorAffinity,
        [ValidateSet('Current', 'Normal', 'AboveNormal', 'High')]
        [string] $ProcessPriority = 'Current'
    )

    $process = [System.Diagnostics.Process]::GetCurrentProcess()
    $supportsAffinity =
        [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows) -or
        [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)
    $originalAffinity = $null
    $originalPriority = $null
    $affinityApplied = $false
    $priorityApplied = $false
    $affinityDisplay = 'Unsupported'
    $priorityDisplay = 'Unsupported'
    try {
        if ($supportsAffinity) {
            $originalAffinity = $process.ProcessorAffinity
            if (-not [string]::IsNullOrWhiteSpace($ProcessorAffinity)) {
                $text = $ProcessorAffinity.Trim()
                $mask = if ($text.StartsWith('0x', [System.StringComparison]::OrdinalIgnoreCase)) {
                    [uint64]::Parse($text.Substring(2), [System.Globalization.NumberStyles]::AllowHexSpecifier, [System.Globalization.CultureInfo]::InvariantCulture)
                } else {
                    [uint64]::Parse($text, [System.Globalization.NumberStyles]::Integer, [System.Globalization.CultureInfo]::InvariantCulture)
                }

                if ($mask -eq 0) {
                    throw 'ProcessorAffinity must select at least one processor.'
                }

                if ([IntPtr]::Size -eq 4) {
                    if ($mask -gt [uint32]::MaxValue) {
                        throw "ProcessorAffinity '$ProcessorAffinity' exceeds the native 32-bit processor mask."
                    }

                    $signedMask = [BitConverter]::ToInt32([BitConverter]::GetBytes([uint32] $mask), 0)
                    $process.ProcessorAffinity = [IntPtr]::new($signedMask)
                } else {
                    $signedMask = [BitConverter]::ToInt64([BitConverter]::GetBytes($mask), 0)
                    $process.ProcessorAffinity = [IntPtr]::new($signedMask)
                }

                $affinityApplied = $true
            }

            $nativeMask = if ([IntPtr]::Size -eq 4) {
                [uint64] [BitConverter]::ToUInt32([BitConverter]::GetBytes($process.ProcessorAffinity.ToInt32()), 0)
            } else {
                [BitConverter]::ToUInt64([BitConverter]::GetBytes($process.ProcessorAffinity.ToInt64()), 0)
            }
            $affinityDisplay = '0x{0}' -f $nativeMask.ToString(('X{0}' -f ([IntPtr]::Size * 2)), [System.Globalization.CultureInfo]::InvariantCulture)
        } elseif (-not [string]::IsNullOrWhiteSpace($ProcessorAffinity)) {
            throw [System.PlatformNotSupportedException]::new('ProcessorAffinity is supported only on Windows and Linux.')
        }

        try {
            $originalPriority = $process.PriorityClass
            if ($ProcessPriority -ne 'Current') {
                $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::$ProcessPriority
                $priorityApplied = $true
            }
            $priorityDisplay = [string] $process.PriorityClass
        } catch [System.PlatformNotSupportedException], [System.NotSupportedException] {
            if ($ProcessPriority -ne 'Current') {
                throw [System.PlatformNotSupportedException]::new('ProcessPriority is not supported on this platform.', $_.Exception)
            }
        }
    } catch {
        if ($affinityApplied) {
            $process.ProcessorAffinity = [IntPtr] $originalAffinity
        }
        if ($priorityApplied) {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass] $originalPriority
        }
        throw
    }

    [pscustomobject]@{
        ProcessorAffinity = $affinityDisplay
        ProcessPriority = $priorityDisplay
        OriginalProcessorAffinity = $originalAffinity
        OriginalProcessPriority = $originalPriority
        RestoreProcessorAffinity = $affinityApplied
        RestoreProcessPriority = $priorityApplied
    }
}

function Restore-DbaClientXBenchmarkProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object] $State
    )

    $process = [System.Diagnostics.Process]::GetCurrentProcess()
    if ($State.RestoreProcessorAffinity) {
        $process.ProcessorAffinity = [IntPtr] $State.OriginalProcessorAffinity
    }
    if ($State.RestoreProcessPriority) {
        $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass] $State.OriginalProcessPriority
    }
}
