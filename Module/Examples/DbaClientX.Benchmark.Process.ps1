function Set-DbaClientXBenchmarkProcess {
    [CmdletBinding()]
    param(
        [string] $ProcessorAffinity,
        [ValidateSet('Current', 'Normal', 'AboveNormal', 'High')]
        [string] $ProcessPriority = 'Current'
    )

    $process = [System.Diagnostics.Process]::GetCurrentProcess()
    $originalAffinity = $process.ProcessorAffinity
    $originalPriority = $process.PriorityClass
    try {
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
        }

        if ($ProcessPriority -ne 'Current') {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::$ProcessPriority
        }
    } catch {
        $process.ProcessorAffinity = $originalAffinity
        $process.PriorityClass = $originalPriority
        throw
    }

    $nativeMask = if ([IntPtr]::Size -eq 4) {
        [uint64] [BitConverter]::ToUInt32([BitConverter]::GetBytes($process.ProcessorAffinity.ToInt32()), 0)
    } else {
        [BitConverter]::ToUInt64([BitConverter]::GetBytes($process.ProcessorAffinity.ToInt64()), 0)
    }

    [pscustomobject]@{
        ProcessorAffinity = '0x{0}' -f $nativeMask.ToString(('X{0}' -f ([IntPtr]::Size * 2)), [System.Globalization.CultureInfo]::InvariantCulture)
        ProcessPriority = [string] $process.PriorityClass
        OriginalProcessorAffinity = $originalAffinity
        OriginalProcessPriority = $originalPriority
    }
}

function Restore-DbaClientXBenchmarkProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [object] $State
    )

    $process = [System.Diagnostics.Process]::GetCurrentProcess()
    $process.ProcessorAffinity = [IntPtr] $State.OriginalProcessorAffinity
    $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass] $State.OriginalProcessPriority
}
