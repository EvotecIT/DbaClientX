Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXQuery cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXQuery | Should -Not -BeNullOrEmpty
    }

    it 'supports StoredProcedure parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'StoredProcedure'
    }

    it 'supports Stream parameter' {
        (Get-Command Invoke-DbaXQuery).Parameters.Keys | Should -Contain 'Stream'
    }
}
