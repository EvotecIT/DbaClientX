Import-Module "$PSScriptRoot/../DbaClientX.psd1" -Force

describe 'Invoke-DbaXPostgreSql cmdlet' {
    it 'is exported' {
        Get-Command Invoke-DbaXPostgreSql | Should -Not -BeNullOrEmpty
    }

    it 'supports StoredProcedure parameter' {
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'StoredProcedure'
    }

    it 'supports Username and Password parameters' {
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'Username'
        (Get-Command Invoke-DbaXPostgreSql).Parameters.Keys | Should -Contain 'Password'
    }
}
