---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# New-FabricXWarehouseConnectionOptions
## SYNOPSIS
Creates Fabric Warehouse connection options from a caller-acquired secure SQL token.

## SYNTAX
### __AllParameterSets
```powershell
New-FabricXWarehouseConnectionOptions [-AccessToken] <securestring> [-ExpiresOn] <DateTimeOffset> [<CommonParameters>]
```

## DESCRIPTION
Creates Fabric Warehouse connection options from a caller-acquired secure SQL token.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $token = Get-AzAccessToken -ResourceUrl 'https://database.windows.net' -AsSecureString; $options = New-FabricXWarehouseConnectionOptions -AccessToken $token.Token -ExpiresOn $token.ExpiresOn
```

The reusable callback preserves SqlClient pooling while the fixed token remains valid.

## PARAMETERS

### -AccessToken
Caller-acquired Microsoft Entra token for the Azure SQL resource.

```yaml
Type: SecureString
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -ExpiresOn
Token expiry reported by the identity provider.

```yaml
Type: DateTimeOffset
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 1
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `DBAClientX.SqlServerConnectionOptions`

## RELATED LINKS

- None
