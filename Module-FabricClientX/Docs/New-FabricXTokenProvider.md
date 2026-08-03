---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# New-FabricXTokenProvider
## SYNOPSIS
Creates a short-lived FabricClientX token provider from a caller-acquired secure token.

## SYNTAX
### __AllParameterSets
```powershell
New-FabricXTokenProvider [-AccessToken] <securestring> [-ExpiresOn] <DateTimeOffset> [<CommonParameters>]
```

## DESCRIPTION
Creates a short-lived FabricClientX token provider from a caller-acquired secure token.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $azToken = Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com' -AsSecureString; $provider = New-FabricXTokenProvider -AccessToken $azToken.Token -ExpiresOn $azToken.ExpiresOn
```

The returned provider can be reused by Fabric discovery cmdlets until the token nears expiry.

## PARAMETERS

### -AccessToken
Caller-acquired Microsoft Entra access token.

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

- `FabricClientX.IFabricTokenProvider`

## RELATED LINKS

- None
