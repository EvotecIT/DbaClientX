# Build and Publish

Package publishing is intentionally **manual** for this repository.

Reason:
- releases must be signed locally with the USB key certificate
- we do not auto-publish NuGet packages from GitHub Actions

## Local release flow

1. Build and validate release artifacts:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Core\BuildPackage.ps1
```

2. Publish signed packages to NuGet (local machine, local cert, local API key file):

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Core\PublishPackageNuget.ps1
```

3. Optionally publish GitHub release assets:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Core\PublishPackageGitHub.ps1
```

## Configuration

Primary config file: `Build/project.build.json`

Important fields:
- `CertificateThumbprint`
- `CertificateStore`
- `PublishApiKeyFilePath`
- `GitHubAccessTokenFilePath`

Keep these configured for local publishing only.
