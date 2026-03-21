# CI/CD Templates

Pre-built pipeline templates for automated deployment of the IBP Forecast solution to Microsoft Fabric.

## Available Templates

| File | Platform | Description |
|------|----------|-------------|
| `github-actions-deploy.yml` | GitHub Actions | Deploys on push to `main` or manual trigger |
| `azure-pipelines-deploy.yml` | Azure DevOps | Deploys on push to `main` via Azure CLI task |

## Setup

### GitHub Actions

1. Copy `github-actions-deploy.yml` to `.github/workflows/deploy.yml`
2. Create a service principal: `az ad sp create-for-rbac --name "ibp-deploy"`
3. Grant the SP access to your Fabric workspace (Workspace Admin role)
4. Add repository secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`

### Azure DevOps

1. Create an Azure service connection in your project settings
2. Create a pipeline pointing to `deploy/cicd/azure-pipelines-deploy.yml`
3. Set pipeline variable `AZURE_SERVICE_CONNECTION` to your connection name

## Configuration

All deployment settings are in `deploy/deploy.config.toml`. The `[cicd]` section controls optional Fabric Git integration:

```toml
[cicd]
enabled           = false
provider          = "github"
organization_name = "your-org"
repository_name   = "your-repo"
branch_name       = "main"
directory_name    = "/"
```

When `enabled = true`, the deployment script will connect the Fabric workspace to the specified Git repository using the Fabric REST API.
