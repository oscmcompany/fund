# GitHub Pulumi Environment Runbook

Required GitHub Environment: `pulumi`

Required environment secrets for operations with Pulumi:

- `AWS_IAM_INFRASTRUCTURE_ROLE_ARN`
- `AWS_REGION`
- `PULUMI_ACCESS_TOKEN`

`AWS_IAM_INFRASTRUCTURE_ROLE_ARN` and `AWS_REGION` are set automatically when running
`mask infrastructure stack up --bootstrap` from a local machine with `gh` authenticated.

## Update `PULUMI_ACCESS_TOKEN` from Pulumi account

Generate a new access token from your Pulumi account at <https://app.pulumi.com/account/tokens>

Run from repository root:

```bash
gh secret set PULUMI_ACCESS_TOKEN --env pulumi --body "<your-token>"
```

## Verify all secrets

Optional verification:

```bash
gh secret list --env pulumi
```
