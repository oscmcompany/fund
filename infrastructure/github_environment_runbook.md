# GitHub Pulumi Environment Runbook

Required GitHub Environment: `pulumi`

Required environment secrets for operations with Pulumi:

- `AWS_IAM_INFRASTRUCTURE_ROLE_ARN`
- `AWS_REGION`
- `PULUMI_ACCESS_TOKEN`

## Update `AWS_IAM_INFRASTRUCTURE_ROLE_ARN` from Pulumi output

Run from repository root:

```bash
cd infrastructure
pulumi stack select "$(pulumi org get-default)/fund/production"
role_arn="$(pulumi stack output aws_iam_github_actions_infrastructure_role_arn --stack production)"
cd ..
gh secret set AWS_IAM_INFRASTRUCTURE_ROLE_ARN --env pulumi --body "$role_arn"
```

## Update `AWS_REGION` from Pulumi stack config

Run from repository root:

```bash
cd infrastructure
pulumi stack select "$(pulumi org get-default)/fund/production"
region="$(pulumi config get aws:region --stack production)"
cd ..
gh secret set AWS_REGION --env pulumi --body "$region"
```

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
