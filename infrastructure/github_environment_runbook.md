# GitHub pulumi environment runbook

Required GitHub Environment: `pulumi`

Required environment secrets:
- `AWS_IAM_INFRASTRUCTURE_ROLE_ARN`
- `AWS_REGION` *(still required in GitHub for now)*
- `PULUMI_ACCESS_TOKEN`
- `AWS_S3_ARTIFACTS_BUCKET_NAME`

## Update `AWS_IAM_INFRASTRUCTURE_ROLE_ARN` from Pulumi output

Run from repository root:

```bash
cd infrastructure
pulumi stack select <pulumi-org>/oscmcompany/production
role_arn="$(pulumi stack output aws_iam_github_actions_infrastructure_role_arn --stack production)"
cd ..
gh secret set AWS_IAM_INFRASTRUCTURE_ROLE_ARN --env pulumi --body "$role_arn"
```

Optional verification:

```bash
gh secret list --env pulumi
```
