resource "aws_ecr_repository" "dataworks-batch-job-handlers" {
  name = "dataworks-batch-job-handlers"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/dataworks-batch-job-handlers" }
  )
}

resource "aws_ecr_repository_policy" "dataworks-batch-job-handlers" {
  repository = aws_ecr_repository.dataworks-batch-job-handlers.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.dataworks-batch-job-handlers.repository_url
}
