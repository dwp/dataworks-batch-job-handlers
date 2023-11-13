# DO NOT USE THIS REPO - MIGRATED TO GITLAB

# dataworks-batch-job-handlers

## An AWS lambda which receives SNS messages from batch job status changes and handles them.

This repo contains Makefile to fit the standard pattern. This repo is a base to create new non-Terraform repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`

## Environment variables

|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services|No|
|AWS_REGION| eu-west-1 |The region the lambda is running in|No|
|ENVIRONMENT| dev |The environment the lambda is running in|No|
|APPLICATION| batch-job-handler |The name of the application|No|
|LOG_LEVEL| INFO |The logging level of the Lambda|No|
|SNS_TOPIC_ARN|The arn of the sns topic to send monitoring messages to|Yes|

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.
