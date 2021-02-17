# dataworks-batch-job-handlers

## An AWS lambda which receives SNS messages from batch job status changes and handles them.

This repo contains Makefile, and Dockerfile to fit the standard pattern.
This repo is a base to create new Docker image repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`