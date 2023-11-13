#!/usr/bin/env python3

"""batch_job_handler_lambda"""
import argparse
import boto3
import json
import logging
import os
import sys
import socket
import re
import datetime

UNSET_TEXT = "NOT_SET"

FAILED_JOB_STATUS = "FAILED"
PENDING_JOB_STATUS = "PENDING"
RUNNABLE_JOB_STATUS = "RUNNABLE"
STARTING_JOB_STATUS = "STARTING"
SUCCEEDED_JOB_STATUS = "SUCCEEDED"

IGNORED_JOB_STATUSES = [PENDING_JOB_STATUS, RUNNABLE_JOB_STATUS, STARTING_JOB_STATUS]

JOB_NAME_KEY = "jobName"
JOB_STATUS_KEY = "status"
JOB_QUEUE_KEY = "jobQueue"
JOB_STATUS_REASON_KEY = "statusReason"

JOB_CREATED_AT_KEY = ("createdAt", "Created at")
JOB_STARTED_AT_KEY = ("startedAt", "Started at")
JOB_STOPPED_AT_KEY = ("stoppedAt", "Stopped at")
OPTIONAL_TIME_KEYS = [JOB_CREATED_AT_KEY, JOB_STARTED_AT_KEY, JOB_STOPPED_AT_KEY]

ERROR_NOTIFICATION_TYPE = "Error"
WARNING_NOTIFICATION_TYPE = "Warning"
INFORMATION_NOTIFICATION_TYPE = "Information"

CRITICAL_SEVERITY = "Critical"
HIGH_SEVERITY = "High"
MEDIUM_SEVERITY = "Medium"

REGEX_OBJECT_TAGGING_JOB_QUEUE_ARN = re.compile("^.*/.*_object_tagger$")
REGEX_UCFS_CLAIMANT_JOB_QUEUE_ARN = re.compile("^.*/ucfs_claimant_api$")
REGEX_TRIMMER_JOB_QUEUE_ARN = re.compile("^.*/k2hb_reconciliation_trimmer$")
REGEX_COALESCER_JOB_QUEUE_ARN = re.compile("^.*/batch_corporate_storage_coalescer.*$")
REGEX_KAFKA_RECONCILIATION_JOB_QUEUE_ARN = re.compile("^.*/kafka-reconciliation$")

args = None
logger = None


# Initialise logging
def setup_logging(logger_level):
    """Set the default logger with json output."""
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)
    hostname = socket.gethostname()

    json_format = (
        f'{{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process":"%(process)s", '
        f'"thread": "[%(thread)s]", "host": "{hostname}" }}'
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level)
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_parameters():
    """Parse the supplied command line arguments.

    Returns:
        args: The parsed and validated command line arguments

    """
    parser = argparse.ArgumentParser(
        description="Start up and shut down ASGs on demand"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--sns-topic", help="SNS topic ARN")
    parser.add_argument("--environment", help="Environment value", default=UNSET_TEXT)
    parser.add_argument("--application", help="Application", default=UNSET_TEXT)
    parser.add_argument(
        "--slack-channel-override",
        help="Slack channel to use for overriden jobs",
        default=UNSET_TEXT,
    )
    parser.add_argument("--log-level", help="Log level for lambda", default="INFO")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "SNS_TOPIC" in os.environ:
        _args.sns_topic = os.environ["SNS_TOPIC"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    if "SLACK_CHANNEL_OVERRIDE" in os.environ:
        _args.slack_channel_override = os.environ["SLACK_CHANNEL_OVERRIDE"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


def get_sns_client():
    return boto3.client("sns")


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global args
    global logger

    args = get_parameters()
    logger = setup_logging(args.log_level)

    dumped_event = get_escaped_json_string(event)
    logger.info(f'SNS Event", "sns_event": {dumped_event}, "mode": "handler')

    sns_client = get_sns_client()

    if not args.sns_topic:
        raise Exception("Required argument SNS_TOPIC is unset")

    detail_dict = get_and_validate_job_details(
        event,
        args.sns_topic,
    )

    job_name = detail_dict[JOB_NAME_KEY]
    job_status = detail_dict[JOB_STATUS_KEY]
    job_queue = detail_dict[JOB_QUEUE_KEY]

    if job_status in IGNORED_JOB_STATUSES:
        logger.info(
            f'Exiting normally as job status warrants no notification", '
            + f'"job_name": "{job_name}", "job_queue": "{job_queue}", "job_status": "{job_status}'
        )
        sys.exit(0)

    severity = get_severity(job_queue, job_status, job_name)
    notification_type = get_notification_type(job_queue, job_status, job_name)

    slack_channel_override = get_slack_channel_override(
        args.slack_channel_override,
        job_queue,
        job_name,
        job_status,
    )

    payload = generate_monitoring_message_payload(
        detail_dict,
        slack_channel_override,
        job_queue,
        job_name,
        job_status,
        severity,
        notification_type,
    )

    send_sns_message(
        sns_client,
        payload,
        args.sns_topic,
        job_queue,
        job_status,
        job_name,
    )


def generate_monitoring_message_payload(
    detail_dict,
    slack_channel_override,
    job_queue,
    job_name,
    job_status,
    severity,
    notification_type,
):
    """Generates a payload for a monitoring message.

    Arguments:
        detail_dict (dict): the dict of the details
        slack_channel_override (string): slack channel to override (or None)
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_status (string): the status of the job
        export_date (string): the date of the export
        severity (string): the severity of the alert
        notification_type (string): the notification type of the alert

    """
    custom_elements = generate_custom_elements(
        detail_dict,
        job_queue,
        job_name,
        job_status,
    )

    friendly_name = get_friendly_name(
        job_queue,
        job_name,
        job_status,
    )

    title_text = f"{friendly_name} changed to {job_status}"

    payload = {
        "severity": severity,
        "notification_type": notification_type,
        "slack_username": "AWS Batch Job Notification",
        "title_text": title_text,
        "custom_elements": custom_elements,
    }

    if (
        slack_channel_override is not None
        and notification_type == INFORMATION_NOTIFICATION_TYPE
    ):
        payload["slack_channel_override"] = slack_channel_override

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Generated monitoring SNS payload", "payload": {dumped_payload}, '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    return payload


def get_friendly_name(
    job_queue,
    job_name,
    job_status,
):
    """Returns a friendly name for the given job queue.

    Arguments:
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_status (string): the status of the job

    """
    logger.info(
        f'Generating job queue friendly name", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    friendly_name = "Batch job"

    if REGEX_OBJECT_TAGGING_JOB_QUEUE_ARN.match(job_queue):
        if "_pt_1" in job_name.lower() or "mongo-latest" in job_name.lower():
            friendly_name = "PT-1 object tagger"
        elif "_pt_2" in job_name.lower():
            friendly_name = "PT-2 object tagger"
        elif "pt-" in job_name.lower():
            friendly_name = "PT object tagger"
        elif "clive" in job_name.lower():
            friendly_name = "Clive object tagger"
        elif "pdm" in job_name.lower():
            friendly_name = "PDM object tagger"
        else:
            friendly_name = "Object tagger"
    elif REGEX_UCFS_CLAIMANT_JOB_QUEUE_ARN.match(job_queue):
        friendly_name = "UCFS claimant data load"
    elif REGEX_TRIMMER_JOB_QUEUE_ARN.match(job_queue):
        friendly_name = "K2HB trimmer"
    elif REGEX_COALESCER_JOB_QUEUE_ARN.match(job_queue):
        friendly_name = "Batch coalescer"
    elif REGEX_KAFKA_RECONCILIATION_JOB_QUEUE_ARN.match(job_queue):
        friendly_name = "Kafka reconciliation batch"

    logger.info(
        f'Generated job queue friendly name", "friendly_name": "{friendly_name}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )
    return friendly_name


def get_slack_channel_override(
    slack_channel_override,
    job_queue,
    job_name,
    job_status,
):
    """Returns the slack channel to use as an override or None if not set.

    Arguments:
        slack_channel_override (string): the override from args
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_status (string): the status of the job

    """
    logger.info(
        f'Getting slack channel override", "slack_channel_override_arg": "{slack_channel_override}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    if slack_channel_override == UNSET_TEXT:
        logger.info(
            f'Not using override as slack channel override not passed in", "slack_channel_override_arg": "{slack_channel_override}", '
            + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
        )
        return None

    if (
        REGEX_COALESCER_JOB_QUEUE_ARN.match(job_queue)
        or REGEX_KAFKA_RECONCILIATION_JOB_QUEUE_ARN.match(job_queue)
        or REGEX_TRIMMER_JOB_QUEUE_ARN.match(job_queue)
    ):
        logger.info(
            f'Using slack channel override for job", "slack_channel_override": "{slack_channel_override}", '
            + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
        )
        return slack_channel_override

    logger.info(
        f'Not using slack channel override for queue", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    return None


def generate_custom_elements(
    detail_dict,
    job_queue,
    job_name,
    job_status,
):
    """Generates a custom elements array.

    Arguments:
        detail_dict (dict): the dict of the details
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_status (string): the status of the job

    """
    dumped_detail_dict = get_escaped_json_string(detail_dict)
    logger.info(
        f'Generating custom elements", "detail_dict": {dumped_detail_dict}, '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    job_queue_end = job_queue.split("/")[-1]

    custom_elements = [
        {"key": "Job name", "value": job_name},
        {"key": "Job queue", "value": job_queue_end},
    ]

    if JOB_STATUS_REASON_KEY in detail_dict:
        custom_elements.append(
            {"key": "Job status reason", "value": detail_dict[JOB_STATUS_REASON_KEY]}
        )

    for time_key, time_name in OPTIONAL_TIME_KEYS:
        if time_key in detail_dict and detail_dict[time_key]:
            timestamp = datetime.datetime.fromtimestamp(detail_dict[time_key] / 1000)
            timestamp_string = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
            custom_elements.append({"key": time_name, "value": timestamp_string})

    return custom_elements


def send_sns_message(
    sns_client,
    payload,
    sns_topic_arn,
    job_queue,
    job_status,
    job_name,
):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic
        job_queue (dict): The job queue arn
        job_status (dict): The job status
        job_name (dict): The job name

    """
    global logger

    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)


def get_and_validate_job_details(event, sns_topic_arn):
    """Get the job name from the SNS event.

    Arguments:
        event (dict): The SNS event
        sns_topic_arn (string): SNS topic arn for logging
    """
    message = json.loads(event["Records"][0]["Sns"]["Message"])

    dumped_message = get_escaped_json_string(message)
    logger.info(
        f'Validating message", "message_details": {dumped_message}, "sns_topic_arn": "{sns_topic_arn}'
    )

    if "detail" not in message:
        raise KeyError("Message contains no 'detail' key")

    detail_dict = message["detail"]
    required_keys = [JOB_NAME_KEY, JOB_STATUS_KEY, JOB_QUEUE_KEY]

    for required_key in required_keys:
        if required_key not in detail_dict:
            error_string = f"Details dict contains no '{required_key}' key"
            raise KeyError(error_string)

    logger.info(
        f'Message has been validated", "message_details": {dumped_message}, "job_queue": "{detail_dict[JOB_QUEUE_KEY]}", '
        + f'"job_name": "{detail_dict[JOB_NAME_KEY]}", "job_status": "{detail_dict[JOB_STATUS_KEY]}'
    )

    return detail_dict


def get_severity(job_queue, job_status, job_name):
    """Get the severity of the given alert.

    Arguments:
        job_queue (dict): The job queue arn
        job_status (dict): The job status
        job_name (dict): The job name
    """
    severity = MEDIUM_SEVERITY

    if job_status in [SUCCEEDED_JOB_STATUS]:
        severity = HIGH_SEVERITY
    elif job_status in [FAILED_JOB_STATUS]:
        severity = (
            CRITICAL_SEVERITY
            if REGEX_OBJECT_TAGGING_JOB_QUEUE_ARN.match(job_queue)
            else HIGH_SEVERITY
        )

    logger.info(
        f'Generated severity", "severity": "{severity}", "job_name": "{job_name}, '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    return severity


def get_notification_type(job_queue, job_status, job_name):
    """Get the type of the given alert.

    Arguments:
        job_queue (dict): The job queue arn
        job_status (dict): The job status
        job_name (dict): The job name
    """
    notification_type = INFORMATION_NOTIFICATION_TYPE

    if job_status in [FAILED_JOB_STATUS]:
        notification_type = (
            ERROR_NOTIFICATION_TYPE
            if REGEX_OBJECT_TAGGING_JOB_QUEUE_ARN.match(job_queue)
            else WARNING_NOTIFICATION_TYPE
        )

    logger.info(
        f'Generated notification type", "notification_type": "{notification_type}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_status": "{job_status}'
    )

    return notification_type


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging("INFO")

        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        logger.info(os.getcwd())
        json_content = json.loads(open("resources/event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')
