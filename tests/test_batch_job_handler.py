#!/usr/bin/env python3

"""batch_job_handler_lambda"""
import pytest
import argparse
import json
from batch_job_handler_lambda import batch_job_handler

import unittest
from unittest import mock
from unittest.mock import MagicMock
from unittest.mock import call

FAILED_JOB_STATUS = "FAILED"
PENDING_JOB_STATUS = "PENDING"
RUNNABLE_JOB_STATUS = "RUNNABLE"
STARTING_JOB_STATUS = "STARTING"
SUCCEEDED_JOB_STATUS = "SUCCEEDED"

JOB_NAME_KEY = "jobName"
JOB_STATUS_KEY = "jobStatus"
JOB_QUEUE_KEY = "jobQueue"

ERROR_NOTIFICATION_TYPE = "Error"
WARNING_NOTIFICATION_TYPE = "Warning"
INFORMATION_NOTIFICATION_TYPE = "Information"

CRITICAL_SEVERITY = "Critical"
HIGH_SEVERITY = "High"
MEDIUM_SEVERITY = "Medium"

PDM_JOB_QUEUE = "test/pdm_object_tagger"
OTHER_JOB_QUEUE = "test_queue"
JOB_NAME = "test job"

SNS_TOPIC_ARN = "test-sns-topic-arn"

args = argparse.Namespace()
args.sns_topic = SNS_TOPIC_ARN
args.log_level = "INFO"


class TestRetriever(unittest.TestCase):
    @mock.patch("batch_job_handler_lambda.batch_job_handler.send_sns_message")
    @mock.patch(
        "batch_job_handler_lambda.batch_job_handler.generate_monitoring_message_payload"
    )
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_notification_type")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_severity")
    @mock.patch(
        "batch_job_handler_lambda.batch_job_handler.get_and_validate_job_details"
    )
    @mock.patch("batch_job_handler_lambda.batch_job_handler.setup_logging")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_parameters")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_sns_client")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_handler_gets_clients_and_processes_all_messages(
        self,
        mock_logger,
        get_sns_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        get_and_validate_job_details_mock,
        get_severity_mock,
        get_notification_type_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_parameters_mock.return_value = args

        details_dict = {
            JOB_NAME_KEY: JOB_NAME,
            JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
            JOB_QUEUE_KEY: PDM_JOB_QUEUE,
        }

        get_and_validate_job_details_mock.return_value = details_dict
        get_severity_mock.return_value = CRITICAL_SEVERITY
        get_notification_type_mock.return_value = INFORMATION_NOTIFICATION_TYPE

        payload = {
            "severity": HIGH_SEVERITY,
            "notification_type": ERROR_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Notification",
            "title_text": "Job changed to - _FAILED_",
            "custom_elements": [
                {"key": "Job name", "value": JOB_NAME},
                {"key": "Job queue", "value": PDM_JOB_QUEUE},
            ],
        }

        generate_monitoring_message_payload_mock.return_value = payload

        event = {
            "test_key": "test_value",
        }

        batch_job_handler.handler(event, None)

        get_sns_client_mock.assert_called_once()
        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()

        get_and_validate_job_details_mock.assert_called_once_with(event)
        get_severity_mock.assert_called_once_with(
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )
        get_notification_type_mock.assert_called_once_with(
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )
        generate_monitoring_message_payload_mock.assert_called_once_with(
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
            CRITICAL_SEVERITY,
            INFORMATION_NOTIFICATION_TYPE,
        )
        send_sns_message_mock.assert_called_once_with(
            sns_client_mock,
            payload,
            args.sns_topic,
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.send_sns_message")
    @mock.patch(
        "batch_job_handler_lambda.batch_job_handler.generate_monitoring_message_payload"
    )
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_notification_type")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_severity")
    @mock.patch(
        "batch_job_handler_lambda.batch_job_handler.get_and_validate_job_details"
    )
    @mock.patch("batch_job_handler_lambda.batch_job_handler.setup_logging")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_parameters")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.get_sns_client")
    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_handler_ignored_jobs_with_ignored_status(
        self,
        mock_logger,
        get_sns_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        get_and_validate_job_details_mock,
        get_severity_mock,
        get_notification_type_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        sns_client_mock = mock.MagicMock()
        get_sns_client_mock = sns_client_mock
        get_parameters_mock.return_value = args

        details_dict = {
            JOB_NAME_KEY: JOB_NAME,
            JOB_STATUS_KEY: PENDING_JOB_STATUS,
            JOB_QUEUE_KEY: PDM_JOB_QUEUE,
        }

        get_and_validate_job_details_mock.return_value = details_dict

        event = {
            "test_key": "test_value",
        }

        with pytest.raises(SystemExit) as pytest_wrapped_e:
            batch_job_handler.handler(event, None)

        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 0

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_sns_payload_generates_valid_payload(self, mock_logger):
        expected_payload = {
            "severity": CRITICAL_SEVERITY,
            "notification_type": INFORMATION_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Notification",
            "title_text": "Job changed to - _FAILED_",
            "custom_elements": [
                {"key": "Job name", "value": JOB_NAME},
                {"key": "Job queue", "value": PDM_JOB_QUEUE},
            ],
        }
        actual_payload = batch_job_handler.generate_monitoring_message_payload(
            PDM_JOB_QUEUE,
            JOB_NAME,
            FAILED_JOB_STATUS,
            CRITICAL_SEVERITY,
            INFORMATION_NOTIFICATION_TYPE,
        )
        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_send_sns_message_sends_right_message(
        self,
        mock_logger,
    ):
        sns_mock = mock.MagicMock()
        sns_mock.publish = mock.MagicMock()

        payload = {"test_key": "test_value"}

        batch_job_handler.send_sns_message(
            sns_mock,
            payload,
            SNS_TOPIC_ARN,
            PDM_JOB_QUEUE,
            JOB_NAME,
            FAILED_JOB_STATUS,
        )

        sns_mock.publish.assert_called_once_with(
            TopicArn=SNS_TOPIC_ARN, Message='{"test_key": "test_value"}'
        )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_notification_type_returns_error_for_failed_pdm_job(self, mock_logger):
        expected = ERROR_NOTIFICATION_TYPE
        actual = batch_job_handler.get_notification_type(
            PDM_JOB_QUEUE,
            FAILED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_notification_type_returns_error_for_failed_other_job(
        self, mock_logger
    ):
        expected = WARNING_NOTIFICATION_TYPE
        actual = batch_job_handler.get_notification_type(
            OTHER_JOB_QUEUE,
            FAILED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_notification_type_returns_information_for_non_failed_pdm_job(
        self, mock_logger
    ):
        expected = INFORMATION_NOTIFICATION_TYPE
        actual = batch_job_handler.get_notification_type(
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_notification_type_returns_information_for_non_failed_other_job(
        self, mock_logger
    ):
        expected = INFORMATION_NOTIFICATION_TYPE
        actual = batch_job_handler.get_notification_type(
            OTHER_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_severity_returns_critical_for_failed_pdm_job(self, mock_logger):
        expected = CRITICAL_SEVERITY
        actual = batch_job_handler.get_severity(
            PDM_JOB_QUEUE,
            FAILED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_severity_returns_high_for_failed_other_job(self, mock_logger):
        expected = HIGH_SEVERITY
        actual = batch_job_handler.get_severity(
            OTHER_JOB_QUEUE,
            FAILED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_severity_returns_high_for_succeeded_pdm_job(self, mock_logger):
        expected = HIGH_SEVERITY
        actual = batch_job_handler.get_severity(
            PDM_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_severity_returns_high_for_succeeded_other_job(self, mock_logger):
        expected = HIGH_SEVERITY
        actual = batch_job_handler.get_severity(
            OTHER_JOB_QUEUE,
            SUCCEEDED_JOB_STATUS,
            JOB_NAME,
        )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_get_severity_returns_high_for_other_status(self, mock_logger):
        expected = MEDIUM_SEVERITY
        actual = batch_job_handler.get_severity(
            OTHER_JOB_QUEUE,
            RUNNABLE_JOB_STATUS,
            JOB_NAME,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_job_is_valid_with_valid_input(self, mock_logger):
        message = {
            "detail": {
                JOB_NAME_KEY: JOB_NAME,
                JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
                JOB_QUEUE_KEY: PDM_JOB_QUEUE,
            }
        }
        event = {"Records": [{"Sns": {"Message": f"{json.dumps(message)}"}}]}

        expected = {
            JOB_NAME_KEY: JOB_NAME,
            JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
            JOB_QUEUE_KEY: PDM_JOB_QUEUE,
        }
        actual = batch_job_handler.get_and_validate_job_details(
            event,
        )

        self.assertEqual(expected, actual)

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_job_is_invalid_with_no_detail_object(self, mock_logger):
        message = {
            "test": {
                JOB_NAME_KEY: JOB_NAME,
                JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
                JOB_QUEUE_KEY: PDM_JOB_QUEUE,
            }
        }
        event = {"Records": [{"Sns": {"Message": f"{json.dumps(message)}"}}]}

        with pytest.raises(KeyError):
            actual = batch_job_handler.get_and_validate_job_details(
                event,
            )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_job_is_invalid_with_no_job_name_field(self, mock_logger):
        message = {
            "detail": {
                JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
                JOB_QUEUE_KEY: PDM_JOB_QUEUE,
            }
        }
        event = {"Records": [{"Sns": {"Message": f"{json.dumps(message)}"}}]}

        with pytest.raises(KeyError):
            actual = batch_job_handler.get_and_validate_job_details(
                event,
            )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_job_is_invalid_with_no_job_status_field(self, mock_logger):
        message = {
            "detail": {
                JOB_NAME_KEY: JOB_NAME,
                JOB_QUEUE_KEY: PDM_JOB_QUEUE,
            }
        }
        event = {"Records": [{"Sns": {"Message": f"{json.dumps(message)}"}}]}

        with pytest.raises(KeyError):
            actual = batch_job_handler.get_and_validate_job_details(
                event,
            )

    @mock.patch("batch_job_handler_lambda.batch_job_handler.logger")
    def test_job_is_invalid_with_no_job_queue_field(self, mock_logger):
        message = {
            "detail": {
                JOB_NAME_KEY: JOB_NAME,
                JOB_STATUS_KEY: SUCCEEDED_JOB_STATUS,
            }
        }
        event = {"Records": [{"Sns": {"Message": f"{json.dumps(message)}"}}]}

        with pytest.raises(KeyError):
            actual = batch_job_handler.get_and_validate_job_details(
                event,
            )


if __name__ == "__main__":
    unittest.main()
