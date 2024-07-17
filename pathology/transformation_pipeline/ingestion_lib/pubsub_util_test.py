# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Tests for ingestion_complete_oof_trigger_pubsub_topic."""
from absl.testing import absltest
from google.api_core import exceptions as google_exceptions
import google.auth
from google.cloud import pubsub_v1
import mock
from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import pubsub_util


class PubSubUtilTest(absltest.TestCase):

  @mock.patch.object(cloud_logging_client, 'critical')
  @mock.patch.object(
      google.auth,
      'default',
      autospec=True,
      return_value=(
          mock.create_autospec(
              google.auth.credentials.Credentials, instance=True
          ),
          'mock-gcp-project',
      ),
  )
  @mock.patch.object(pubsub_v1.PublisherClient, 'get_topic', autospec=True)
  def test_unfound_subscription_rasise_and_logs(
      self, get_topic_mock, unused_auth_mock, mock_logger
  ):
    exp = google_exceptions.NotFound('')
    get_topic_mock.side_effect = exp
    with self.assertRaises(google_exceptions.NotFound):
      pubsub_util.validate_topic_exists('foo1')
    mock_logger.assert_called_once_with(
        'Pub/sub topic is not defined.', {'pubsub_topic_name': 'foo1'}, exp
    )

  @mock.patch.object(cloud_logging_client, 'critical')
  @mock.patch.object(
      google.auth,
      'default',
      autospec=True,
      return_value=(
          mock.create_autospec(
              google.auth.credentials.Credentials, instance=True
          ),
          'mock-gcp-project',
      ),
  )
  @mock.patch.object(pubsub_v1.PublisherClient, 'get_topic', autospec=True)
  def test_permission_denined_rasise_and_logs(
      self, get_topic_mock, unused_auth_mock, mock_logger
  ):
    exp = google_exceptions.PermissionDenied('')
    get_topic_mock.side_effect = exp
    with self.assertRaises(google_exceptions.PermissionDenied):
      pubsub_util.validate_topic_exists('foo2')
    mock_logger.assert_called_once_with(
        'Permission denied accessing pub/sub topic.',
        {'pubsub_topic_name': 'foo2'},
        exp,
    )

  @mock.patch.object(cloud_logging_client, 'critical')
  @mock.patch.object(
      google.auth,
      'default',
      autospec=True,
      return_value=(
          mock.create_autospec(
              google.auth.credentials.Credentials, instance=True
          ),
          'mock-gcp-project',
      ),
  )
  @mock.patch.object(pubsub_v1.PublisherClient, 'get_topic', autospec=True)
  def test_unexpected_rasise_and_logs(
      self, get_topic_mock, unused_auth_mock, mock_logger
  ):
    exp = ValueError('')
    get_topic_mock.side_effect = exp
    with self.assertRaises(ValueError):
      pubsub_util.validate_topic_exists('foo3')
    mock_logger.assert_called_once_with(
        'Unexpected exception accessing pub/sub topic.',
        {'pubsub_topic_name': 'foo3'},
        exp,
    )

  @mock.patch.object(cloud_logging_client, 'logger')
  @mock.patch.object(
      google.auth,
      'default',
      autospec=True,
      return_value=(
          mock.create_autospec(
              google.auth.credentials.Credentials, instance=True
          ),
          'mock-gcp-project',
      ),
  )
  @mock.patch.object(pubsub_v1.PublisherClient, 'get_topic', autospec=True)
  def test_validate_function_is_memoized(
      self, get_topic_mock, unused_auth_mock, mock_logger
  ):
    pubsub_util.validate_topic_exists('foo4')
    pubsub_util.validate_topic_exists('foo4')
    get_topic_mock.assert_called_once()
    mock_logger.assert_not_called()


if __name__ == '__main__':
  absltest.main()
