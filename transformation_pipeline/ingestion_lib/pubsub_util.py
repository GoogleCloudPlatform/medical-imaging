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
"""Utility functions for pub/sub."""
import functools

from google.api_core import exceptions as google_exceptions
from google.cloud import pubsub_v1

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline.ingestion_lib import ingest_const


@functools.cache
def validate_topic_exists(topic_name: str) -> None:
  """Validates pub/sub topic is defined.

  Args:
    topic_name: Name of pub/sub topic.

  Raises:
    google_exceptions.NotFound: Pub/sub topic sub is not defined.
    google_exceptions.PermissionDenied: Permission denied accessing topic.
  """
  with pubsub_v1.PublisherClient() as publisher:
    try:
      publisher.get_topic(topic=topic_name)
    except google_exceptions.NotFound as exp:
      cloud_logging_client.critical(
          'Pub/sub topic is not defined.',
          {ingest_const.LogKeywords.PUBSUB_TOPIC_NAME: topic_name},
          exp,
      )
      raise
    except google_exceptions.PermissionDenied as exp:
      cloud_logging_client.critical(
          'Permission denied accessing pub/sub topic.',
          {ingest_const.LogKeywords.PUBSUB_TOPIC_NAME: topic_name},
          exp,
      )
      raise
    except Exception as exp:
      cloud_logging_client.critical(
          'Unexpected exception accessing pub/sub topic.',
          {ingest_const.LogKeywords.PUBSUB_TOPIC_NAME: topic_name},
          exp,
      )
      raise
