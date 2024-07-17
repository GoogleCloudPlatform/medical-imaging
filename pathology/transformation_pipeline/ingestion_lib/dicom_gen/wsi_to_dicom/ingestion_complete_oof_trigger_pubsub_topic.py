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
"""Accessor for OOF pub/sub topic."""

from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import pubsub_util


def get_oof_trigger_pubsub_topic() -> str:
  """Returns pub/sub topic triggering OOF.

  Raises:
    google_exceptions.NotFound: Pub/sub topic sub is not defined.
    google_exceptions.PermissionDenied: Permission denied accessing topic.
  """
  topic = (
      ingest_flags.INGEST_COMPLETE_OOF_TRIGGER_PUBSUB_TOPIC_FLG.value.strip()
  )
  if topic:
    pubsub_util.validate_topic_exists(topic)
  return topic
