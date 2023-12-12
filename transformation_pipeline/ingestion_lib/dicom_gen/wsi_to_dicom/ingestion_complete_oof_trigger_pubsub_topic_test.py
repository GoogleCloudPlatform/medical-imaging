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
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver

from transformation_pipeline.ingestion_lib import pubsub_util
from transformation_pipeline.ingestion_lib.dicom_gen.wsi_to_dicom import ingestion_complete_oof_trigger_pubsub_topic


class IngestCompletePubSubTopicTest(absltest.TestCase):

  def test_get_oof_trigger_pubsub_topic_default(self):
    self.assertEmpty(
        ingestion_complete_oof_trigger_pubsub_topic.get_oof_trigger_pubsub_topic()
    )

  @flagsaver.flagsaver(ingest_complete_oof_trigger_pubsub_topic=' foo ')
  @mock.patch.object(pubsub_util, 'validate_topic_exists', autospec=True)
  def test_get_oof_trigger_pubsub_topic_initialized(
      self, unused_validate_topic_exits
  ):
    self.assertEqual(
        ingestion_complete_oof_trigger_pubsub_topic.get_oof_trigger_pubsub_topic(),
        'foo',
    )


if __name__ == '__main__':
  absltest.main()
