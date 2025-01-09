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
#
# ==============================================================================
"""Main entrypoint for Refresher GKE pod that checks the status of DPAS LROs."""

from absl import app as absl_app

from pathology.orchestrator.refresher import refresher_thread
from pathology.shared_libs.build_version import build_version
from pathology.shared_libs.logging_lib import cloud_logging_client


def main(unused_argv):
  """Main refresher loop."""
  build_version.init_cloud_logging_build_version()
  refresher = refresher_thread.RefresherThread()
  cloud_logging_client.info('DPAS Refresher starting.')
  try:
    while True:
      refresher.run()
      cloud_logging_client.info(
          'Refresher is going to sleep.',
          {'sleep_seconds': refresher_thread.SLEEP_SECONDS_FLG.value},
      )
      refresher.sleep()
  except Exception as exp:
    cloud_logging_client.info('Unexpected exception', exp)
    raise


if __name__ == '__main__':
  absl_app.run(main)
