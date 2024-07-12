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
"""Pipeline utilities."""

import collections
import dataclasses
import logging
import time
from typing import Any, Iterable, Iterator, Optional, Tuple

from google.cloud import storage

import ilm_config
import ilm_types


def read_gcs_file(gcs_uri: str) -> str:
  storage_client = storage.Client()
  blob = storage.Blob.from_string(gcs_uri, storage_client)
  return blob.download_as_text()


def write_gcs_file(
    file_content: str,
    gcs_uri: str,
    storage_client: Optional[storage.Client] = None,
):
  if storage_client is None:
    storage_client = storage.Client()
  blob = storage.Blob.from_string(gcs_uri, client=storage_client)
  blob.upload_from_string(file_content)


def delete_gcs_file(
    gcs_uri: str,
    storage_client: Optional[storage.Client] = None,
):
  if storage_client is None:
    storage_client = storage.Client()
  blob = storage.Blob.from_string(gcs_uri, client=storage_client)
  blob.delete()


def read_ilm_config(
    config_gcs_uri: str,
) -> ilm_config.ImageLifecycleManagementConfig:
  """Reads ILM pipeline config from GCS."""
  config_str = read_gcs_file(config_gcs_uri)
  try:
    return ilm_config.ImageLifecycleManagementConfig.from_json(config_str)
  except Exception as exp:
    logging.error(
        'Failed to read ILM config from %s with error: %s',
        config_gcs_uri,
        str(exp),
    )
    raise exp


def should_keep_instance(
    instance: Tuple[str, Any],
    ilm_cfg: ilm_config.ImageLifecycleManagementConfig,
) -> bool:
  """Whether to keep instance based on allowlist."""
  return instance[0] not in ilm_cfg.instances_disallow_list


def _compute_cumulative_access_count(
    log_access_metadata: ilm_types.LogAccessMetadata, num_frames: int
) -> ilm_types.AccessMetadata:
  """Computes cumulative access count including frames and instances counts."""
  num_days_to_count = collections.defaultdict(float)
  for access in log_access_metadata.frames_access_counts:
    num_days_to_count[access.num_days] += access.count
  # Divide frame counts by total number of frames.
  for access in log_access_metadata.frames_access_counts:
    num_days_to_count[access.num_days] /= num_frames
  for access in log_access_metadata.instance_access_counts:
    num_days_to_count[access.num_days] += access.count
  cumulative_access_counts = []
  total_count = 0.0
  for num_days in sorted(num_days_to_count):
    total_count += num_days_to_count[num_days]
    cumulative_access_counts.append(
        ilm_config.AccessCount(count=total_count, num_days=num_days)
    )
  return ilm_types.AccessMetadata(
      cumulative_access_counts=cumulative_access_counts
  )


def include_access_count_in_metadata(
    merged_access_count_and_metadata: Tuple[
        str,
        Tuple[
            Iterable[ilm_types.LogAccessMetadata],
            Iterable[ilm_types.InstanceMetadata],
        ],
    ],
) -> Iterator[ilm_types.InstanceMetadata]:
  """Include access count in metadata.

  Computes cumulative access counts from both instance and frames access counts.
  Expects a single InstanceMetadata element, and zero or one AccessMetadata
  element.

  Args:
    merged_access_count_and_metadata: tuple(instance, tuple(access, metadata).

  Yields:
    Instance metadata that includes access metadata.

  Raises:
    ValueError in case of unexpected access or instance metadata.
  """
  instance, (log_access_metadata, instance_metadata) = (
      merged_access_count_and_metadata
  )
  log_access_metadata = list(log_access_metadata)
  if not log_access_metadata:
    # Instance was not accessed
    log_access_metadata = [ilm_types.LogAccessMetadata([], [])]
  elif len(log_access_metadata) != 1:
    raise ValueError(
        f'Invalid access count for instance {instance}: {log_access_metadata}'
    )
  instance_metadata = list(instance_metadata)
  if not instance_metadata:
    logging.info(
        'DICOM metadata missing for instance %s. Instance may have been '
        'deleted. Skipping.',
        instance,
    )
    return
  if len(instance_metadata) != 1:
    raise ValueError(
        f'Invalid metadata for instance {instance}: {instance_metadata}'
    )
  instance_metadata = instance_metadata[0]
  log_access_metadata = log_access_metadata[0]
  if (
      log_access_metadata.frames_access_counts
      and not instance_metadata.num_frames
  ):
    logging.warning(
        'Instance %s with RetrieveFrames request is missing number of frames '
        'in metadata. Assuming number of frames is 1.',
        instance_metadata.instance,
    )
    instance_metadata = dataclasses.replace(instance_metadata, num_frames=1)
  total_access_metadata = _compute_cumulative_access_count(
      log_access_metadata, instance_metadata.num_frames
  )
  instance_metadata = dataclasses.replace(
      instance_metadata, access_metadata=total_access_metadata
  )
  yield instance_metadata


class Throttler:
  """Basic throttler for keeping an approximate target QPS or below."""

  def __init__(self, target_qps: float):
    if target_qps <= 0:
      raise ValueError('Target QPS must be positive.')
    self._delta_between_requests = 1 / target_qps
    self._last_request_time = time.time()

  def wait(self):
    delta_since_last_request = time.time() - self._last_request_time
    if delta_since_last_request < self._delta_between_requests:
      time.sleep(self._delta_between_requests - delta_since_last_request)
    self._last_request_time = time.time()
