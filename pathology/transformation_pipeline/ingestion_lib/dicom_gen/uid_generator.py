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
"""Standardized UID generator used to generate all DICOM uids."""
import os
import random
import re
import threading
import time
from typing import Optional

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import ingest_const

TEST_UID_PREFIX = f'{ingest_const.DPAS_UID_PREFIX}.0.0'
MAX_LENGTH_OF_DICOM_UID = 64  # DICOM Specification
_MIN_UID_COUNTER_VALUE = 0
_MAX_UID_COUNTER_VALUE = 999
_TEST_TEXT_FOR_NON_NUMBER_CHAR_REGEX = '.*[^0-9].*'
_MAX_HOSTNAME_COMPONENT_LENGTH = 12


class InvalidUIDPrefixError(Exception):
  pass


def _log_and_raise(msg: str):
  """_getprefix helper function logs and raises InvalidUIDPrefixError.

  Args:
    msg: message to log and raise.

  Raises:
    InvalidUIDPrefixError: Error in uid prefix definition.
  """
  cloud_logging_client.critical(msg)
  raise InvalidUIDPrefixError(msg)


def is_uid_block_correctly_formatted(block: str) -> bool:
  """Tests that a UID block is correctly formatted.

  Args:
    block: UID block to test.

  Returns:
    True if correctly formatted.
  """
  # Check block is numeric only
  if not block:
    return False
  if re.fullmatch(_TEST_TEXT_FOR_NON_NUMBER_CHAR_REGEX, block):
    return False
  # check that UID starts with number 1 or greater if the length of the block
  # is greater than 1
  if len(block) > 1 and ord(block[0]) < ord('1'):
    return False
  return True


def _get_uid(uid_parts: list[str]) -> str:
  return '.'.join([part for part in uid_parts if part])


def validate_uid_format(uid: str) -> bool:
  if not uid:  # Empty uid are valid
    return True
  if len(uid) > MAX_LENGTH_OF_DICOM_UID:
    return False
  for block in uid.split('.'):
    if not is_uid_block_correctly_formatted(block):
      return False
  return True


class _UIDGeneratorState:
  """Module level state for UID generator."""

  def __init__(self, seed: Optional[int] = None):
    random.seed(seed)  # Seed with system time or os methods

    # Internal counter for UID Allocation
    self._counter = random.randint(
        _MIN_UID_COUNTER_VALUE, _MAX_UID_COUNTER_VALUE
    )

    # Random component of UID. Initialized once.
    self._rand_fraction = ''

    # Length of last reported time string. Used to check if the time string
    # length changes. If string length changes we recompute the random
    # component of the UID to avoid exceeding the UID length limits.
    self._last_time_str_len: int = -1

    prefix = ingest_flags.DICOM_GUID_PREFIX_FLG.value.strip().rstrip('.')
    if not prefix:
      _log_and_raise('DICOM UID prefix flag is undefined.')
    # Validates UID prefix starts with DPAS UID
    if not prefix.startswith(ingest_const.DPAS_UID_PREFIX):
      _log_and_raise(
          'DICOM uid prefix flag must start with'
          f' "{ingest_const.DPAS_UID_PREFIX}". The prefix is defined as'
          f' "{prefix}"'
      )
    # Verify that UID defines no more than 7 characters after DPAS UID and that
    # the UID matchs DICOM UID formatting requirements.
    # https://dicom.nema.org/dicom/2013/output/chtml/part05/chapter_9.html
    uid_parts = prefix.split('.')
    base_dpas_uid_block_length = len(ingest_const.DPAS_UID_PREFIX.split('.'))
    customer_uid_len = len(uid_parts)
    for uid_block_index in range(base_dpas_uid_block_length, customer_uid_len):
      # Check that blocks conforms to DICOM spec requirements
      if not is_uid_block_correctly_formatted(uid_parts[uid_block_index]):
        _log_and_raise('DICOM UID suffix is incorrectly formatted.')
    # Not DICOM requirement, but one for DPAS, make sure total size of customer
    # block is small, no less than 7 characters to retain space for other
    # components of the UID.
    customer_ext = '.'.join(uid_parts[base_dpas_uid_block_length:])
    if len(customer_ext) > 7:
      _log_and_raise(
          'DICOM UID customer prefix following the DPAS UID must be <= 7 chars'
          f' in length. DPAS UID Prefix: {prefix}; '
          f'Customer prefix: {customer_ext} exceeds 7 chars.'
      )
    self._prefix = prefix

    # Representation of embedding of hostname in UID.
    hostname = cloud_logging_client.POD_HOSTNAME_FLG.value
    if hostname is None or not hostname:
      self._hostname_uid = ''
    else:
      parts = hostname.strip().split('-')
      hostname = parts[-1]
      hostname_hex = hostname.encode('utf-8').hex()
      self._hostname_uid = str(int(hostname_hex, 16))[
          :_MAX_HOSTNAME_COMPONENT_LENGTH
      ]
    self._init_random_fraction()

  def _init_random_fraction(self):
    """Initializes random fraction of UID."""
    hostname = self._hostname_uid
    counter_str = self.get_counter_str()
    time_str = self.get_time_str()
    prefix_str = self.get_prefix_str()
    self._last_time_str_len = len(time_str)
    # Minial components of uid.  1 = place holder for smallest random part
    uid = _get_uid([prefix_str, hostname, '1', f'{time_str}{counter_str}'])
    # test if UID is overlength
    over_length = len(uid) - MAX_LENGTH_OF_DICOM_UID
    if over_length > 0:
      # This should never happen due to max len constraints on each of the UID
      # components.
      raise ValueError('UID length exceeds max length for DICOM UID')
    # common path, uid is shorter than max.
    # add random digits until uid == MAX_LENGTH_OF_DICOM_UID
    # random component cannot start with 0
    rand_fraction = [str(random.randint(1, 9))]
    for _ in range(-over_length):
      rand_fraction.append(str(random.randint(0, 9)))
    self._rand_fraction = ''.join(rand_fraction)

  def get_prefix_str(self) -> str:
    return self._prefix

  def get_hostname_str(self) -> str:
    return self._hostname_uid

  def get_time_str(self) -> str:
    """Returns unix time in milliseconds.

    Example: '1626105674575' # 13 characters
    """
    time_str = str(int(time.time() * 1000))  # time to nearest millisecond
    return time_str

  def get_counter_str(self) -> str:
    self._counter += 1
    if self._counter > _MAX_UID_COUNTER_VALUE:
      self._counter = _MIN_UID_COUNTER_VALUE
    counter = str(self._counter)
    max_uid_counter_length = len(str(_MAX_UID_COUNTER_VALUE))
    zero_padding = (max_uid_counter_length - len(counter)) * '0'
    return f'{zero_padding}{counter}'

  def init_random_state_if_time_str_length_changed(self, time_str: str):
    if len(time_str) != self._last_time_str_len:
      # Time component of UID changed length.
      # Regenerate random fraction to account for this.
      self._init_random_fraction()

  def get_random_fraction_str(self) -> str:
    return self._rand_fraction


_uid_generator_lock = threading.Lock()
_uid_generator_state: Optional[_UIDGeneratorState] = None


def generate_uid() -> str:
  """Generates a globally unique DICOM GUID for DPAS.

    Composed of: Numbers[0-9] separated by.
    max length = 64 characters
    blocks of numbers, cannot lead with 0.

    Overall format:
    <DPAS_UID_PREFIX>.<GKEHOSTNAME>.<RANDOM>.<TIME_COUNTER>
    RANDOM block initialized once.

    Purpose for <TIME_COUNTER>: Enables multiple calls to generate uid to
    return concecutively unique uids for a given host. A counter is appended to
    the time to enable sub millisecond calls to generate_uid to return
    unique uids.

  Returns:
      DICOM GUID as a string
  """
  global _uid_generator_state
  with _uid_generator_lock:
    if _uid_generator_state is None:
      _uid_generator_state = _UIDGeneratorState()
    time_str = _uid_generator_state.get_time_str()
    _uid_generator_state.init_random_state_if_time_str_length_changed(time_str)
    prefix = _uid_generator_state.get_prefix_str()
    hostname = _uid_generator_state.get_hostname_str()
    rand_fraction = _uid_generator_state.get_random_fraction_str()
    counter = _uid_generator_state.get_counter_str()
    uid = _get_uid([prefix, hostname, rand_fraction, f'{time_str}{counter}'])
    if len(uid) > MAX_LENGTH_OF_DICOM_UID:
      raise ValueError('UID length exceeds max length for DICOM UID')
    cloud_logging_client.debug('Generated GUID', {'uid': str(uid)})
    return uid


def validate_uid_prefix() -> bool:
  """Tests if UID prefix correctly defined.

  Returns:
    True: Valid uid prefix.

  Raises:
     InvalidUIDPrefixError: UID prefix is invalid.
  """
  generate_uid()
  return True


def _init_fork_module_state() -> None:
  global _uid_generator_lock, _uid_generator_state
  _uid_generator_lock = threading.Lock()
  _uid_generator_state = None


# Init module state if module forked, after fork.
os.register_at_fork(after_in_child=_init_fork_module_state)
