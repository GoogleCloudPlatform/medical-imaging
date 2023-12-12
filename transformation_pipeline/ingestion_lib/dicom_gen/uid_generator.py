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
_MAX_UID_COUNTER_VALUE = 999
_MAX_UID_COUNTER_CHAR_LENGTH = len(str(_MAX_UID_COUNTER_VALUE))
_TEST_TEXT_FOR_NON_NUMBER_CHAR_REGEX = '.*[^0-9].*'
_MINIMUM_POD_HOST_COMPONENT_LENGTH = 20


class InvalidUIDPrefixError(Exception):
  pass


class _UIDGenerator(object):
  """Static state for UID generator."""

  counter = None  # Internal counter for UID Allocation
  rand_fraction = None  # Random component of POD UID. Initialized once per pod.
  lock = threading.Lock()  # It it is not safe to generate multiple UID
  # simultaneously.
  prefix = ''  # Holds validated UID prefix used for pod.
  pod = ''  # Representation of embedding of POD Name in UID.
  clip_pod = 0  # number of characters to clip pod str.
  last_time_str_len = None  # Length last reported time string. Used to check if
  # the time string length changes.


def _log_and_raise(msg: str, exp: Optional[Exception] = None):
  """_getprefix helper function logs and raises InvalidUIDPrefixError.

  Args:
    msg: message to log and raise.
    exp: Optional exception to reference raised exception from.

  Raises:
    InvalidUIDPrefixError: Error in uid prefix definition.
  """
  cloud_logging_client.logger().critical(msg)
  if exp is None:
    raise InvalidUIDPrefixError(msg)
  raise InvalidUIDPrefixError(msg) from exp


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
  # check that UID starts with correct character
  # DICOM Standard requirement
  first_char_value = ord(block[0])
  if len(block) == 1 and first_char_value < ord('0'):
    return False
  elif len(block) > 1 and first_char_value < ord('1'):
    return False
  return True


def _get_prefix() -> str:
  """Conduct one time test on first use that customer defined uid prefix valid.

  Returns:
    UID prefix as string.
  Raises:
    InvalidUIDPrefixError: Error in uid prefix definition.
  """
  if not _UIDGenerator.prefix:
    prefix = ingest_flags.DICOM_GUID_PREFIX_FLG.value
    if prefix is None or not prefix:
      msg = 'DICOM UID prefix is undefined.'
      cloud_logging_client.logger().critical(msg)
      raise InvalidUIDPrefixError(msg)

    # clean up customer defined uid (set by flag or env variable)
    # Removes starting and ending white space around uid and trailing periods.
    _UIDGenerator.prefix = prefix.strip().rstrip('.')

    # Validates UID prefix starts with DPAS UID
    if not prefix.startswith(ingest_const.DPAS_UID_PREFIX):
      _log_and_raise(
          f'DICOM uid prefix must start with "{ingest_const.DPAS_UID_PREFIX}". '
          f'The prefix is defined as "{prefix}"'
      )

    # Verify that UID defines one additional uid block after the DPAS UID.
    # e.g., if DPAS UID was 1.2.3,   we are makeing sure that uid is defined as
    #                       1.2.3.4
    uid_parts = _UIDGenerator.prefix.split('.')
    base_dpas_uid_block_length = len(ingest_const.DPAS_UID_PREFIX.split('.'))
    customer_uid_len = len(uid_parts)
    if (
        base_dpas_uid_block_length + 1 != customer_uid_len
        and base_dpas_uid_block_length + 2 != customer_uid_len
    ):
      _log_and_raise(
          (
              'DICOM uid suffix must be defined with 1 or 2 sub-domains '
              f'of {ingest_const.DPAS_UID_PREFIX}'
          )
      )
    for uid_block_index in range(base_dpas_uid_block_length, customer_uid_len):
      # Check that last block conforms to DICOM spec requirements
      test_block = uid_parts[uid_block_index]
      if not is_uid_block_correctly_formatted(test_block):
        _log_and_raise('DICOM UID suffix is incorrectly formatted.')
      # Not DICOM requirement, but one for DPAS, make sure block is small,
      # 3 digits or less, required to retain space for other UID components.
      if not test_block or len(test_block) > 3:
        _log_and_raise(
            'DICOM uid suffix must end with a suffix of 3 or less digits.'
        )

  return _UIDGenerator.prefix


def _get_pod() -> str:
  """Returns GKE POD ID component of UID.

  Returns:
    POD ID component of UID as str
  """
  if not _UIDGenerator.pod:
    hostname = cloud_logging_client.POD_HOSTNAME_FLG.value
    if not hostname:
      cloud_logging_client.logger().critical('HOSTNAME env not set.')
      raise ValueError('HOSTNAME env is not set')
    parts = hostname.split('-')
    pod_name = parts[-1]
    pod_name_hex = pod_name.encode('utf-8').hex()
    pod_name_uid = str(int(pod_name_hex, 16))
    if _UIDGenerator.clip_pod == 0:
      _UIDGenerator.pod = pod_name_uid
    else:
      _UIDGenerator.pod = pod_name_uid[: -_UIDGenerator.clip_pod]
  return _UIDGenerator.pod


def _get_time() -> str:
  """Returns unix time in milliseconds.

  Example: '1626105674575' # 13 characters
  """
  time_str = str(int(time.time() * 1000))  # time to nearest millisecond
  if _UIDGenerator.last_time_str_len is None:
    _UIDGenerator.last_time_str_len = len(time_str)
  return time_str


def _get_counter() -> str:
  if _UIDGenerator.counter is None:
    _UIDGenerator.counter = random.randint(0, _MAX_UID_COUNTER_VALUE - 1)
  _UIDGenerator.counter += 1
  if _UIDGenerator.counter > _MAX_UID_COUNTER_VALUE:
    _UIDGenerator.counter = 1
  counter = str(_UIDGenerator.counter)
  zero_padding = (_MAX_UID_COUNTER_CHAR_LENGTH - len(counter)) * '0'
  return f'{zero_padding}{counter}'


def _init_random_fraction():
  """Initializes random fraction of guid and clips host name fraction.

  Random fraction is dermined by generating POD
  """
  random.seed(None)  # Seed with system time or os methods
  prefix = _get_prefix()
  pod = _get_pod()
  tme = _get_time()
  counter = _get_counter()
  # Minial components of uid.  1 = place holder for smallest random part
  uid_parts = [prefix, pod, '1', f'{tme}{counter}']
  # test if UID is overlength
  over_length = len('.'.join(uid_parts)) - MAX_LENGTH_OF_DICOM_UID
  if over_length == 0:
    # valid for single diget parts to start with 0
    _UIDGenerator.rand_fraction = str(random.randint(0, 9))
  elif over_length > 0:
    # Generated UID is to long.
    # attempt to shortern UID by clipping the pod host name component.
    # require that pod host name be a minimum of 20 digets, unclipped.
    if len(pod) - over_length <= _MINIMUM_POD_HOST_COMPONENT_LENGTH:
      raise ValueError('dicom_guid_prefix is excessively long')
    _UIDGenerator.clip_pod = over_length
    _UIDGenerator.rand_fraction = str(random.randint(1, 9))
  else:
    # common path, uid is shorter than max.
    # add random digits until uid == MAX_LENGTH_OF_DICOM_UID
    # random component cannot start with 0
    rand_fraction = [str(random.randint(1, 9))]
    for _ in range(-over_length):
      rand_fraction.append(str(random.randint(0, 9)))
    _UIDGenerator.rand_fraction = ''.join(rand_fraction)


def generate_uid() -> str:
  """Generates a globally unique DICOM GUID for DPAS.

    Composed of: Numbers[0-9] separated by.
    max length = 64 characters
    blocks of numbers, cannot lead with 0.

    Overall format:
    <CUSTOMER_PREFIX>.<GKEHOSTNAME>.<RANDOM>.<TIME_COUNTER>
    RANDOM block initialized once.

    Purpose for <TIME_COUNTER>: Enables multiple calls to generate uid to
    return concecutively unique uids for a given host. A counter is appended to
    the back of time to enable calls to genreate_uid within a millisecond to
    return unique uids.

  Returns:
      DICOM GUID as a string
  """
  with _UIDGenerator.lock:
    if _UIDGenerator.rand_fraction is None:
      _init_random_fraction()

    tme = _get_time()
    if len(tme) != _UIDGenerator.last_time_str_len:
      # Time component of UID changed length.
      # Regenerate random fraction to account for this.
      _UIDGenerator.last_time_str_len = len(tme)
      _init_random_fraction()

    prefix = _get_prefix()
    pod = _get_pod()
    rand_fraction = _UIDGenerator.rand_fraction
    counter = _get_counter()
    uid = '.'.join([prefix, pod, rand_fraction, f'{tme}{counter}'])
    if len(uid) > MAX_LENGTH_OF_DICOM_UID:
      raise ValueError('UID length exceeds max length for DICOM UID')
    cloud_logging_client.logger().debug('Generated GUID', {'uid': str(uid)})
    return uid


def validate_uid_prefix() -> bool:
  """Tests if UID prefix correctly defined.

  Returns:
    True: Valid uid prefix.

  Raises:
     InvalidUIDPrefixError: UID prefix is invalid.
  """
  with _UIDGenerator.lock:
    return bool(_get_prefix())
