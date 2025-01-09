# Copyright 2024 Google LLC
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

"""Resources for DPAS Orchestrator Spanner Schema."""

import enum


class OperationStatus(enum.Enum):
  """Enum to define status of an operation."""
  STATUS_UNSPECIFIED = 0
  IN_PROGRESS = 1
  COMPLETE = 2


class OrchestratorTables(enum.Enum):
  """Enum to define the table name."""
  USERS = 1
  USER_ALIASES = 2
  COHORTS = 3
  SLIDES = 4
  COHORT_SLIDES = 5
  EXPORT_KEYS = 6
  OPERATIONS = 7
  COHORT_USER_ACCESS = 8
  SAVED_COHORTS = 9


ORCHESTRATOR_TABLE_NAMES = {
    OrchestratorTables.USERS: 'Users',
    OrchestratorTables.USER_ALIASES: 'UserAliases',
    OrchestratorTables.COHORTS: 'Cohorts',
    OrchestratorTables.SLIDES: 'Slides',
    OrchestratorTables.COHORT_SLIDES: 'CohortSlides',
    OrchestratorTables.EXPORT_KEYS: 'ExportKeys',
    OrchestratorTables.OPERATIONS: 'Operations',
    OrchestratorTables.COHORT_USER_ACCESS: 'CohortUserAccess',
    OrchestratorTables.SAVED_COHORTS: 'SavedCohorts',
}

# Column Headers.

# Users.
USER_ID = 'UserId'

# UserAliases.
USER_ALIAS = 'UserAlias'
ALIAS_TYPE = 'AliasType'

# Cohorts.
COHORT_ID = 'CohortId'
CREATOR_USER_ID = 'CreatorUserId'
DISPLAY_NAME = 'DisplayName'
DESCRIPTION = 'Description'
COHORT_STAGE = 'CohortStage'
IS_DEID = 'IsDeId'
COHORT_BEHAVIOR_CONSTRAINTS = 'CohortBehaviorConstraints'
COHORT_UPDATE_TIME = 'UpdateTime'
COHORT_ACCESS = 'CohortAccess'
EXPIRE_TIME = 'ExpireTime'
COHORTS_BY_CREATOR_USER_ID = 'CohortsByCreatorUserId'

# Slides.
SCAN_UNIQUE_ID = 'ScanUniqueId'
DICOM_URI = 'DicomUri'
LOOKUP_HASH = 'LookupHash'
SLIDES_BY_LOOKUP_HASH = 'SlidesByLookupHash'

# Operations.
DPAS_OPERATION_ID = 'DpasOperationId'
CLOUD_OPERATION_NAME = 'CloudOperationName'
NOTIFICATION_EMAILS = 'NotificationEmails'
STORED_PATHOLOGY_OPERATION_METADATA = 'StoredPathologyOperationMetadata'
OPERATION_STATUS = 'OperationStatus'
RPC_ERROR_CODE = 'RpcErrorCode'
OP_UPDATE_TIME = 'UpdateTime'

# CohortsUserAccess.
ACCESS_ROLE = 'AccessRole'
COHORTS_BY_USER_ID_ACCESS = 'CohortsByUserIdAccess'

# ExportKeys.
DICOM_STORE_WEB_URL = 'DicomStoreWebUrl'
AES_KEY = 'AesKey'

USERS_COLS = [USER_ID]
USER_ALIASES_COLS = [USER_ALIAS, USER_ID, ALIAS_TYPE]
COHORTS_COLS = [
    COHORT_ID, CREATOR_USER_ID, DISPLAY_NAME, DESCRIPTION, COHORT_STAGE,
    IS_DEID, COHORT_BEHAVIOR_CONSTRAINTS, COHORT_UPDATE_TIME, COHORT_ACCESS,
    EXPIRE_TIME
]
COHORTS_BY_CREATOR_USER_ID_COLS = [CREATOR_USER_ID, COHORT_ID]

# CreatorUserId, CohortAccess, ExpireTime are not updated in an update call.
COHORTS_UPDATE_COLS = [
    COHORT_ID, DISPLAY_NAME, DESCRIPTION, COHORT_STAGE, IS_DEID,
    COHORT_BEHAVIOR_CONSTRAINTS, COHORT_UPDATE_TIME
]
# Used to update cohort sharing access.
COHORTS_UPDATE_SHARING_COLS = [COHORT_ID, COHORT_ACCESS]
# Used to activate a pending cohort.
COHORTS_ACTIVATE_COLS = [COHORT_ID, COHORT_STAGE, COHORT_UPDATE_TIME]
# Used to suspend a cohort.
COHORTS_SUSPEND_COLS = [
    COHORT_ID, COHORT_STAGE, COHORT_UPDATE_TIME, EXPIRE_TIME
]

SLIDES_COLS = [SCAN_UNIQUE_ID, DICOM_URI, LOOKUP_HASH]
SLIDES_BY_LOOKUP_HASH_COLS = [LOOKUP_HASH, DICOM_URI, SCAN_UNIQUE_ID]
COHORT_SLIDES_COLS = [COHORT_ID, SCAN_UNIQUE_ID]
EXPORT_KEYS_COLS = [DICOM_STORE_WEB_URL, AES_KEY]
OPERATIONS_COLS = [
    DPAS_OPERATION_ID, CLOUD_OPERATION_NAME, NOTIFICATION_EMAILS,
    STORED_PATHOLOGY_OPERATION_METADATA, OPERATION_STATUS, RPC_ERROR_CODE,
    OP_UPDATE_TIME
]
OPERATIONS_UPDATE_STATUS_COLS = [
    DPAS_OPERATION_ID, OPERATION_STATUS, RPC_ERROR_CODE, OP_UPDATE_TIME
]
OPERATIONS_UPDATE_TIME_COLS = [DPAS_OPERATION_ID, OP_UPDATE_TIME]
COHORT_USER_ACCESS_COLS = [COHORT_ID, USER_ID, ACCESS_ROLE]
SAVED_COHORTS_COLS = [COHORT_ID, USER_ID]
