-- Schema for DPAS Orchestrator.
-- Design Doc: go/dpas-cohorts-design
-- LINT.IfChange


-- Users Table
-- Stores user metadata for users of DPAS.
CREATE TABLE Users (
  UserId INT64 NOT NULL
) PRIMARY KEY(UserId);

-- UserAliases Table
-- Stores aliases for users for secondary lookup.
CREATE TABLE UserAliases (
  UserAlias STRING(MAX) NOT NULL,
  UserId INT64 NOT NULL,
  AliasType INT64 NOT NULL,
  FOREIGN KEY (UserId) REFERENCES Users (UserId)
) PRIMARY KEY(UserAlias);

-- Secondary index to query users by aliases and alias types.
CREATE INDEX UsersByTypedAliases ON UserAliases(UserId, AliasType);

-- Cohorts Table
-- Stores cohorts and associated cohort metadata.
CREATE TABLE Cohorts (
  CohortId INT64 NOT NULL,
  CreatorUserId INT64 NOT NULL,
  DisplayName STRING(MAX),
  Description STRING(MAX),
  -- Corresponds to PathologyCohortLifecycleStage enum value.
  CohortStage INT64,
  IsDeId BOOL,
  -- Corresponds to PathologyCohortBehaviorConstraints enum value.
  CohortBehaviorConstraints INT64,
  -- Timestamp of when cohort was created or last updated.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  -- Corresponds to PathologyCohortAccess enum value.
  CohortAccess INT64,
  ExpireTime TIMESTAMP,
  FOREIGN KEY (CreatorUserId) REFERENCES Users (UserId)
) PRIMARY KEY(CohortId),
ROW DELETION POLICY(OLDER_THAN(ExpireTime, INTERVAL 0 DAY));

-- Secondary index to query cohorts by user.
CREATE INDEX CohortsByCreatorUserId ON Cohorts(CreatorUserId);

-- Slides Table
-- Stores references to slides contained in Dicom Store by id.
CREATE TABLE Slides (
  -- Internal Orchestrator unique id for a scan derived from hash of DicomUri.
  -- The name ScanUniqueId is used to distinguish this id from 'SlideId' which
  -- represents the barcode in DPAS ingest pipeline and doesn't uniquely
  -- identify a scan.
  ScanUniqueId INT64 NOT NULL,
  -- Full Dicom path for a scan.
  -- Ex. projects/.../instances/...
  DicomUri STRING(MAX) NOT NULL,
  -- Hash field to be used for lookups.
  -- Current implementation stores hash of DicomUri using hashlib.sha256.
  LookupHash INT64 NOT NULL
) PRIMARY KEY (ScanUniqueId);

-- Secondary index to query slides by lookup hash.
CREATE INDEX SlidesByLookupHash ON Slides(LookupHash, DicomUri);

-- CohortSlides Table
-- Maps cohorts to the slides that they contain.
CREATE TABLE CohortSlides (
  CohortId INT64 NOT NULL,
  ScanUniqueId INT64 NOT NULL,
  FOREIGN KEY (ScanUniqueId) REFERENCES Slides (ScanUniqueId)
) PRIMARY KEY (CohortId, ScanUniqueId),
  INTERLEAVE IN PARENT Cohorts ON DELETE CASCADE;

-- ExportKeys Table
-- Maps cryptohash keys used for cohort export to the dicom store destination.
CREATE TABLE ExportKeys (
  DicomStoreWebUrl STRING(MAX) NOT NULL,
  -- Raw AES key to be encrypted.
  AesKey BYTES(MAX)
) PRIMARY KEY (DicomStoreWebUrl);

-- Operations Table
-- Stores metadata of current and completed operations.
CREATE TABLE Operations (
  -- Internal Orchestrator Operation Id.
  DpasOperationId INT64 NOT NULL,
  -- Corresponds to CloudHealthcareAPI Operation resource name returned from
  -- API call.
  CloudOperationName STRING(MAX) NOT NULL,
  NotificationEmails ARRAY<STRING(MAX)>,
  -- Binary wrapper of StoredPathologyOperationMetadata for operation.
  StoredPathologyOperationMetadata BYTES(MAX),
  -- Corresponds to OperationStatus enum indicating the status of the operation.
  OperationStatus INT64 NOT NULL,
  -- Corresponds to a google.rpc.code indicating the error code.
  RpcErrorCode INT64,
  -- Timestamp of when status was last checked of operation.
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(DpasOperationId);

-- CohortUserAccess Table
-- Maps cohorts to the users that have access to them.
CREATE TABLE CohortUserAccess (
  CohortId INT64 NOT NULL,
  UserId INT64 NOT NULL,
  -- Corresponds to PathologyUserAccessRole enum.
  AccessRole INT64,
  FOREIGN KEY (UserId) REFERENCES Users (UserId)
) PRIMARY KEY (CohortId, UserId),
  INTERLEAVE IN PARENT Cohorts ON DELETE CASCADE;

-- Secondary index to query cohorts by user id and access role.
CREATE INDEX CohortsByUserIdAccess ON CohortUserAccess(UserId, AccessRole);

-- SavedCohorts Table
-- Maps cohorts to the users that have saved them.
CREATE TABLE SavedCohorts (
  CohortId INT64 NOT NULL,
  UserId INT64 NOT NULL,
  FOREIGN KEY (UserId) REFERENCES Users (UserId)
) PRIMARY KEY (CohortId, UserId),
  INTERLEAVE IN PARENT Cohorts ON DELETE CASCADE

-- LINT.ThenChange(//depot/pathology.orchestrator/spanner/schema_resources.py)