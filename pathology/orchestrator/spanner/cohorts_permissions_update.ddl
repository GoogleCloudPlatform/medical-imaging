-- Alter Cohorts Table
ALTER TABLE Cohorts ADD COLUMN CohortAccess INT64;

-- Add CohortUserAccess Table
-- Maps cohorts to the users that have access to them.
CREATE TABLE CohortUserAccess (
  CohortId INT64 NOT NULL,
  UserId INT64 NOT NULL,
  -- Corresponds to PathologyUserAccessRole enum.
  AccessRole INT64,
  FOREIGN KEY (UserId) REFERENCES Users (UserId)
) PRIMARY KEY (CohortId, UserId),
  INTERLEAVE IN PARENT Cohorts ON DELETE CASCADE;

-- Add SavedCohorts Table
-- Maps cohorts to the users that have saved them.
CREATE TABLE SavedCohorts (
  CohortId INT64 NOT NULL,
  UserId INT64 NOT NULL,
  FOREIGN KEY (UserId) REFERENCES Users (UserId)
) PRIMARY KEY (CohortId, UserId),
  INTERLEAVE IN PARENT Cohorts ON DELETE CASCADE;

-- Drop SharingLinks tables
DROP TABLE GrantViews;
DROP TABLE GrantEdits;
DROP TABLE SavedLinks;
DROP INDEX SharingLinksByRequestHash;
DROP TABLE SharingLinks;

-- Secondary index to query cohorts by user id and access role.
CREATE INDEX CohortsByUserIdAccess ON CohortUserAccess(UserId, AccessRole);