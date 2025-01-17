/**
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** PathologyUserAccessRole */
export type PathologyUserAccessRole = 'PATHOLOGY_USER_ACCESS_ROLE_UNSPECIFIED'|
    'PATHOLOGY_USER_ACCESS_ROLE_OWNER'|'PATHOLOGY_USER_ACCESS_ROLE_ADMIN'|
    'PATHOLOGY_USER_ACCESS_ROLE_EDITOR'|'PATHOLOGY_USER_ACCESS_ROLE_VIEWER';
/** PathologyUserAliasType */
export type PathologyUserAliasType = 'PATHOLOGY_USER_ALIAS_TYPE_UNSPECIFIED'|
    'PATHOLOGY_USER_ALIAS_TYPE_EMAIL'|'PATHOLOGY_USER_ALIAS_TYPE_NAME';

/** NumericPathologyUserAccessRole */
export declare enum NumericPathologyUserAccessRole {
  PATHOLOGY_USER_ACCESS_ROLE_UNSPECIFIED = 0,
  PATHOLOGY_USER_ACCESS_ROLE_OWNER = 1,
  PATHOLOGY_USER_ACCESS_ROLE_ADMIN = 2,
  PATHOLOGY_USER_ACCESS_ROLE_EDITOR = 3,
  PATHOLOGY_USER_ACCESS_ROLE_VIEWER = 4,
}
/** PathologyUser */
export interface PathologyUser {
  name?: string;
  userId?: string;
  aliases?: PathologyUserAlias[];
}
/** PathologyUserAlias */
export interface PathologyUserAlias {
  alias?: string;
  aliasType?: PathologyUserAliasType;
}
/** PathologyUserAccess */
export interface PathologyUserAccess {
  userEmail?: string;
  accessRole?: PathologyUserAccessRole;
}
/** IdentifyCurrentUserRequest */
export interface IdentifyCurrentUserRequest {}