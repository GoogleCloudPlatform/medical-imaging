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
"""Handler for PathologyUsers rpc methods of DigitalPathology service.

Implementation of PathologyUsers rpc methods for DPAS API.
"""

from typing import List, Optional

from google.api_core import exceptions
from google.cloud import spanner
from google.cloud.spanner_v1 import transaction as tr
import grpc

from google.protobuf import field_mask_pb2
from pathology.orchestrator import id_generator
from pathology.orchestrator import logging_util
from pathology.orchestrator import pathology_resources_util
from pathology.orchestrator import rpc_status
from pathology.orchestrator.spanner import schema_resources
from pathology.orchestrator.spanner import spanner_util
from pathology.orchestrator.v1alpha import users_pb2
from pathology.shared_libs.logging_lib import cloud_logging_client
from pathology.shared_libs.spanner_lib import cloud_spanner_client

_IMMUTABLE_STATE_FIELDS = ('user_id',)


def _get_alias_names(user: users_pb2.PathologyUser) -> List[str]:
  """Returns list of alias names from list of PathologyUserAlias instances.

  Args:
    user: User to parse alias names from.
  """
  return [alias.alias for alias in user.aliases]


def _build_user(
    transaction: tr.Transaction,
    user_id: int,
    view: users_pb2.PathologyUserAliasListView = users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NO_ALIASES,
    alias_filter: Optional[str] = None,
) -> users_pb2.PathologyUser:
  """Builds a PathologyUser instance adding the aliases from a spanner lookup.

  Args:
    transaction: Transaction to run Spanner reads/writes on.
    user_id: User Id for user.
    view: Specifies the alias types to include in response.
    alias_filter: String value for filtering by alias.

  Returns:
    PathologyUser instance.

  Raises:
    RpcFailure error if no aliases are present after filtering.
  """
  user = users_pb2.PathologyUser(
      name=pathology_resources_util.convert_user_id_to_name(str(user_id)),
      user_id=str(user_id),
  )
  if view == users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NO_ALIASES:
    return user
  if view == users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NAME_ONLY:
    # Look up only alias names.
    alias_rows = transaction.read(
        'UserAliases',
        schema_resources.USER_ALIASES_COLS,
        spanner.KeySet([[user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_NAME]]),
        'UsersByTypedAliases',
    )
    for r in alias_rows:
      dict_r = dict(zip(schema_resources.USER_ALIASES_COLS, r))
      if (
          alias_filter
          and alias_filter in dict_r['UserAlias']
          or not alias_filter
      ):
        user.aliases.append(
            users_pb2.PathologyUserAlias(
                alias=dict_r['UserAlias'], alias_type=dict_r['AliasType']
            )
        )
  else:
    alias_rows = transaction.read(
        'UserAliases',
        schema_resources.USER_ALIASES_COLS,
        spanner.KeySet([
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL],
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_NAME],
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_UNSPECIFIED],
        ]),
        'UsersByTypedAliases',
    )
    for r in alias_rows:
      dict_r = dict(zip(schema_resources.USER_ALIASES_COLS, r))
      if (
          alias_filter
          and alias_filter in dict_r['UserAlias']
          or not alias_filter
      ):
        user.aliases.append(
            users_pb2.PathologyUserAlias(
                alias=dict_r['UserAlias'], alias_type=dict_r['AliasType']
            )
        )
  if not user.aliases:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not retrieve user {user_id}. No aliases were found in'
            ' database for user with requested filters.',
        )
    )
  return user


def _get_user_by_id(
    transaction: tr.Transaction,
    user_id: int,
    alias_view: users_pb2.PathologyUserAliasListView = users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NO_ALIASES,
) -> users_pb2.PathologyUser:
  """Returns the user associated with a given id.

  Args:
    transaction: Transaction to run spanner reads/writes on.
    user_id: Id of user to retrieve.
    alias_view: Specifies the alias types to include in the response.
  """
  row = transaction.read(
      'Users', schema_resources.USERS_COLS, spanner.KeySet(keys=[[user_id]])
  )
  try:
    row.one()
    return _build_user(transaction, user_id, alias_view)
  except exceptions.NotFound as exc:
    raise rpc_status.RpcFailureError(
        rpc_status.build_rpc_method_status_and_log(
            grpc.StatusCode.NOT_FOUND,
            f'Could not retrieve user {user_id}.',
            exp=exc,
        )
    ) from exc


def insert_user_in_table(
    transaction, user: users_pb2.PathologyUser
) -> users_pb2.PathologyUser:
  """Inserts user and user aliases into table.

  Args:
    transaction: Transaction to run spanner read/writes on.
    user: User to insert.

  Returns:
    PathologyUser
  """
  # Generate new user id.
  user_id = id_generator.OrchestratorIdGenerator.generate_new_id_for_table(
      transaction, 'Users', schema_resources.USERS_COLS
  )

  # Insert row in users table.
  transaction.insert('Users', schema_resources.USERS_COLS, [[user_id]])

  # Insert aliases.
  for alias in user.aliases:
    transaction.insert(
        'UserAliases',
        schema_resources.USER_ALIASES_COLS,
        [[alias.alias, user_id, alias.alias_type]],
    )

  user.name = pathology_resources_util.convert_user_id_to_name(str(user_id))
  user.user_id = str(user_id)
  cloud_logging_client.info(f'Creating PathologyUser {user.name}.')
  return user


def _identify_or_create_user(
    transaction: tr.Transaction, alias: str
) -> users_pb2.PathologyUser:
  """Returns the current user identified by the alias or creates a new user.

  Args:
    transaction: Spanner transaction to run reads/writes on.
    alias: Alias to identify user.

  Returns:
    PathologyUser
  """
  try:
    user_id = spanner_util.search_user_by_alias(alias, transaction)
    cloud_logging_client.info(f'Searching for user with alias {alias}.')
    return _get_user_by_id(
        transaction,
        user_id,
        alias_view=users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NO_ALIASES,
    )
  except (exceptions.NotFound, rpc_status.RpcFailureError):
    cloud_logging_client.info(f'Creating new PathologyUser with alias {alias}.')
    # User was not found, create new user.
    return insert_user_in_table(
        transaction,
        users_pb2.PathologyUser(
            aliases=[
                users_pb2.PathologyUserAlias(
                    alias=alias,
                    alias_type=users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL,
                )
            ]
        ),
    )


def _list_users(
    transaction: tr.Transaction,
    alias_view: users_pb2.PathologyUserAliasListView = users_pb2.PATHOLOGY_USER_ALIAS_LIST_VIEW_NO_ALIASES,
    alias_filter: str = '',
) -> List[users_pb2.PathologyUser]:
  """Returns a list of PathologyUsers in database.

  Args:
    transaction: Transaction to run Spanner reads/writes on.
    alias_view: View to return aliases.
    alias_filter: Filter for aliases.
  """
  result = []
  rows = transaction.read(
      'Users', schema_resources.USERS_COLS, spanner.KeySet(all_=True)
  )
  for r in rows:
    try:
      user = _build_user(transaction, r[0], alias_view, alias_filter)
      result.append(user)
    except rpc_status.RpcFailureError as rpc_err:
      cloud_logging_client.warning(rpc_err.status.error_msg)
      continue
  return result


def _process_update_mask(update_mask: field_mask_pb2.FieldMask) -> None:
  """Processes the update mask to check for invalid fields.

  Args:
    update_mask: Field mask with fields to update.

  Raises:
    RpcFailureError if invalid fields are found.
  """
  for field in update_mask.paths:
    if field in _IMMUTABLE_STATE_FIELDS:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'Field {field} is an immutable state field and cannot be '
              'manually updated.',
          )
      )
    if field != 'aliases':
      # No other mutable field in PathologyUser.
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              f'Invalid mask provided. Field in {update_mask.paths} is '
              'immutable or does not exist for users.',
          )
      )


def _update_aliases_in_table(
    transaction, user_id: int, user: users_pb2.PathologyUser
) -> None:
  """Updates user aliases in Spanner table and removes any deleted aliases.

  Args:
    transaction: Transaction to run spanner read/writes on.
    user_id: User id to associate alias with.
    user: User instance with updated aliases.
  """
  # Insert aliases in table.
  alias_vals = [
      [alias.alias, user_id, alias.alias_type] for alias in user.aliases
  ]
  transaction.insert_or_update(
      'UserAliases', schema_resources.USER_ALIASES_COLS, alias_vals
  )

  # Check for removed aliases.
  alias_rows = transaction.read(
      'UserAliases',
      schema_resources.USER_ALIASES_COLS,
      spanner.KeySet([
          [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL],
          [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_NAME],
          [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_UNSPECIFIED],
      ]),
      'UsersByTypedAliases',
  )
  alias_names = _get_alias_names(user)
  for row in alias_rows:
    alias_name = row[schema_resources.USER_ALIASES_COLS.index('UserAlias')]
    if alias_name not in alias_names:
      transaction.delete('UserAliases', spanner.KeySet(keys=[[alias_name]]))


class PathologyUsersHandler:
  """Handler for PathologyUsers rpc methods of DigitalPathology service."""

  def __init__(self):
    super().__init__()
    self._spanner_client = cloud_spanner_client.CloudSpannerClient()

  def _verify_user_aliases(
      self, user_id: int, user: users_pb2.PathologyUser
  ) -> bool:
    """Verifies if the UserAliases rows match the User proto's aliases.

    Args:
      user_id: Id of user.
      user: User instance to match aliases against.

    Returns:
      True if rows match proto data.
    """
    user_aliases_rows = self._spanner_client.read_data(
        'UserAliases',
        schema_resources.USER_ALIASES_COLS,
        spanner.KeySet([
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_EMAIL],
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_NAME],
            [user_id, users_pb2.PATHOLOGY_USER_ALIAS_TYPE_UNSPECIFIED],
        ]),
        'UsersByTypedAliases',
    )
    alias_names = _get_alias_names(user)
    count = 0
    for row in user_aliases_rows:
      count += 1
      if (
          row[schema_resources.USER_ALIASES_COLS.index('UserAlias')]
          not in alias_names
      ):
        return False
    return count == len(alias_names)

  def create_pathology_user(
      self, request: users_pb2.CreatePathologyUserRequest
  ) -> users_pb2.PathologyUser:
    """Creates a PathologyUser.

    Args:
        request: A CreatePathologyUserRequest with user to create.

    Returns:
      PathologyUser.

    Raises:
      RpcFailureError on failure.
    """
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.CREATE_PATHOLOGY_USER
        )
    )
    if not request.pathology_user.aliases:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INVALID_ARGUMENT,
              'User must have at least one alias.',
          )
      )

    user = request.pathology_user

    # Check if any of aliases already exist.
    alias_names = [[alias.alias] for alias in user.aliases]
    check_aliases_result = self._spanner_client.read_data(
        'UserAliases', ['UserAlias'], spanner.KeySet(keys=alias_names)
    )
    try:
      for row in check_aliases_result:
        raise rpc_status.RpcFailureError(
            rpc_status.build_rpc_method_status_and_log(
                grpc.StatusCode.ALREADY_EXISTS,
                f'User with alias {row[0]} already exists.',
            )
        )

    except exceptions.NotFound as exp:
      cloud_logging_client.info('Verified that user aliases do not exist.', exp)

    # Insert user and user aliases into table.
    return self._spanner_client.run_in_transaction(insert_user_in_table, user)

  def get_pathology_user(
      self, request: users_pb2.GetPathologyUserRequest, alias: str
  ) -> users_pb2.PathologyUser:
    """Gets a PathologyUser.

    Args:
      request: A GetPathologyUserRequest with user to retrieve.
      alias: Alias of rpc caller.

    Returns:
      PathologyUser.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.GET_PATHOLOGY_USER, resource_name
        )
    )
    cloud_logging_client.info(f'Retrieving user {resource_name}.')
    try:
      caller_id = spanner_util.search_user_by_alias(alias)
    except exceptions.NotFound:
      caller_id = None
    user_id = pathology_resources_util.get_id_from_name(resource_name)
    if not caller_id or caller_id != user_id:
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.PERMISSION_DENIED,
              f'User does not have access to requested user {resource_name}.',
          )
      )
    return self._spanner_client.run_in_transaction(
        _get_user_by_id, user_id, request.view
    )

  def identify_current_user(self, alias: str) -> users_pb2.PathologyUser:
    """Identifies the current PathologyUser by alias or creates a new user.

    Args:
      alias: Alias of rpc caller.

    Returns:
      PathologyUser.

    Raises:
      RpcFailureError on failure.
    """
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.IDENTIFY_PATHOLOGY_USER
        )
    )

    return self._spanner_client.run_in_transaction(
        _identify_or_create_user, alias
    )

  def list_pathology_users(
      self, request: users_pb2.ListPathologyUsersRequest
  ) -> users_pb2.ListPathologyUsersResponse:
    """Lists PathologyUsers.

    Args:
      request: A ListPathologyUsersRequest.

    Returns:
      ListPathologyUsersResponse including all users in database.
    """
    response = users_pb2.ListPathologyUsersResponse(
        pathology_users=self._spanner_client.run_in_transaction(
            _list_users, request.view, request.filter
        )
    )
    return response

  def update_pathology_user(
      self, request: users_pb2.UpdatePathologyUserRequest, alias: str
  ) -> users_pb2.PathologyUser:
    """Updates a PathologyUser.

    Args:
      request: An UpdatePathologyUserRequest with user to update and fields to
        update contained in a field mask.
      alias: Email of rpc caller.

    Returns:
      PathologyUser.

    Raises:
      RpcFailureError on failure.
    """
    resource_name = request.pathology_user.name
    cloud_logging_client.set_log_signature(
        logging_util.get_structured_log(
            logging_util.RpcMethodName.UPDATE_PATHOLOGY_USER, resource_name
        )
    )
    user_to_update = self.get_pathology_user(
        users_pb2.GetPathologyUserRequest(name=resource_name), alias
    )
    # Verify caller matches user to update.
    caller_id = int(self.identify_current_user(alias).user_id)
    if caller_id != int(user_to_update.user_id):
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.PERMISSION_DENIED,
              'User to update must match the caller.',
          )
      )

    # Check for invalid arguments and pass to handler to process.
    if request.update_mask:
      _process_update_mask(request.update_mask)
      user_to_update.ClearField('aliases')
      user_to_update.aliases.MergeFrom(request.pathology_user.aliases)
    else:
      cloud_logging_client.info(
          'No fields to update specified in update field mask.'
      )
      return user_to_update

    # Update aliases in spanner.
    try:
      self._spanner_client.run_in_transaction(
          _update_aliases_in_table, caller_id, user_to_update
      )
      cloud_logging_client.info(f'Updated user {user_to_update.name}.')
    except Exception as exc:
      if exc.__class__ is rpc_status.RpcFailureError:
        raise exc
      raise rpc_status.RpcFailureError(
          rpc_status.build_rpc_method_status_and_log(
              grpc.StatusCode.INTERNAL, f'Failed to update user {caller_id}.'
          )
      ) from exc

    return user_to_update
