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
"""Interface for accessing non-imaging metadata and CSV -> DICOM schema."""

from __future__ import annotations

from concurrent import futures
import copy
import dataclasses
import functools
import json
import os
import tempfile
import time
from typing import Any, Dict, List, Optional, Union

import cachetools
import google.api_core
from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage as cloud_storage
import pandas

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib import csv_util
from transformation_pipeline.ingestion_lib import hash_util
from transformation_pipeline.ingestion_lib import ingest_const
from transformation_pipeline.ingestion_lib.dicom_gen import dicom_schema_util
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard

_METADATA_DOWNLOAD_THREAD_COUNT = 2


class MetadataNotFoundExceptionError(Exception):
  pass


class MetadataDefinedOnMultipleRowError(Exception):
  """Exception when primary key value defined on multiple rows."""

  def __init__(self, primary_key: str, source: str):
    super().__init__('Primary key defined on multiple rows')
    self._primary_key = primary_key
    self._source = source

  @property
  def source(self) -> str:
    return self._source

  @property
  def primary_key(self) -> str:
    return self._primary_key


class MetadataSchemaExceptionError(Exception):
  pass


class MetadataDownloadExceptionError(Exception):
  pass


class MetadataStorageBucketNotConfiguredError(Exception):
  pass


@dataclasses.dataclass
class MetadataBlob:
  """Metadata filecache entry for csv and schema files."""

  name: str
  md5_hash: str
  size: int
  create_time: float
  filename: str

  def equals(self, obj: MetadataBlob) -> bool:
    if self.name != obj.name:
      return False
    if self.md5_hash != obj.md5_hash:
      return False
    if self.size != obj.size:
      return False
    if self.create_time != obj.create_time:
      return False
    return True


def is_schema(name: str) -> bool:
  upper_name = name.upper()
  return upper_name.endswith('.JSON') or upper_name.endswith('.SCHEMA')


def _get_key(json_dict: Dict[str, Any], search_key: str) -> Optional[str]:
  """Normalizes search_key and returns returns actual dictionary's key rep.

  Args:
    json_dict: Dictionary with string keys.
    search_key: Search key.

  Returns:
    Key in Dictionary.
  """
  search_key = dicom_schema_util.norm_column_name(search_key)
  for key in json_dict:
    norm_key = dicom_schema_util.norm_column_name(key)
    if norm_key == search_key:
      return key
  return None


class _BigQueryMetadataTableUtil:
  """Wrapper for accessing metadata from a BigQuery dataset."""

  def __init__(self, project_id: str, dataset_name: str, table_name: str):
    """Init BigQuery wrapper.

    Args:
      project_id: Project id where table is located.
      dataset_name: Dataset name for BigQuery table.
      table_name: Table name for BigQuery table.
    """
    self._client = bigquery.Client()
    self._project_id = project_id
    self._dataset_name = dataset_name
    self._table_name = table_name
    inf_schema_table = (
        f'{self._project_id}.{self._dataset_name}.INFORMATION_SCHEMA.COLUMNS'
    )
    columns_query = (
        f'SELECT COLUMN_NAME FROM {inf_schema_table} WHERE TABLE_NAME ='
        f' "{self._table_name}"'
    )
    full_table_name = (
        f'{self._project_id}.{self._dataset_name}.{self._table_name}'
    )
    bq_log = {ingest_const.LogKeywords.BIGQUERY_TABLE: full_table_name}
    query_job = self._client.query(columns_query)
    try:
      self._column_names = [row[0] for row in query_job.result()]
      cloud_logging_client.debug(
          f'Big Query Metadata Table columns: {self.column_names}', bq_log
      )
    except exceptions.GoogleAPICallError as exp:
      self._column_names = []
      msg = 'Error retrieving column names from BigQuery table.'
      cloud_logging_client.error(msg, bq_log, exp)
      raise MetadataNotFoundExceptionError(msg) from exp

  def _find_bq_column_name(self, searchtxt: str) -> Optional[str]:
    """Returns name of column in BigQuery table.

    Args:
      searchtxt: text to look for across table columns.

    Returns:
      column name (str)
    """
    searchtxt = dicom_schema_util.norm_column_name(searchtxt)
    for column_name in self.column_names:
      if dicom_schema_util.norm_column_name(column_name) == searchtxt:
        return column_name
    return None

  def get_slide_metadata(
      self,
      pk_value: str,
  ) -> Optional[bigquery.table.RowIterator]:
    """Returns query result of metadata for a slide.

    Args:
      pk_value: Primary key value to return metadata for.

    Returns:
      table.RowIterator

    Raises:
      MetadataNotFoundExceptionError: Metadata column not found in BQ table.
    """
    pk_col = self._find_bq_column_name(
        ingest_flags.METADATA_PRIMARY_KEY_COLUMN_NAME_FLG.value
    )
    full_table_name = (
        f'{self._project_id}.{self._dataset_name}.{self._table_name}'
    )
    bq_log = {ingest_const.LogKeywords.BIGQUERY_TABLE: full_table_name}
    if pk_col is None:
      msg = 'BigQuery table does not contain metadata primary key column name.'
      bq_log[ingest_const.LogKeywords.METADATA_PRIMARY_KEY_COLUMN_NAME] = (
          ingest_flags.METADATA_PRIMARY_KEY_COLUMN_NAME_FLG.value
      )
      bq_log[ingest_const.LogKeywords.BIGQUERY_TABLE_COLUMN_NAMES] = (
          self.column_names
      )
      cloud_logging_client.error(msg, bq_log)
      raise MetadataNotFoundExceptionError(msg)
    cloud_logging_client.info(
        f'Searching for BigQuery metadata for slide where {pk_col} is'
        f' {pk_value}.',
        bq_log,
    )
    query = (
        f'SELECT * from {full_table_name} WHERE {pk_col}="{pk_value}" LIMIT 2'
    )
    query_job = self._client.query(query)
    try:
      return query_job.result()
    except (
        exceptions.GoogleAPICallError,
        google.api_core.exceptions.BadRequest,
    ) as exp:
      msg = 'Error retrieving metadata from BigQuery table.'
      cloud_logging_client.error(msg, bq_log, exp)
      raise MetadataNotFoundExceptionError(msg) from exp

  @property
  def column_names(self) -> List[str]:
    return self._column_names


def _download_blob(
    storage_client,
    metadata_dir: str,
    metadata_ingest_storage_bucket: str,
    blob: MetadataBlob,
) -> str:
  """Downloads metadata blob to file."""
  _, fname = os.path.split(blob.name)
  metadata_path = os.path.join(metadata_dir, fname)
  uri = f'gs://{metadata_ingest_storage_bucket}/{blob.name}'
  try:
    with open(metadata_path, 'wb') as file_obj:
      storage_client.download_blob_to_file(uri, file_obj, raw_download=True)
  except google.api_core.exceptions.NotFound as exp:
    msg = f'Error downloading metadata {uri} to bucket {metadata_path}'
    cloud_logging_client.error(
        msg,
        {
            'source': uri,
            'dest': metadata_path,
        },
        exp,
    )
    raise MetadataDownloadExceptionError(msg) from exp
  blob.filename = metadata_path
  return str(dict(source=uri, dest=metadata_path))


class MetadataStorageClient:
  """Interface for accessing non-imaging metadata and CSV -> DICOM schema."""

  def __init__(self):
    self._slide_metadata_cache = cachetools.LRUCache(10)
    self._csv_metadata_cache = []
    self._working_root_metadata_dir = None
    metadata_upload_bucket = ingest_flags.METADATA_BUCKET_FLG.value
    if not metadata_upload_bucket:
      msg = (
          'Metadata storage bucket env variable or commandline param not'
          ' specified.'
      )
      cloud_logging_client.critical(msg)
      raise MetadataStorageBucketNotConfiguredError(msg)

    if metadata_upload_bucket.startswith('gs://'):
      metadata_upload_bucket = metadata_upload_bucket[len('gs://') :]
    self._metadata_ingest_storage_bucket = metadata_upload_bucket

  def _has_metadata_changed(self, metadata_blobs: List[MetadataBlob]) -> bool:
    """Checks if metadata has changed.

    Args:
      metadata_blobs: List of metadata currently in bucket.

    Returns:
      bool (True) if metadata is different.
    """
    if len(metadata_blobs) != len(self._csv_metadata_cache):
      return True
    for index, item in enumerate(self._csv_metadata_cache):
      if not item.equals(metadata_blobs[index]):
        return True
    return False

  def set_debug_metadata(self, filelist: Union[List[str], str]):
    """Mechanism for test util to set the csv file list.

    Args:
      filelist: Filename or list of file names.
    """
    if isinstance(filelist, str):
      filelist = [filelist]
    self._csv_metadata_cache = []
    for filepath in filelist:
      st_result = os.stat(filepath)
      md5_hash = hash_util.md5hash(filepath)
      self._csv_metadata_cache.append(
          MetadataBlob(
              filepath,
              md5_hash,
              st_result.st_size,
              st_result.st_ctime,
              filepath,
          )
      )

  def update_metadata(self):
    """Checks if cache is out of date.

    Updates cache if necessary.

    Raises:
      MetadataDownloadExceptionError
    """
    metadata_files_found = []
    try:
      storage_client = cloud_storage.Client()
      metadata_bucket = cloud_storage.bucket.Bucket(
          client=storage_client, name=self._metadata_ingest_storage_bucket
      )
      metadata_blobs = []
      bucket_uri = f'gs://{self._metadata_ingest_storage_bucket}'
      cloud_logging_client.info(
          'Checking for new wsi-slide metadata.',
          {'metadata_bucket': bucket_uri},
      )

      for blob in storage_client.list_blobs(metadata_bucket):
        name = blob.name
        upper_name = name.upper()
        if upper_name.endswith('.CSV') or is_schema(upper_name):
          md5_hash = blob.md5_hash
          size = blob.size
          create_time = blob.time_created.timestamp()
          metadata_blobs.append(
              MetadataBlob(name, md5_hash, size, create_time, '')
          )
          metadata_files_found.append(name)
    except google.api_core.exceptions.NotFound as exp:
      msg = (
          f'Error querying {self._metadata_ingest_storage_bucket} '
          ' for csv metadata.'
      )
      cloud_logging_client.error(
          msg,
          {
              'Metadata_storage_bucket': self._metadata_ingest_storage_bucket,
              'metadata_files_found': str(metadata_files_found),
          },
          exp,
      )
      raise MetadataDownloadExceptionError(msg) from exp

    metadata_blobs = sorted(
        metadata_blobs, key=lambda x: x.create_time, reverse=True
    )
    if not self._has_metadata_changed(metadata_blobs):
      cloud_logging_client.info(
          'Metadata unchanged. Using cached files.',
          {'metadata_files_found': str(metadata_files_found)},
      )
    else:
      cloud_logging_client.info(
          'Metadata changed.',
          {'metadata_files_found': str(metadata_files_found)},
      )
      if self._working_root_metadata_dir is not None:
        self._working_root_metadata_dir.cleanup()
      self._working_root_metadata_dir = tempfile.TemporaryDirectory('metadata')
      start_time = time.time()
      download_blob_partial = functools.partial(
          _download_blob,
          storage_client,
          self._working_root_metadata_dir.name,
          self._metadata_ingest_storage_bucket,
      )
      with futures.ThreadPoolExecutor(
          max_workers=_METADATA_DOWNLOAD_THREAD_COUNT
      ) as th_pool:
        downloaded_metadata_list = [
            log for log in th_pool.map(download_blob_partial, metadata_blobs)
        ]
      cloud_logging_client.info(
          'Downloaded metadata',
          {
              'metadata_file_list': str(downloaded_metadata_list),
              'download_time_sec': time.time() - start_time,
          },
      )
      self._csv_metadata_cache = metadata_blobs

  def get_slide_metadata_from_csv(self, pk_value: str) -> pandas.DataFrame:
    """Returns metadata for a slide.

    Args:
      pk_value: Slide metadata primary key value.

    Returns:
      Pandas Dataframe

    Raises:
      MetadataNotFoundExceptionError: Unable to find metadata.
      MetadataDefinedOnMultipleRowError: Metadata defined on multiple rows.
    """
    tbl = self._slide_metadata_cache.get(pk_value)
    if tbl is not None:
      return tbl.copy()
    csv_found = False
    chunksize = 10**6
    for metadata in self._csv_metadata_cache:
      if metadata.filename.upper().endswith('.CSV'):
        csv_found = True
        csv_chunks = csv_util.read_csv(metadata.filename, chunksize)
        for df in csv_chunks:
          primary_key_column_name = (
              dicom_schema_util.find_data_frame_column_name(
                  df, ingest_flags.METADATA_PRIMARY_KEY_COLUMN_NAME_FLG.value
              )
          )
          if primary_key_column_name is None:
            cloud_logging_client.warning(
                'CSV file does not contain metadata primary key column name;'
                ' CSV file ignored.',
                {
                    ingest_const.LogKeywords.FILENAME: metadata.filename,
                    ingest_const.LogKeywords.METADATA_PRIMARY_KEY_COLUMN_NAME: (
                        ingest_flags.METADATA_PRIMARY_KEY_COLUMN_NAME_FLG.value
                    ),
                },
            )
            break
          searchdf = df.loc[df[primary_key_column_name] == pk_value]
          row, _ = searchdf.shape
          if row == 1:
            cloud_logging_client.info(
                f'Primary key {pk_value} found in CSV metadata',
                {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
            )
            self._slide_metadata_cache[pk_value] = searchdf.copy()
            return searchdf
          elif row > 1:
            cloud_logging_client.error(
                'Multiple primary keys found in metadata',
                {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
            )
            raise MetadataDefinedOnMultipleRowError(pk_value, metadata.filename)

    if not csv_found:
      cloud_logging_client.error(
          'No CSV metadata found. Primary key not found in metadata',
          {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
      )
      raise MetadataNotFoundExceptionError(
          'No CSV metadata found. Primary key is not in metadata'
      )
    raise MetadataNotFoundExceptionError(
        f'Primary key: {pk_value} is not in metadata'
    )

  def get_slide_metadata_from_bigquery(
      self, project_id: str, dataset_name: str, table_name: str, pk_value: str
  ) -> Optional[dicom_schema_util.DictMetadataTableWrapper]:
    """Returns metadata for a slide from BigQuery.

    Args:
      project_id: Project id where table is located.
      dataset_name: Dataset name for BigQuery table.
      table_name: Table name for BigQuery table.
      pk_value: Slide metadata primary key value.

    Returns:
      DictMetadataTableWrapper

    Raises:
      MetadataNotFoundExceptionError: Unable to find metadata.
      MetadataDefinedOnMultipleRowError: Metadata defined on multiple rows.
    """
    tbl = self._slide_metadata_cache.get(pk_value)
    if tbl is not None:
      return tbl.copy()
    bq_table = _BigQueryMetadataTableUtil(project_id, dataset_name, table_name)
    table_result = bq_table.get_slide_metadata(pk_value)
    if table_result is None or not table_result or table_result.total_rows == 0:
      cloud_logging_client.info(
          'Primary key not found in metadata.',
          {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
      )

      raise MetadataNotFoundExceptionError(
          f'Primary key: {pk_value} not found in metadata.'
      )
    if table_result.total_rows > 1:
      cloud_logging_client.error(
          'Multiple primary keys found in metadata',
          {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
      )
      raise MetadataDefinedOnMultipleRowError(
          'Primary key found on multiple rows.',
          f'BigQuery Table; GCP Project: {project_id}; Dataset: {dataset_name};'
          f' BQ Table: {table_name}',
      )

    cloud_logging_client.info(
        'Primary key found in BigQuery metadata.',
        {ingest_const.LogKeywords.METADATA_PRIMARY_KEY: pk_value},
    )
    columns = bq_table.column_names
    for row in table_result:
      # Column names will have '_' in BigQuery.
      # https://cloud.google.com/bigquery/docs/schemas#column_names
      tbl = dicom_schema_util.DictMetadataTableWrapper(
          {columns[i].replace('_', ' '): row[i] for i in range(len(columns))}
      )
      self._slide_metadata_cache[pk_value] = tbl.copy()
      return tbl
    return None

  def _do_schemas_match(
      self,
      filename: str,
      file_scheme_def: Dict[str, str],
      search_schema_def: Dict[str, str],
  ) -> bool:
    """Tests if file schema matches search schema.

    Args:
      filename: File name of dicom schema being tested.
      file_scheme_def: DicomSchemaDef for file being tested.
      search_schema_def: Search schema definition.

    Returns:
      True if file schema_def matches search_schema
    """

    if not search_schema_def:
      msg = 'DICOM schema has not search parameters matching firs '
      cloud_logging_client.info(msg, {'filename': filename})
      return True
    for key, value in search_schema_def.items():
      found_key = _get_key(file_scheme_def, key)
      if found_key is None or not found_key:
        msg = 'DICOM schema does not have search key ignoreing key filter'
        cloud_logging_client.warning(msg, {'filename': filename, 'key': key})
        continue
      search_val = dicom_schema_util.norm_column_name(value)
      schema_val = dicom_schema_util.norm_column_name(
          file_scheme_def[found_key]
      )
      if schema_val != search_val:
        msg = 'DICOM schema does not match search key'
        cloud_logging_client.info(
            msg,
            {
                'filename': filename,
                'key': key,
                'file_value': schema_val,
                'search_value': search_val,
            },
        )
        return False
    return True

  def _norm_schema(self, schema_def: Dict[str, str]) -> Dict[str, str]:
    """Normalize schema definition.

      If SOPClassUID_Name value endings in "STORAGE" replaces with
                 "IODMODULES".

    Args:
      schema_def: Schema def to normalize.

    Returns:
      normalized copy of schema def.
    """
    schema_def = copy.copy(schema_def)
    key = _get_key(schema_def, 'SOPCLASSUID_NAME')
    if not key:
      return schema_def
    # normalize_sop_lass_name converts names representing a sop class uid
    # to a common string representation.
    norm_iod_name = (
        dicom_standard.dicom_standard_util().normalize_sop_class_name(
            schema_def[key]
        )
    )
    # norm_column_name removes spaces and case to further simplify comparison.
    schema_def[key] = dicom_schema_util.norm_column_name(norm_iod_name)
    return schema_def

  def get_dicom_schema(self, schema_search: Dict[str, Any]) -> Dict[str, Any]:
    """Returns file path containing dicom mapping schema.

    Args:
      schema_search: Parameters to search schemas.

    Returns:
      File path

    Raises:
      MetadataSchemaExceptionError: Unable to find or read DICOM schema
    """
    schema_search = self._norm_schema(schema_search)
    schema_files_found = []
    for metadata in self._csv_metadata_cache:
      schema_file = metadata.filename
      if not is_schema(schema_file):
        continue
      schema_files_found.append(schema_file)

      try:
        with open(schema_file, 'rt') as infile:
          schema = json.load(infile)
      except json.JSONDecodeError as exp:
        msg = 'Error occurred reading metadata file'
        cloud_logging_client.warning(
            msg,
            {
                'filename': schema_file,
                'Metadata_storage_bucket': self._metadata_ingest_storage_bucket,
            },
            exp,
        )
        continue
      schema_def_key = _get_key(schema, 'DICOMSCHEMADEF')
      if not schema_def_key:
        msg = 'Schema missing DICOMSchemaDef file ignored.'
        cloud_logging_client.warning(
            msg,
            {
                'filename': schema_file,
                'Metadata_storage_bucket': self._metadata_ingest_storage_bucket,
            },
        )
        continue
      scheme_def = self._norm_schema(schema[schema_def_key])
      if self._do_schemas_match(metadata.filename, scheme_def, schema_search):
        cloud_logging_client.info(
            f'DICOM schema found: {metadata.filename}',
            {
                'schema_search': str(schema_search),
                'schema_filename': metadata.filename,
            },
        )
        del schema[schema_def_key]
        return schema

    cloud_logging_client.error(
        'DICOM schema not found',
        {
            'schema_search': str(schema_search),
            'schema_files_tested': str(schema_files_found),
        },
    )
    raise MetadataSchemaExceptionError('Schema not found')
