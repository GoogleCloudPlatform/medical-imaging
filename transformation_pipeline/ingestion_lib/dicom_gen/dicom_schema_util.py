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
"""DICOM schema utilities."""

from __future__ import annotations

import abc
import collections
import copy
import traceback
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Set, Union

import pandas

from shared_libs.logging_lib import cloud_logging_client
from transformation_pipeline import ingest_flags
from transformation_pipeline.ingestion_lib.dicom_gen import uid_generator
from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard
from transformation_pipeline.ingestion_lib.dicom_util import dicomtag


_DICOM_TYPE_1_TAG = '1'
_DICOM_TYPE_2_TAG = '2'
DICOMSpecMetadataError = dicom_standard.DICOMSpecMetadataError


class _SchemaTagKeywords:
  """Keywords used to define tag schema."""

  CONDITIONAL_ON = 'CONDITIONAL_ON'
  FALSE = 'FALSE'
  KEYWORD = 'KEYWORD'
  META = 'META'
  META_JOIN = 'META_JOIN'
  REQUIRED = 'REQUIRED'
  SEQ = 'SEQ'
  SQ = 'SQ'
  STATIC_VALUE = 'STATIC_VALUE'
  TRUE = 'TRUE'
  VALUE_CHAR_LIMIT = 'VALUE_CHAR_LIMIT'
  WRITE_EMPTY = 'WRITE_EMPTY'


class DICOMSchemaTagError(Exception):

  def __init__(self, tag: Optional[SchemaDICOMTag], message: str):
    super().__init__(message)
    self._tag = tag

  def __str__(self):
    if self._tag is None:
      return super().__str__()
    return '\n'.join([super().__str__(), str(self._tag)])


class DICOMSchemaError(Exception):
  pass


class MissingRequiredMetadataValueError(Exception):

  def __init__(self, tag: SchemaDICOMTag):
    super().__init__('Missing required metadata value for DICOM tag.')
    self._tag = tag

  def __str__(self):
    return '\n'.join([super().__str__(), str(self._tag)])


class MetadataTableWrapper(metaclass=abc.ABCMeta):
  """Wrapper for accessing metadata from pandas dataset."""

  def __init__(self, metadata: Any):
    self.slide_metadata = metadata

  @abc.abstractmethod
  def find_data_frame_column_name(self, searchtxt: str) -> Optional[str]:
    """Returns name of column in pandas DF.

    Args:
      searchtxt: Text to look for across dataframe columns.

    Returns:
      column name (str)
    """

  def has_column_name(self, metadata_column_name: str) -> bool:
    """Returns true if metadata defines column name.

    Args:
      metadata_column_name: Column name to search for.

    Returns:
      True if column is defined.
    """
    return self.find_data_frame_column_name(metadata_column_name) is not None

  @abc.abstractmethod
  def lookup_metadata_value(self, metadata_column_name: str) -> Optional[Any]:
    """Look up column name in and return associated value.

    Args:
      metadata_column_name: column name to return value for

    Returns:
      Value stored in table column.
    """

  @property
  def column_names(self) -> List[str]:
    return ['']


def norm_column_name(column_name: str) -> str:
  """Normalize column name to remove effects of case and spacing.

  Args:
    column_name: Str name of column.

  Returns:
    Uppercase name with spaces removed.
  """
  return column_name.strip().replace(' ', '').replace('_', '').upper()


def find_data_frame_column_name(
    df: pandas.DataFrame, searchtxt: str
) -> Optional[str]:
  """Returns name of column in pandas DF.

  Args:
    df: Pandas Dataframe.
    searchtxt: Text to look for across dataframe columns.

  Returns:
    Pandas column name (str)
  """
  searchtxt = norm_column_name(searchtxt)
  for column_text in df.columns:
    if norm_column_name(column_text) == searchtxt:
      return column_text
  return None


class PandasMetadataTableWrapper(MetadataTableWrapper):
  """Wrapper for accessing metadata from pandas dataset."""

  def __init__(self, metadata: pandas.DataFrame):
    super().__init__(metadata)

  def find_data_frame_column_name(self, searchtxt: str) -> Optional[str]:
    """Returns name of column in pandas DF.

    Args:
      searchtxt: text to look for across dataframe columns.

    Returns:
      column name (str)
    """
    return find_data_frame_column_name(self.slide_metadata, searchtxt)

  def lookup_metadata_value(self, metadata_column_name: str) -> Optional[Any]:
    """Look up column name in and return associated value.

    Args:
      metadata_column_name: column name to return value for

    Returns:
      Value stored in table column.
    """
    column = self.find_data_frame_column_name(metadata_column_name)
    if column is None:
      return None
    value = self.slide_metadata.iloc[0][column]
    if pandas.isna(value):
      # if no value in CSV cell
      return None
    return value

  @property
  def column_names(self) -> List[str]:
    return self.slide_metadata.columns


class DictMetadataTableWrapper(MetadataTableWrapper):
  """Wrapper for accessing metadata from pandas dataset."""

  def __init__(self, metadata: Mapping[str, Any]):
    super().__init__(metadata)

  def copy(self) -> DictMetadataTableWrapper:
    return DictMetadataTableWrapper(dict(self.slide_metadata))

  def find_data_frame_column_name(self, searchtxt: str) -> Optional[str]:
    """Returns name of column in pandas DF.

    Args:
      searchtxt: text to look for across dataframe columns.

    Returns:
      column name (str)
    """
    searchtxt = norm_column_name(searchtxt)
    for column_text in self.slide_metadata:
      if norm_column_name(column_text) == searchtxt:
        return column_text
    return None

  def lookup_metadata_value(self, metadata_column_name: str) -> Optional[Any]:
    """Look up column name in and return associated value.

    Args:
      metadata_column_name: column name to return value for

    Returns:
      Value stored in table column.
    """
    column = self.find_data_frame_column_name(metadata_column_name)
    if column is None:
      return None
    return copy.deepcopy(self.slide_metadata.get(column))

  @property
  def column_names(self) -> List[str]:
    return list(self.slide_metadata)


class SchemaDICOMTag:
  """Represents instances of dicom tags during dicom json generation."""

  def __init__(
      self,
      address: str,
      vr_types: Set[str],
      tag_schema: Dict[str, Any],
      table_req: Set[str],
      dicom_ds: Optional[SchemaDicomDatasetBlock],
  ):
    self._container_ds = dicom_ds
    self._schema_def = tag_schema

    self._schema_key_mapping = {}
    for key in self._schema_def.keys():
      ukey = key.upper()
      if ukey != key:
        self._schema_key_mapping[ukey] = key

    self._schema_tag_value_source = None
    self._address = address
    self._vr = vr_types
    self._value = None
    self._meta_join = None
    self._metadata_columns_with_values = set()

    self._value_char_limit = ''
    self._conditional_on = set()
    self._parent_tag = None
    if self._container_ds:
      self._parent_tag = self._container_ds.parent

    self._write_uninitialized = False
    self._tag_value_definition_required = False
    self._table_req = table_req
    self._validate_and_init_schema_dicom_tag()

  @property
  def metadata_columns_with_values(self) -> Set[str]:
    """Returns list of metadata column names which were used to set tags val."""
    return self._metadata_columns_with_values

  def add_metadata_column_with_value(self, metadata_col_name: str):
    """Adds metadata column name to list of columns which set tag val."""
    self._metadata_columns_with_values.add(metadata_col_name)

  def set_value_from_metadata_list(self, val_list: List[Any]) -> bool:
    """Sets tag values from a list of values.

    Args:
      val_list: list of one or more values to set tag to.

    Returns:
      True if tag value set.

    Raises:
      DICOMSchemaError: if joining values for tag with non character vr type.
    """
    if not val_list:
      return False
    if len(val_list) == 1:
      if self.is_value_char_len_valid(str(val_list[0]), self.vr):
        self.value = val_list[0]
        return True
      return False
    if not dicom_standard.dicom_standard_util().is_vr_str_type(self.vr):
      raise DICOMSchemaError('Cannot join values for tag of non-string vr type')
    if self._meta_join is None:
      join_str = ''
    else:
      join_str = self._meta_join
    test_val = join_str.join([str(val) for val in val_list])
    if self.is_value_char_len_valid(test_val, self.vr):
      self.value = test_val
      return True
    return False

  def __str__(self) -> str:
    """Returns string representation of SchemaDICOMTag."""

    lines = [f'Schema Tag: ({self.address})']
    lines.append(f'  Address path: {self.path()}')
    lines.append(f'  Keyword path: {self.keyword_path()}')
    lines.append(f'  vr: {self.vr}')
    lines.append(f'  type: {self._table_req}')
    lines.append('  schema:')
    for key, value in self._schema_def.items():
      if not isinstance(value, list):
        lines.append(f'    {key}: {value}')
      else:
        lines.append(f'    {key}: [count: {len(value)}]')
    return '\n'.join(lines)

  def get_sq_index(self, block: SchemaDicomDatasetBlock) -> Optional[int]:
    """Gets index of a specific SchemaDicomDatasetBlock in sequence.

       method used to determine sequence [index] of blocks in a keyword or
       address path, .e.g sq[2]

    Args:
      block: instance of block to return index of

    Returns:
      index of bock in sequence.

    Raises:
      IndexError: cannot get index of non SQ tags.
    """
    if not self.is_tag_schema_define_seq():
      raise IndexError(f'Cannot get index of non sq tags.\n{self}')
    if self._value is None:
      return None
    try:
      return self._value.index(block)
    except ValueError:
      # if not found then assume it is in the block is in process of being
      # added to end of the sequence.
      return len(self._value)

  @property
  def container_dataset(self) -> Optional[SchemaDicomDatasetBlock]:
    """Returns DICOM Container Dataset holding this tag."""
    return self._container_ds

  def path(self, strip_sq_index: bool = False) -> List[str]:
    """Returns DICOM Schema address path to tag.

    Args:
      strip_sq_index: if true strips sequence index from path. Strip for path
        based queries1 to dicom_iod_util  Returns Dicom Address Path
    """
    if self._container_ds is None:
      return [self._address]
    else:
      return self._container_ds.path(strip_sq_index=strip_sq_index) + [
          self._address
      ]

  def keyword_path(self, strip_sq_index: bool = False) -> List[str]:
    """Returns DICOM Schema keyword path to tag.

    Args:
      strip_sq_index: if true strips sequence index from path. Strip for path
        based queries to dicom_iod_util  Returns Dicom Keyword Path

    Raises:
      DICOMSchemaTagError: address of tag not found in DICOM Spec
    """
    tag = dicom_standard.dicom_standard_util().get_tag(self._address)
    if tag is None:
      raise DICOMSchemaTagError(
          self, f'Tag address: {self._address} not found in DICOM Spec.'
      )
    keyword = tag.keyword
    if self._container_ds is None:
      return [keyword]
    else:
      return self._container_ds.keyword_path(strip_sq_index=strip_sq_index) + [
          keyword
      ]

  @property
  def address(self) -> str:
    """Returns address of tag in schema."""
    return self._address

  @property
  def parent(self) -> Optional[SchemaDICOMTag]:
    """Returns tag of parent to this tag [either None or a SQ tag]."""
    return self._parent_tag

  @property
  def write_uninitialized(self) -> bool:
    """Returns true if tag should be written out in json as a valueless entry.

    write_uninitialized, defaults to True for type all 2 tags.
    otherwise defaults to False.
    """
    return self._write_uninitialized

  @property
  def tag_value_definition_required(self) -> bool:
    """True if missing tag value exception should be raised if missing value."""
    return self._tag_value_definition_required

  @property
  def tag_value_source(self) -> str:
    """Returns schema key defining tag value e.g."META", "STATIC_VALUE, or "SQ"."""
    return self._schema_tag_value_source

  def is_tag_schema_define_seq(self) -> bool:
    """Returns True if schema value initializes a SQ."""
    return self.tag_value_source in (
        _SchemaTagKeywords.SQ,
        _SchemaTagKeywords.SEQ,
    )

  @property
  def seq_metadata(self) -> Optional[List[Mapping[str, Any]]]:
    """Returns schema metadata that inits the elements of a SQ."""
    if not self.is_tag_schema_define_seq():
      return None
    val = self.get_schema_key_val(self.tag_value_source)
    if isinstance(val, Mapping):  # Enable SQ of 1 item to be declared.
      return [val]
    return val

  def is_tag_value_set_from_metadata(self) -> bool:
    """Returns True if schema value initializes from metadata."""
    return self.tag_value_source == _SchemaTagKeywords.META

  def is_tag_value_set_from_static_value(self) -> bool:
    """Returns True if schema value initializes from a static value."""
    return self.tag_value_source == _SchemaTagKeywords.STATIC_VALUE

  @property
  def metadata_column_name(self) -> Optional[Union[str, List[str]]]:
    """Returns metadata column name conting value."""
    if not self.is_tag_value_set_from_metadata():
      return None
    meta_list = self.get_schema_key_val(_SchemaTagKeywords.META)
    if isinstance(meta_list, str):
      meta_list = [meta_list]
    if not isinstance(meta_list, list):
      raise DICOMSchemaTagError(
          self, 'Invalid meta value. Must be a string or [string].'
      )
    col_name_list = []
    for val in meta_list:
      if not isinstance(val, str):
        raise DICOMSchemaTagError(
            self, 'Invalid meta value. Must be a string or [string].'
        )
      col_name = norm_column_name(val)
      if col_name:
        col_name_list.append(col_name)
    if col_name_list:
      return col_name_list
    return None

  @property
  def static_value(self) -> Optional[str]:
    """Returns static value for tag."""
    if not self.is_tag_value_set_from_static_value():
      return None
    return self.get_schema_key_val(_SchemaTagKeywords.STATIC_VALUE)

  @property
  def vr(self) -> Set[str]:
    """Return set of valid vr types for tag."""
    return self._vr

  @property
  def value(self) -> Any:
    """Value tag initialized to."""
    return self._value

  @classmethod
  def _get_byte_char_count(cls, val: str, max_bytes: int) -> int:
    """Determines the char position to crop # bytes in utf-8 string.

    Args:
      val: string to crop
      max_bytes: max bytes to encode in string

    Returns:
      Character position to crop at
    """
    size = 0
    for index, ch in enumerate(val):
      size += len(ch.encode('utf-8'))
      if size > max_bytes:
        return index
    return len(val)

  def _test_tag_value_is_formatted_correctly_for_vr_type(
      self, val: Any
  ) -> None:
    """Test DICOM Tag value is formatted as required by DICOM Standard.

    Args:
      val: Value of DICOM tag.

    Raises:
     DICOMSchemaTagError: Tag value formatting violates DICOM Standard.
    """
    uid_validation = ingest_flags.METADATA_UID_VALIDATION_FLG.value
    if (
        dicom_standard.dicom_standard_util().is_vr_ui_type(self.vr)
        and uid_validation != ingest_flags.MetadataUidValidation.NONE
        and isinstance(val, str)
        and not uid_generator.validate_uid_format(val)
    ):
      msg = 'Metadata defines incorrectly formatted DICOM UID.'
      log = {'tag': str(self), 'value': val}
      if uid_validation == ingest_flags.MetadataUidValidation.LOG_WARNING:
        cloud_logging_client.warning(msg, log)
        return
      cloud_logging_client.error(msg, log)
      raise DICOMSchemaTagError(
          self,
          'DICOMTag has UI VR code, tag value does not UI VR code formatting'
          ' requirements.',
      )

  def crop_str_to_vr_type(self, val: Any) -> Any:
    """Crops string values to VR type max character count definitions.

    Args:
      val: value to crop

    Returns:
      Value either unchangned or cropped.
    Raises:
     DICOMSchemaTagError: Length of tag value violates DICOM Standard VR code
       requirements.
    """
    if not isinstance(
        val, str
    ) or not dicom_standard.dicom_standard_util().is_vr_str_type(self.vr):
      return val
    length_validation = ingest_flags.METADATA_TAG_LENGTH_VALIDATION_FLG.value
    if length_validation == ingest_flags.MetadataTagLengthValidation.NONE:
      return val
    max_str_len = [
        dicom_standard.dicom_standard_util().get_vr_max_chars(self.vr)
    ]
    max_bytes = dicom_standard.dicom_standard_util().get_vr_max_bytes(self.vr)
    if max_bytes > 0:
      max_str_len.append(SchemaDICOMTag._get_byte_char_count(val, max_bytes))
    max_str_len = max(max_str_len)
    # VR Codes: 'OB', 'OV', 'OL', 'OW' do not have defined lengths and return -1
    if max_str_len < 0 or len(val) <= max_str_len:
      return val
    if (
        length_validation
        == ingest_flags.MetadataTagLengthValidation.LOG_WARNING
    ):
      cloud_logging_client.warning(
          'DICOM tag value exceeds DICOM Standard length limits for tag VR'
          ' type.',
          {'tag_value': val, 'tag': str(self)},
      )
      return val
    elif (
        length_validation
        == ingest_flags.MetadataTagLengthValidation.LOG_WARNING_AND_CLIP
    ):
      cropped_val = val[:max_str_len]
      cloud_logging_client.warning(
          'DICOM tag value cropped; value exceeds DICOM Standard length limits'
          ' for tag VR type.',
          {
              'uncropped_tag_value': val,
              'cropped_tag_value': cropped_val,
              'tag': str(self),
          },
      )
      return cropped_val
    msg = (
        'DICOM tag value exceeds DICOM Standard length limits for tag VR type.'
    )
    cloud_logging_client.error(
        msg,
        {'tag_value': val, 'tag': str(self)},
    )
    raise DICOMSchemaTagError(self, msg)

  @value.setter
  def value(self, val: Any):
    self._test_tag_value_is_formatted_correctly_for_vr_type(val)
    self._value = self.crop_str_to_vr_type(val)

  def is_value_set(self) -> bool:
    """Returns true if tags value has been initialized."""
    if not self.is_tag_schema_define_seq():
      return self._value is not None
    for dataset in self._value:
      if dataset.is_any_value_set():
        return True
    return False

  @property
  def conditional_on(self) -> Set[str]:
    """Returns set of metadata which needs to be defined for tag to be init."""
    return self._conditional_on

  @property
  def is_seq(self) -> bool:
    """Returns True if dicom spec defines tag vr as SQ."""
    return _SchemaTagKeywords.SQ in self._vr

  @classmethod
  def get_address_vr_types(cls, address: str) -> Set[str]:
    """Return DICOM Spec VR type(s) for dicom tag.

    Args:
      address: DICOM Tag Address

    Returns:
      set of VR types defined for tag.

    Raises:
      DICOMSchemaError: if address not found in DICOM Spec
    """
    tag = dicom_standard.dicom_standard_util().get_tag(address)
    if tag is None:
      raise DICOMSchemaError(f'Tag address: {address} not found in DICOM Spec.')
    return tag.vr

  @classmethod
  def get_kw_address(cls, keyword: str) -> str:
    """Get Address associated with DICOM tag keyword.

    Args:
      keyword: dicom tag keyword.

    Returns:
      Tag address associated with dicom keyworld.

    Raises:
      DICOMSchemaError: If keyword not found in DICOM Spec
    """
    keyword = dicom_standard.DicomKeyword(keyword)
    address = dicom_standard.dicom_standard_util().get_keyword_address(keyword)
    if address is None:
      raise DICOMSchemaError(f'Keyword: {keyword} not found in DICOM Spec.')
    return dicomtag.DicomTag.standardize_address(address)

  @property
  def schema_def(self) -> Mapping[str, Any]:
    """Get schema definition associated with a tag instance."""
    return self._schema_def

  def get_schema_key_val(self, key: str) -> Any:
    """Return value associated with key in the schema."""
    if self._schema_key_mapping:
      m_key = self._schema_key_mapping.get(key.upper())
      if m_key is not None:
        return self._schema_def.get(m_key)
    return self._schema_def.get(key)

  @classmethod
  def _has_vals_not_in(cls, vals: Set[str], tst: List[str]) -> bool:
    """Returns true if set has values not in test list.

    Args:
      vals: set to test
      tst: test list

    Returns:
      True if values not in test list
    """
    for val in vals:
      if val not in tst:
        return True
    return False

  @classmethod
  def _has_vals_in(cls, vals: Set[str], tst: List[str]) -> bool:
    """Returns true if set has any value in test list.

    Args:
      vals: set to test
      tst: test list

    Returns:
      True if any value not in test list
    """

    for val in vals:
      if val in tst:
        return True
    return False

  @classmethod
  def _valid_value_char_limit(cls, value_limit: str) -> bool:
    """Tests if schema tag value limit text is formatted correctly.

    Args:
      value_limit: Value limite definition txt.

    Returns:
      True if text is formatted correctly.
    """
    for op in ['==', '<=', '>=', '=', '<', '>']:
      if value_limit.startswith(op):
        remainder = value_limit[len(op) :]
        if remainder:
          try:
            _ = int(remainder)
            return True
          except ValueError:
            pass
    return False

  def is_value_char_len_valid(
      self, test_value: str, vr: Set[str]
  ) -> Optional[bool]:
    """Tests if test_value meets tag value length requirements.

    Args:
      test_value: Value to test.
      vr: vr type of value

    Returns:
      True specified value is valid satisfies tags requirements.
    """
    if not self._value_char_limit:
      return True
    if not dicom_standard.dicom_standard_util().is_vr_str_type(vr):
      test_value = str(test_value)
      cloud_logging_client.warning(
          (
              'Performing VALUE_CHAR_LIMIT on non-string VR type may produce '
              'unexpected results. Casting value to string.'
          ),
          {'tag': str(self), 'cast_string_value': test_value, 'VR': str(vr)},
      )

    val_len = len(test_value)
    for op in ['==', '<=', '>=', '=', '<', '>']:
      if not self._value_char_limit.startswith(op):
        continue
      remainder = self._value_char_limit[len(op) :]
      if not remainder:
        continue
      try:
        limit = int(remainder)
      except ValueError:
        continue
      if op == '==' or op == '=':
        return val_len == limit
      if op == '<=':
        return val_len <= limit
      if op == '>=':
        return val_len >= limit
      if op == '<':
        return val_len < limit
      if op == '>':
        return val_len > limit
    return False

  def _validate_and_init_schema_dicom_tag(self):
    """Validates DICOM tag schema definition and further inits tag.

    Raises:
      DICOMSchemaTagError: error in schema tag def
    """
    tagkw = self.get_schema_key_val(_SchemaTagKeywords.KEYWORD)
    try:
      if SchemaDICOMTag.get_kw_address(tagkw) != self._address:
        raise DICOMSchemaTagError(
            self,
            (
                'Schema tag keyword does not resolve to '
                f' specified address; keyword: {tagkw};'
                f' keyword: {tagkw} != {self._address}'
            ),
        )
    except DICOMSchemaError as exp:
      raise DICOMSchemaTagError(
          self,
          (
              'Schema tag keyword does not resolve to '
              f' address; keyword: {tagkw};\n'
              f'exception: {traceback.format_exc()}'
          ),
      ) from exp
    tag_param = []
    for test_param in [
        _SchemaTagKeywords.META,
        _SchemaTagKeywords.STATIC_VALUE,
        _SchemaTagKeywords.SQ,
        _SchemaTagKeywords.SEQ,
    ]:
      sq = self.get_schema_key_val(test_param)
      if sq is not None:
        tag_param.append(test_param)
    if not tag_param:
      raise DICOMSchemaTagError(
          self, 'Tag is missing expected value def: META, STATIC_VALUE, or SQ'
      )
    if len(tag_param) > 1:
      raise DICOMSchemaTagError(
          self,
          (
              'Tag definition has multiple [META, '
              'STATIC_VALUE, or SQ] value defs. '
              f'Tag defines: {str(tag_param)}'
          ),
      )
    self._schema_tag_value_source = tag_param[0]

    self._meta_join = self.get_schema_key_val(_SchemaTagKeywords.META_JOIN)
    if self._meta_join is not None and not isinstance(self._meta_join, str):
      raise DICOMSchemaTagError(
          self, 'Invalid meta_join value. Must be a string.'
      )

    value_limit = self.get_schema_key_val(_SchemaTagKeywords.VALUE_CHAR_LIMIT)
    if value_limit is not None:
      if isinstance(
          value_limit, str
      ) and SchemaDICOMTag._valid_value_char_limit(value_limit):
        self._value_char_limit = value_limit
      else:
        raise DICOMSchemaTagError(
            self, 'Invalid value_limit. Must be a valid string.'
        )

    conditional_on = self.get_schema_key_val(_SchemaTagKeywords.CONDITIONAL_ON)
    if conditional_on is not None:
      if isinstance(conditional_on, str):
        m_name = norm_column_name(conditional_on)
        self._conditional_on.add(m_name)
      else:
        for m_name in conditional_on:
          m_name = norm_column_name(m_name)
          self._conditional_on.add(m_name)
    write_uninit = self.get_schema_key_val(_SchemaTagKeywords.WRITE_EMPTY)
    if write_uninit is not None:
      write_uninit = norm_column_name(write_uninit)
      if write_uninit not in (
          _SchemaTagKeywords.TRUE,
          _SchemaTagKeywords.FALSE,
      ):
        raise DICOMSchemaTagError(
            self,
            (
                'WRITE_EMPTY expects "TRUE" or "FALSE"'
                f' values; WRITE_EMPTY: {write_uninit}'
            ),
        )
      self._write_uninitialized = write_uninit == _SchemaTagKeywords.TRUE

    required = self.get_schema_key_val(_SchemaTagKeywords.REQUIRED)
    if required is not None:
      required = norm_column_name(str(required))
      if required not in (_SchemaTagKeywords.TRUE, _SchemaTagKeywords.FALSE):
        raise DICOMSchemaTagError(
            self,
            (
                'REQUIRED expects "TRUE" or "FALSE"'
                f' values; WRITE_EMPTY: {required}'
            ),
        )
      self._tag_value_definition_required = required == _SchemaTagKeywords.TRUE
    else:
      self._tag_value_definition_required = (
          ingest_flags.REQUIRE_TYPE1_DICOM_TAG_METADATA_IS_DEFINED_FLG.value
          and SchemaDICOMTag._has_vals_in(self._table_req, [_DICOM_TYPE_1_TAG])
      )

    if SchemaDICOMTag._has_vals_in(self._table_req, [_DICOM_TYPE_2_TAG]):
      self._write_uninitialized = True

    if not self.is_seq and self.is_tag_schema_define_seq():
      raise DICOMSchemaTagError(self, 'Tags which are not SQ VR cannot be SQ.')
    elif self.is_seq and not self.is_tag_schema_define_seq():
      raise DICOMSchemaTagError(
          self, 'SQ VR type tags must be initialized as SQ'
      )
    if (
        self.is_tag_schema_define_seq()
        and not isinstance(self.seq_metadata, list)
        and not isinstance(self.seq_metadata, dict)
    ):
      raise DICOMSchemaTagError(
          self, '"SQ" should be set to a list of datasets.'
      )

  def add_tag_to_json(self, dataset: MutableMapping[str, Any]):
    """Output tag value to json.

    Args:
      dataset: Json Dict to write tag to.

    Raises:
      DICOMSchemaTagError: error in schema tag def
    """
    if len(self.vr) == 1:
      vr = list(self.vr)[0]
    else:
      schema_vr_type = self._schema_def.get('VR')
      if schema_vr_type is None:
        raise DICOMSchemaTagError(
            self,
            'Tag defines multiple VR types. Specify type. "VR": "<value>" ',
        )
      if schema_vr_type in self.vr:
        vr = schema_vr_type
      else:
        raise DICOMSchemaTagError(self, 'Specified VR type does not match tag.')
    data_element = {'vr': vr}
    if not self.is_seq:
      # if setting a tag value
      if self._value is not None:
        # if tag has a value set data element to write
        value = str(self._value)
        if dicom_standard.dicom_standard_util().is_vr_float_type(set([vr])):
          value = float(value)
        elif dicom_standard.dicom_standard_util().is_vr_int_type(set([vr])):
          value = int(value)
        if vr == 'PN':
          data_element['Value'] = [{'Alphabetic': value}]
        elif vr in ('OB', 'OD', 'OF', 'OW', 'UN'):
          data_element['InlineBinary'] = value
        elif vr == 'AT':
          data_element['Value'] = [dicomtag.DicomTag.standardize_address(value)]
        elif isinstance(value, list):
          data_element['Value'] = value
        else:
          data_element['Value'] = [value]
    else:
      # if setting a sequence initialize the sequence to write
      child_datasets = []
      for child in self._value:
        if child.is_any_value_set():
          dataset_json = child.get_dicom_json()
          child_datasets.append(dataset_json)
      if child_datasets:
        data_element['Value'] = child_datasets
    # set the output to either just a null initialized if tag vr type is 2
    # requiring null initialization.
    # null initialization = "address": { "vr": "TagValue"}
    # or initialized  = "address": { "vr": "TagValue", "Value": [ stuff]}
    if self.write_uninitialized or 'Value' in data_element:
      dataset[self._address] = data_element


class SchemaDicomDatasetBlock:
  """Represents block of DICOM tags (root or element in a sq)."""

  def __init__(
      self,
      dataset_schema: Mapping[str, Any],
      iod_name: str,
      parent_tag: Optional[SchemaDICOMTag] = None,
      json_base: Optional[Mapping[str, Any]] = None,
  ):
    self._json_base = json_base
    self._tags = {}
    self._is_any_value_set = None
    self._parent_tag = parent_tag
    try:
      iod_name = dicom_standard.IODName(iod_name)
      dcm_dataset = (
          dicom_standard.dicom_iod_dataset_util().get_iod_dicom_dataset(
              iod_name, self.path(strip_sq_index=True)
          )
      )
    except dicom_standard.DicomPathError as exp:
      raise DICOMSchemaError(
          f'DICOM path[{self.path}] not defined in'
          f' IOD({iod_name});\n'
          f'exception: {traceback.format_exc()}'
      ) from exp

    for address, schema_tag_def in dataset_schema.items():
      address = dicomtag.DicomTag.standardize_address(address)
      iod_tag = dcm_dataset.get(address)
      if iod_tag is None:
        raise DICOMSchemaError(
            f'DICOM tag[{address}] not defined in'
            f' IOD({iod_name}); At: {dcm_dataset}'
        )
      try:
        vr_types = SchemaDICOMTag.get_address_vr_types(address)
      except DICOMSchemaError as exp:
        tag_path = self.path() + [address]
        keyword_path = self.path() + [address]
        raise DICOMSchemaError(
            f'Schema tag:{tag_path} does not resolve '
            f' to IOD tag\nKeyword Path: {keyword_path}\n'
            f'exception: {traceback.format_exc()}'
        ) from exp
      tag = SchemaDICOMTag(
          address, vr_types, schema_tag_def, iod_tag.required, self
      )
      self._tags[address] = tag

  @property
  def tags(self) -> List[SchemaDICOMTag]:
    """Returns list of tags defined."""
    return list(self._tags.values())

  @property
  def parent(self) -> Optional[SchemaDICOMTag]:
    """Returns parent dicom tag containing SchemaDicomDatasetBlock instance."""
    return self._parent_tag

  def path(self, strip_sq_index: bool = False) -> List[str]:
    """Returns dicom tag address to dataset."""
    block = self
    parent = self._parent_tag
    parent_list = list()
    while parent is not None:
      if not strip_sq_index:
        index = parent.get_sq_index(block)
        if index is not None:
          parent_list.append(f'[{index}]')
      parent_list.append(parent.address)
      block = parent.container_dataset
      parent = parent.parent
    parent_list.reverse()
    return parent_list

  def keyword_path(self, strip_sq_index: bool = False) -> List[str]:
    """Returns DICOM Schema keyword path to dataset.

    Args:
      strip_sq_index: if true strips sequence index from path. Strip for path
        based queries to dicom_iod_util  Returns Dicom Keyword Path

    Raises:
      DICOMSchemaTagError: address of tag not found in DICOM Spec
    """
    path = []
    for address in self.path(strip_sq_index=strip_sq_index):
      if address.startswith('[') and address.endswith(']'):
        path.append(address)
      else:
        tag = dicom_standard.dicom_standard_util().get_tag(address)
        if tag is None:
          if self._parent_tag is None:
            raise DICOMSchemaTagError(
                None, f'Tag address: {address} not found in DICOM IOD root'
            )
          else:
            raise DICOMSchemaTagError(
                self._parent_tag,
                f'Tag address: {address} not found in DICOM ID',
            )
        path.append(tag.keyword)
    return path

  def is_any_value_set(self) -> bool:
    """Returns true if dataset contains any tags with set values."""
    if self._is_any_value_set is not None:
      return self._is_any_value_set
    for tag in self._tags.values():
      if tag.is_value_set():
        self._is_any_value_set = True
        break
    return self._is_any_value_set

  def get_dicom_json(self) -> Dict[str, Any]:
    """Returns JSON representation of initialized dataset."""
    json_dicom = {}
    if self._json_base is not None and self._json_base:
      for key, value in self._json_base.items():
        json_dicom[key] = copy.deepcopy(value)
    for tag in self._tags.values():
      is_value_set = tag.is_value_set()
      if not is_value_set and tag.tag_value_definition_required:
        raise MissingRequiredMetadataValueError(tag)
      if is_value_set or tag.write_uninitialized:
        tag.add_tag_to_json(json_dicom)
    return json_dicom


class ConditionalTagHandler:
  """Managed tracking defined conditions and tracking tags with undef values."""

  def __init__(self):
    self._metadata_written = set()
    self._conditional_tag = collections.defaultdict(set)

  def has_conditions(self, tag: SchemaDICOMTag) -> bool:
    """Removes met conditions from tag, returns true if tag has unmet cond.

    Args:
      tag: tag to process

    Returns:
      True if tag has unsatisfied condtions
    """
    conditional_tags = list(tag.conditional_on)
    for cond in conditional_tags:
      if cond in self._metadata_written:
        tag.conditional_on.remove(cond)
    return bool(tag.conditional_on)

  def add_tag_to_monitor(self, tag: SchemaDICOMTag):
    """Adds unmet tag conditions to conditional monitors.

    Args:
      tag: tag to process
    """
    for cond in tag.conditional_on:
      self._conditional_tag[cond].add(tag)

  def tag_processed(self, tag: SchemaDICOMTag) -> List[SchemaDICOMTag]:
    """records tag metadata def and returns list tags dependent on metadata.

    Args:
      tag: Tag defined.

    Returns:
      List of tags which no longer have remaining conditional dependencies.
    """
    return_list_of_tags_with_no_conditions = list()
    for column_name in tag.metadata_columns_with_values:
      if column_name is None:
        continue
      if column_name in self._metadata_written:
        continue
      self._metadata_written.add(column_name)
      tags_with_condition = self._conditional_tag.get(column_name)
      if tags_with_condition is None:
        continue
      for tag in tags_with_condition:
        tag.conditional_on.remove(column_name)
        if not tag.conditional_on:
          return_list_of_tags_with_no_conditions.append(tag)
      del self._conditional_tag[column_name]
    return return_list_of_tags_with_no_conditions


def get_json_dicom(
    iod_name: str,
    metadata: MetadataTableWrapper,
    dicom_schema: Mapping[str, Any],
    json_base: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
  """Generates JSON representation of DICOM from metadata and schema.

  Args:
    iod_name: string name of IOD to generate
    metadata: metadata
    dicom_schema: schema def to map metadata to DICOM
    json_base: Optional JSON dict to build schema on

  Returns:
    python dict representation of DICOM JSON.

  Raises:
      DICOMSchemaTagError: error in schema tag def
      DICOMSchemaError: error in schema
                        Correct error in schema definition.
      DICOMSpecMetadataError: error in dicom spec metadata
                              (Fix requires fixing metadata def.)

      Catch exceptions at higher level. Fixes for these require fixing schema
      or rebuilding/fixing dicom schema JSON def.
  """
  conditiona_tag_handler = ConditionalTagHandler()
  cloud_logging_client.info('Starting generating dicom json from schema')
  root = SchemaDicomDatasetBlock(dicom_schema, iod_name, json_base=json_base)
  tag_processing_stack = list(root.tags)
  while tag_processing_stack:
    tag = tag_processing_stack.pop()
    if conditiona_tag_handler.has_conditions(tag):
      conditiona_tag_handler.add_tag_to_monitor(tag)
    else:
      if tag.is_tag_value_set_from_metadata():
        col_name_list = tag.metadata_column_name
        if isinstance(col_name_list, str):
          col_name_list = [col_name_list]
        metadata_value_list = []
        for col_name in col_name_list:
          if not metadata.has_column_name(col_name):
            # Enable a newer schema to be used with older CSV data.
            # Omit missing value and log warning
            cloud_logging_client.warning(
                (
                    'Invalid metadata column name in DICOM schema. '
                    'DICOM tag may not be assigned a value.'
                ),
                {
                    'tag': str(tag),
                    'schema_column_name': col_name,
                    'csv_columns': metadata.column_names,
                },
            )
            continue
          meta_value = metadata.lookup_metadata_value(col_name)
          if meta_value is not None:
            tag.add_metadata_column_with_value(col_name)
            metadata_value_list.append(meta_value)
        if tag.set_value_from_metadata_list(metadata_value_list):
          tag_processing_stack.extend(conditiona_tag_handler.tag_processed(tag))
      elif tag.is_tag_value_set_from_static_value():
        if not tag.is_value_char_len_valid(str(tag.static_value), tag.vr):
          continue
        tag.value = tag.static_value
        tag_processing_stack.extend(conditiona_tag_handler.tag_processed(tag))
      elif tag.is_tag_schema_define_seq():
        child_dataset_tags = []
        tag_block_list = []
        tag.value = tag_block_list
        for child_dataset_schema in tag.seq_metadata:
          if not isinstance(child_dataset_schema, dict):
            raise DICOMSchemaTagError(tag, 'Sequence should contain dict')
          block = SchemaDicomDatasetBlock(
              child_dataset_schema, iod_name, parent_tag=tag
          )
          child_dataset_tags += list(block.tags)
          tag_block_list.append(block)
        tag_processing_stack.extend(child_dataset_tags)
        tag_processing_stack.extend(conditiona_tag_handler.tag_processed(tag))
  cloud_logging_client.info('Done generating dicom json')
  return root.get_dicom_json()
