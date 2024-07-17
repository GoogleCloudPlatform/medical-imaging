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
"""Utility class decodes and work with DICOM spec generated json.

DICOM standard is represented as XML.
  Definitions of the DICOM document model (IOD) and
  Dicom tags have been extracted from the xml and saved in a JSON.

  This class loads the generated json into python data classes to provide
  methods access to the json representation.

Main class: DicomStandardIODUtil
"""
import dataclasses
import json
import os
import re
from typing import Any, List, Mapping, NewType, Optional, Sequence, Set, Union

IODName = NewType('IODName', str)
ModuleName = NewType('ModuleName', str)
ModuleRef = NewType('ModuleRef', str)
ModuleUsage = NewType('ModuleUsage', str)
TableName = NewType('TableName', str)
DicomTagAddress = NewType('DicomTagAddress', str)
DicomKeyword = NewType('DicomKeyword', str)
DicomVRCode = NewType('DicomVRCode', str)
DicomVMCode = NewType('DicomVMCode', str)
DicomTableTagRequirement = NewType('DicomTableTagRequirement', str)

DicomPathEntry = Union[DicomTagAddress, DicomKeyword, str]
DicomPathType = List[DicomPathEntry]

_RE_IS_UID_IOD = re.compile(r'[0-9\.]+')


@dataclasses.dataclass
class InlineTableLine:
  """class representation of InlineTableLine json(see, dicom_tables.json)."""

  name: str
  address: DicomTagAddress
  required: DicomTableTagRequirement
  comment: str

  def __str__(self):
    return (
        f'InlineTableLine(name: {self.name}, address: '
        f'{self.address}, required: {self.required}, '
        f'comment: {self.comment})'
    )


@dataclasses.dataclass
class LinkedTableLine:
  """class representation of LinkedTableLine json(see, dicom_tables.json)."""

  prefix: str
  linked_table: str
  usage: str

  @property
  def table_name(self) -> TableName:
    """Returns name of linked table."""
    return TableName(self.linked_table[len('table_') :])

  def __str__(self):
    return (
        f'LinkedTableLine(prefix: {self.prefix}, linked_table: '
        f'{self.linked_table}, usage: {self.usage})'
    )


TableLine = Union[LinkedTableLine, InlineTableLine]


@dataclasses.dataclass
class Table:
  """class representation of table json(see, dicom_tables.json)."""

  table_name: TableName
  table_caption: str
  table_lines: List[TableLine]


@dataclasses.dataclass
class ModuleSection:
  """class representation of module section json(see, dicom_modules.json)."""

  module_name: ModuleName
  table_name: TableName
  caption: str


@dataclasses.dataclass
class ModuleDef:
  """class representation of iod modules json(see, dicom_iod.json)."""

  name: ModuleName
  ref: ModuleRef
  usage: ModuleUsage

  @property
  def module_section_table_name(self) -> Optional[str]:
    if self.ref.startswith('sect_'):
      return self.ref[len('sect_') :]
    return None


@dataclasses.dataclass
class DcmTagDef:
  """class representation of dicom tags json(see, dicom_tag.json)."""

  address: DicomTagAddress
  keyword: DicomKeyword
  vr: Set[DicomVRCode]
  vm: DicomVMCode
  retired: str
  description: str


class DICOMSpecMetadataError(Exception):
  pass


def _module_name_search(name: str) -> str:
  return name.replace(' ', '').strip()


class DicomStandardIODUtil(object):
  """Utility class decodes and work with DICOM spec generated json.

  XML version of DICOM is paresed using other tools and used to generate
  referenced JSON metadata files.

  Dicom Information model:
    IOD(Document model) - set of modules [required, optional, conditional]
                modules - set of tables
                tables (nested and linked (possibly circular)) - map to other
                tables and tags.  Tables define tag requirements
                (required defined : 1
                 conditinally required defined : 1c
                 required nullable : 2
                 conditinally required nullable : 2c
                 optional: 3)

                Dicom Tags : container for data (may be array)
                  Dicom Tags : address : (hex string) unique identifier
                               keyword : (human string) unique identifier
                               vr code : representation type of data
                               vm      : value multiplicity


  * Note same tag may be defined by multiple modules.  Tag uniqueness is
    its address x position in document model.

  JSON metadata describes.  DICOM document modules (IODS) and DICOM tags.
    dicom_iod.json : DICOM IODs and modules described in each.
    dicom_iod_class_uid.json : IOD Name to UID mapping
    dicom_modules.json : root tables defined by a module
    dicom_tables.json : list of tags and linked tables defined in a table.
    dicom_tag.json : dicom tag definitions.
  """

  def __init__(self, json_dir: Optional[str] = None):
    self._version = None
    self._header_test_result = None
    self._tags_keywords = None

    # Read json data files.
    self._iod_uid = DicomStandardIODUtil._read_json(
        'dicom_iod_class_uid.json', json_dir
    )
    self._iod_to_module_map = DicomStandardIODUtil._read_json(
        'dicom_iod_module_map.json', json_dir
    )
    self._iod_modules = DicomStandardIODUtil._read_json(
        'dicom_iod.json', json_dir
    )
    self._modules = DicomStandardIODUtil._read_json(
        'dicom_modules.json', json_dir
    )
    self._table = DicomStandardIODUtil._read_json(
        'dicom_tables_part_1.json', json_dir
    )
    self._table2 = DicomStandardIODUtil._read_json(
        'dicom_tables_part_2.json', json_dir
    )
    self._table3 = DicomStandardIODUtil._read_json(
        'dicom_tables_part_3.json', json_dir
    )
    self._tags = DicomStandardIODUtil._read_json('dicom_tags.json', json_dir)
    self._iod_functional_group_modules = DicomStandardIODUtil._read_json(
        'dicom_iod_func_groups.json', json_dir
    )

    self._header_test_result = self._check_header_versions()
    if not self._header_test_result:
      raise DICOMSpecMetadataError(
          'Dicom json metadata was generated from '
          ' different versions of the specification. Json '
          'metadata should be regenerated.'
      )
    self._version = self._iod_modules['header']['dicom_standard_version']
    self._iod_modules = self._iod_modules['dicom_iod']
    self._modules = self._modules['dicom_modules']
    self._table = self._table['dicom_tables_part_1']
    self._table2 = self._table2['dicom_tables_part_2']
    self._table3 = self._table3['dicom_tables_part_3']
    self._iod_functional_group_modules = self._iod_functional_group_modules[
        'dicom_iod_func_groups'
    ]

    for key, value in self._table2.items():
      self._table[key] = value
    del self._table2
    for key, value in self._table3.items():
      self._table[key] = value
    del self._table3
    self._iod_uid = self._iod_uid['dicom_iod_class_uid']
    self._tags = self._tags['dicom_tags']

  def _build_keyword_dict(self):
    """Builds a tag dictionary to map keywords to dicom tag addresses.

    Raises:
      DICOMSpecMetadataError: Error in spec key address mapping
    """
    self._tags_keywords = {}
    for tag_block in ['main', 'mask']:
      for address, tag in self._tags[tag_block].items():
        keyword = tag['keyword'].strip()
        if keyword:
          if (
              keyword in self._tags_keywords
              and self._tags_keywords[keyword] != address
          ):
            raise DICOMSpecMetadataError(
                f'Duplicate tag keyword;keyword: {keyword}'
            )
          if keyword.startswith('0x'):
            raise DICOMSpecMetadataError(
                f'Keywords and address are not unique; keyword: {keyword}'
            )
          self._tags_keywords[keyword] = address

  def get_keyword_address(
      self, keyword: Union[DicomKeyword, str], striphex: bool = False
  ) -> Optional[DicomTagAddress]:
    """Converts DICOM tag keyword into address string.

    Args:
      keyword: Dicom tag keyword (str).
      striphex: if true strip leading '0x' from returned addresses.

    Returns:
      DICOM tag hex address as a string

    Raises:
      DICOMSpecMetadataError: Error in spec key address mapping
    """
    if self._tags_keywords is None:
      self._build_keyword_dict()
    address = self._tags_keywords.get(keyword)
    if address is not None and striphex and address.startswith('0x'):
      address = address[2:]
    return address  # pytype: disable=bad-return-type

  @classmethod
  def _read_json(
      cls, filename: str, json_dir: Optional[str] = None
  ) -> Mapping[str, Any]:
    """Reads json file.

    Args:
      filename: Filename to read.
      json_dir: Optional file path to json files.

    Returns:
      dicom dict
    """
    if json_dir is None:
      dir_name, _ = os.path.split(__file__)
      path = os.path.join(dir_name, 'spec', filename)
    else:
      path = os.path.join(json_dir, filename)
    with open(path, 'rt') as infile:
      return json.load(infile)

  def _check_header_versions(self) -> bool:
    """Validates that json files were built using same version of DICOM spec.

    Returns:
      True if json versions match
    """
    if self._header_test_result is not None:
      return self._header_test_result
    version_set = set()
    for tstobj in [
        self._iod_modules,
        self._modules,
        self._table,
        self._tags,
        self._iod_uid,
        self._table2,
        self._table3,
        self._iod_functional_group_modules,
    ]:
      version_set.add(tstobj['header']['dicom_standard_version']['Version'])
    if len(version_set) != 1:
      return False
    return True

  def list_dicom_iods(self) -> List[IODName]:
    """Returns a list of DICOM IODs."""
    return [IODName(iod_name) for iod_name in sorted(list(self._iod_uid))]

  def get_table(self, table_name: TableName) -> Table:
    """Returns named tables JSON definition.

    Args:
      table_name: Name of DICOM table to return.

    Returns:
      DICOM table definition.
    """
    tbl = self._table[table_name]
    table_lines = []
    for line in tbl['table_lines']:
      if line['type'] == 'InlineObject':
        if line['address'] == '0x':  # skip table without addresses
          continue
        table_lines.append(
            InlineTableLine(
                line['name'], line['address'], line['required'], line['comment']
            )
        )
      elif line['type'] == 'LinkedObject':
        table_lines.append(
            LinkedTableLine(
                line['prefix'], line['linked_resource'], line['usage']
            )
        )
    return Table(tbl['table_name'], tbl['table_caption'], table_lines)

  def get_module_section_tables(self, name: ModuleName) -> List[ModuleSection]:
    """Returns sections defined within a module.

    Args:
      name: DICOM module name to return sections for.

    Returns:
      List of Module sections
    """
    return [
        ModuleSection(name, item['name'], item['caption'])
        for item in self._modules[name]
    ]

  def get_iod_modules(
      self, iod_name: IODName, require_modules: Optional[Sequence[str]] = None
  ) -> List[ModuleDef]:
    """Returns list of modules defined in an IOD.

    Args:
      iod_name: Name of DICOM IOD to return modules (str).
      require_modules: Require listed IOD modules to be included regardless of
        Module IOD requirement level; e.g., treat listed modules with C or U
        usage, requirement as having being mandatory.

    Returns:
      List of module definitions

    Raises:
      DICOMSpecMetadataError: DICOM IOD name not found.
    """
    if require_modules is None:
      require_modules = set()
    else:
      require_modules = {_module_name_search(name) for name in require_modules}
    module_list = self._iod_modules.get(self._get_iod_primary_module(iod_name))
    if module_list is None:
      raise DICOMSpecMetadataError(f'IOD: {iod_name} not found.')
    return_module_list = []
    for module in module_list:
      name = module['name']
      ref = module['ref']
      usage = module['usage']
      if usage != 'M' and _module_name_search(name) in require_modules:
        usage = 'M'
      return_module_list.append(ModuleDef(name, ref, usage))
    return return_module_list

  def get_iod_functional_group_modules(
      self, iod_name: IODName
  ) -> List[ModuleDef]:
    """Returns list of modules defined in an IOD functional group.

    Args:
      iod_name: Name of DICOM IOD to return modules (str).

    Returns:
      List of module definitions

    Raises:
      DICOMSpecMetadataError: DICOM IOD name not found or functional group not
        found.
    """
    module_list = self._iod_functional_group_modules.get(
        self._get_iod_primary_module(iod_name)
    )
    if module_list is None:
      raise DICOMSpecMetadataError(f'IOD: {iod_name} not found.')
    return [
        ModuleDef(module['name'], module['ref'], module['usage'])
        for module in module_list
    ]

  def has_iod_functional_groups(self, iod_name: IODName) -> bool:
    """Returns list of functional group names defined in an IOD.

    Args:
      iod_name: Name of DICOM IOD to return modules (str).

    Returns:
      True if DICOM IOD contains functional group definition.

    Raises:
      DICOMSpecMetadataError: DICOM IOD name not found.
    """
    module_list = self._iod_functional_group_modules.get(
        self._get_iod_primary_module(iod_name)
    )
    return module_list is not None and module_list

  def get_iod_module_names(self, iod_name: IODName) -> List[ModuleName]:
    """Returns list of modules names defined by an IOD.

    Args:
      iod_name: Name of DICOM IOD.

    Raises:
      DICOMSpecMetadataError: DICOM IOD name not found.
    """
    return [module.name for module in self.get_iod_modules(iod_name)]

  def get_iod_module_usage(
      self, iod_name: IODName, module: ModuleName, full_text: bool = False
  ) -> Optional[str]:
    """Returns character which defines modules requirements in IOD.

       M : Mandatory
       C : Conditionally Mandatory
       U : Optional

    Args:
      iod_name: Name of DICOM IOD.
      module: Name of DICOM module in DICOM IOD.
      full_text: Return full text of usage def.

    Returns:
      Character indicating module usage reuirements.

    Raises:
      DICOMSpecMetadataError : iod name not found
    """
    for test_module in self.get_iod_modules(iod_name):
      if test_module.name == module:
        usage = test_module.usage
        if full_text:
          return usage
        return usage[0].upper()

  def get_iod_module_section(
      self, iod_name: IODName, module: Optional[ModuleName] = None
  ) -> Optional[Union[ModuleRef, List[ModuleRef]]]:
    """Returns list of modules definitions defined in IOD.

    Args:
      iod_name: Name of DICOM IOD to return modules for.
      module: Optional name of module to selectively return ref for.

    Returns:
      modele ref or list of module references.

    Raises:
      DICOMSpecMetadataError: DICOM IOD name not found.
    """
    if module is None:
      return [module.ref for module in self.get_iod_modules(iod_name)]
    for module in self.get_iod_modules(iod_name):
      if module.name == module:
        return module.ref

  def is_repeated_group_element_tag(self, tag_address: str) -> bool:
    """Returns True if tag address defines a repeated group element tag address.

    https://dicom.nema.org/dicom/2013/output/chtml/part05/sect_7.6.html

    For tag definitions see:
    /transformation_pipeline/ingestion_lib/dicom_util/spec/dicom_tags.json

    Args:
      tag_address: DICOM tag address as string (hex formatted).

    Returns:
      True if tag address defines repeated group element.
    """
    if tag_address.startswith('0x'):
      tag_address = tag_address[2:]  # masked tags do not start with 0x
    return self._tags['mask'].get(tag_address, None) is not None

  def get_tag(self, tag_address: Union[int, str]) -> Optional[DcmTagDef]:
    """Returns DICOM tag definition for tag address.

    Args:
      tag_address: DICOM tag address (int or hex string).

    Returns:
      DICOM tag definition.
    """
    if isinstance(tag_address, str) and not tag_address.startswith('0x'):
      tag_address = f'0x{tag_address}'
    if isinstance(tag_address, int):
      tag_address = f'0x{tag_address:0>8x}'
    tag = self._tags['main'].get(tag_address, None)
    if tag is None:
      tag_address = tag_address[2:]
      tag = self._tags['mask'].get(tag_address, None)  # masked tags do not
    if tag is None:  # start with 0x
      return None
    return DcmTagDef(
        DicomTagAddress(tag_address),
        tag['keyword'],
        set(tag['vr'].split(' or ')),
        tag['vm'],
        tag['retired'],
        tag['name'],
    )

  def get_tag_vr(
      self, tag_address: Union[int, str]
  ) -> Optional[Set[DicomVRCode]]:
    """Returns DICOM types which a tag defines.

       see: http://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html
       for VR definitions.

       Tags typically are defined by 1 type. A few are defined multiple valid
       types.

    Args:
      tag_address: DICOM tag address.

    Returns:
       Set of valid types(VR code strings).
    """
    tg = self.get_tag(tag_address)
    if tg is None:
      return None
    return tg.vr

  _INT_TYPE_VR_SET = frozenset(['SL', 'SS', 'UL', 'US'])

  @classmethod
  def is_vr_int_type(cls, vr_type: Set[Union[DicomVRCode, str]]) -> bool:
    """Returns True if vr type is a int type."""
    return bool(DicomStandardIODUtil._INT_TYPE_VR_SET & vr_type)

  _FLOAT_TYPE_VR_SET = frozenset(['FL', 'FD'])

  @classmethod
  def is_vr_float_type(cls, vr_type: Set[Union[DicomVRCode, str]]) -> bool:
    """Returns True if vr type is a float type."""
    return bool(DicomStandardIODUtil._FLOAT_TYPE_VR_SET & vr_type)

  _STRING_TYPE_VR_SET = frozenset([
      'AE',
      'AS',
      'CS',
      'DA',
      'DS',
      'DT',
      'IS',
      'LO',
      'LT',
      'OB',
      'OD',
      'OF',
      'OF',
      'OL',
      'OW',
      'PN',
      'SH',
      'ST',
      'TM',
      'UI',
      'UN',
      'UT',
      'UC',
      'UR',
  ])

  def get_normalized_tag_str(
      self, vr_types: Set[str], value: Union[int, str, float]
  ) -> str:
    """Normalize tag value string representation based on VR type.

       Use to fix issue where values, especially floats, in a string
       representation could define same value but have different str values.

    Args:
      vr_types: VR type of value
      value: Tag value to normalize as string.

    Returns:
      Normalized tag string value.
    """
    if self.is_vr_float_type(vr_types):
      return str(float(value))
    elif self.is_vr_int_type(vr_types):
      return str(int(value))
    else:
      return str(value)

  _OTHER_TYPE_VR_SET = frozenset(['AT', 'SQ'])

  @classmethod
  def is_recognized_vr(cls, vr_type: Set[Union[DicomVRCode, str]]) -> bool:
    """Returns true if VR type is recognized by tooling.

    Args:
      vr_type: Known VR type.

    Returns:
      True if VR type is recongized by tooling.
    """
    recognized_types = (
        DicomStandardIODUtil._STRING_TYPE_VR_SET
        | DicomStandardIODUtil._FLOAT_TYPE_VR_SET
        | DicomStandardIODUtil._INT_TYPE_VR_SET
        | DicomStandardIODUtil._OTHER_TYPE_VR_SET
    )
    return bool(recognized_types & vr_type)

  @classmethod
  def is_vr_ui_type(cls, vr_type: Set[Union[DicomVRCode, str]]) -> bool:
    """Returns True if vr type is a string type."""
    return bool(frozenset(['UI']) & vr_type)

  @classmethod
  def is_vr_str_type(cls, vr_type: Set[Union[DicomVRCode, str]]) -> bool:
    """Returns True if vr type is a string type."""
    return bool(DicomStandardIODUtil._STRING_TYPE_VR_SET & vr_type)

  _VR_MAX_BYTES_SET = frozenset([
      ('AE', 16),
      ('AS', 4),
      ('AT', 4),
      ('CS', 16),
      ('DA', 8),
      ('DS', 16),
      ('DT', 26),
      ('FL', 4),
      ('FD', 8),
      ('IS', 12),
      ('OD', pow(2, 32) - 8),
      ('OF', pow(2, 32) - 4),
      ('SL', 4),
      ('SS', 2),
      ('TM', 14),
      ('UI', 64),
      ('UL', 4),
      ('UN', pow(2, 32) - 2),
      ('US', 2),
      ('UT', pow(2, 32) - 2),
      ('UC', pow(2, 32) - 2),
      ('UR', pow(2, 32) - 2),
  ])

  @classmethod
  def get_vr_max_bytes(cls, vr_type: Set[Union[DicomVRCode, str]]) -> int:
    """Returns max length of select vr types in bytes or -1."""
    max_bytes = [-1]
    for code, byt_size in DicomStandardIODUtil._VR_MAX_BYTES_SET:
      if code in vr_type:
        max_bytes.append(byt_size)
    return int(max(max_bytes))

  _VR_MAX_CHARS_SET = frozenset(
      [('LO', 64), ('LT', 10240), ('PN', 64), ('SH', 16), ('ST', 1024)]
  )

  @classmethod
  def get_vr_max_chars(cls, vr_type: Set[Union[DicomVRCode, str]]) -> int:
    """Returns max length of select string vr types in characters or -1."""
    max_characters = [-1]
    for code, ch_size in DicomStandardIODUtil._VR_MAX_CHARS_SET:
      if code in vr_type:
        max_characters.append(ch_size)
    return int(max(max_characters))

  def _search_norm(self, name: str) -> str:
    return name.lower().replace(' ', '')

  def _get_iod_primary_module(self, iod_name: IODName) -> str:
    if iod_name in self._iod_modules:
      return iod_name
    return self._iod_to_module_map.get(
        self.normalize_sop_class_name(iod_name), iod_name
    )

  def normalize_sop_class_name(self, iod_name: str) -> IODName:
    """Return IOD SOPClassUID Name.

    Args:
      iod_name: IOD name can be full sop class uid storage name, primary module
        name or IOD SOPCLASS UID.

    Returns:
      IODName
    """
    if iod_name in self._iod_uid:
      return IODName(iod_name)
    elif _RE_IS_UID_IOD.fullmatch(iod_name) is not None:
      name = self.get_sop_classid_name(iod_name)
      if name is not None:
        return name
    else:
      search_name = self._search_norm(iod_name)
      for name, module in self._iod_to_module_map.items():
        if (
            self._search_norm(name) == search_name
            or self._search_norm(module) == search_name
        ):
          return IODName(name)
    return IODName(iod_name)

  def get_sop_classid_name(self, sop_classid: str) -> Optional[IODName]:
    """Returns the name defined for a given sop_classid.

    Args:
      sop_classid: DICOM SOPClassID.

    Returns:
       String name of DICOM IOD.
    """
    search_id = sop_classid.strip()
    for iod_name, iod_uid in self._iod_uid.items():
      if iod_uid == search_id:
        return IODName(iod_name)
    return None

  def get_sop_classname_uid(
      self, sop_class_name: Union[IODName, str]
  ) -> Optional[str]:
    """Takes SOP Class id name and returns uid.

    Args:
       sop_class_name: Name of class e.g.,'VL Whole Slide Microscopy Image
         Storage'.

    Returns:
      DICOM UID
    """
    return self._iod_uid.get(self.normalize_sop_class_name(sop_class_name))
