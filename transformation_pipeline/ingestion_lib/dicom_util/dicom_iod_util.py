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
"""Utility convert to DICOM Tables and IOD into unified representation.

The dicom standard describes a document model (IOD) as a:
  List of modules.
    Each module describes a list of module tables.
    module tables have M (manditiory), C (conditional), U (optional)
    designations.

        Tables describe one or more (often more) nodes within the IOD document
        model. Tables are described as a combination of inplace tag def
        (inline), and linked tables which are tables whose content is inserted
        in the referencing table in the position it is being referenced from.

        Tables describe inline definitions for dicom tags with each tag having
        a table specific tabel level requirement (1, 1C, 2, 2C, or 3)

        Tables from IOD models can have overlapping TAG defitions.
        With different, respective Module level, and table level requirements.

  The code in this utility takes relatively raw dicom standard derived table
  representations (see dicom_standard_util) and generates flattened IOD node
  level representations of the document model.


class DicomIODDatasetUtil : Wraps, a representation of the dicom standard,
  and provides access to flattened, at IOD node level, representations
  the IOD document model. The class caches IOD decoding to avoid repeative
  IOD node generation.

    def get_iod_dicom_dataset(self,
      iod_name: dcm_util.IODName,
      iod_path: dcm_util.DicomPathType = None
    ) -> DICOMDataset:

class DICOMDataset : Represent a flattened node within a IOD document model.
   The node defines a set of DICOMtags at a given node within the document
   model.  SQ tags define references to child nodes.

class DICOMTag : Represents instance of tag at specific node in the IOD
"""
from __future__ import annotations

import collections
import copy
import dataclasses
from typing import Any, Iterator, List, NewType, Optional, Sequence, Set, Tuple, Union

from transformation_pipeline.ingestion_lib.dicom_util import dicom_standard_util as dcm_util

_DicomTableTagRequirement = dcm_util.DicomTableTagRequirement
DicomModuleUsageRequirement = NewType('DicomModuleUsageRequirement', str)


class IODDoesNotDefineModuleError(Exception):
  """IOD does not define one or more modules."""


class DicomPathError(Exception):
  """Invalid DICOM IOD Path."""

  def __init__(
      self, msg, iod_name: str, error_path: List[str], full_path: List[str]
  ):
    super().__init__(msg)
    self.iod_name = iod_name
    self.error_path = copy.copy(error_path)
    self.full_path = copy.copy(full_path)

  def __str__(self) -> str:
    return '\n'.join([
        super().__str__(),
        f'iod_name: {self.iod_name}',
        f'error_path: {self.error_path}full_path: {self.full_path}',
    ])


class DICOMTag:
  """Represents instance of DICOM tag within a DICOM Dataset."""

  def __init__(
      self,
      dcm: dcm_util.DcmTagDef,
      required: _DicomTableTagRequirement,
      module_usage: DicomModuleUsageRequirement,
  ):
    self._tag_def = dcm  # instance of tag being defined
    self._vr = set()
    for vr in self._tag_def.vr:
      self._vr.add(vr)
    self._child_dataset = None
    self._required = set()
    self._required.add(_DicomTableTagRequirement(required.upper()))
    # tables tag requirement def
    # tags from different modules in same IOD can have different requirements.
    # storing all but general assumption is most strict is typically correct.
    self._module_usage = set()
    if module_usage is not None:
      self._module_usage.add(module_usage)  # module usage requirements.

  @property
  def retired(self) -> bool:
    return bool(self._tag_def.retired)

  @property
  def module_usage(self) -> Set[DicomModuleUsageRequirement]:
    """Returns DICOM module level requirement(s) for tag instance.

    Can have multiple due to multiple modules defining instances of tags
    """
    return self._module_usage

  def get_highest_module_req(self) -> DicomModuleUsageRequirement:
    """Return highest module req for tag.

       The same tag can be defined in multiple iod modules with different
       requirement levels.

       M = manditiory
       C = Conditional
       U = Optional

    Returns:
      Highest module requirement for tag.

    Raises:
      DICOMSpecMetadataError : if module requirement is not recognized.
    """
    for code in ['M', 'C', 'U']:
      if code in self._module_usage:
        return DicomModuleUsageRequirement(code)
    raise dcm_util.DICOMSpecMetadataError(
        f'Unrecognized module requirement; Module Req: {code} in Tag: {self}'
    )

  @property
  def required(self) -> Set[_DicomTableTagRequirement]:
    """Returns DICOM Table requirement(s) for tag instance.

    Can have multiple due to multiple modules defining instances of tags
    """
    return self._required

  def has_required(self, requirement: _DicomTableTagRequirement) -> bool:
    """Returns True if tag defines specified requirement.

       Can have multiple due to multiple modules defining instances of tags

    Args:
      requirement: Requirement to test e.g. ('1','1C','2','2C', or '3').

    Returns:
      True if requirement is specified.
    """
    return requirement.upper() in self._required

  @property
  def lowest_tag_type_requirement(self) -> str:
    """Returns lowest DICOM tag type for tag."""
    for dicom_tag_type in ('1', '1C', '2', '2C', '3'):
      if dicom_tag_type in self._required:
        return dicom_tag_type
    raise ValueError('Tag is missing type requirement.')

  @property
  def vm(self) -> dcm_util.DicomVMCode:
    """Returns DICOM VM of tag."""
    return self._tag_def.vm

  @property
  def vr(self) -> Set[dcm_util.DicomVRCode]:
    """Returns DICOM VR type(s) of tag."""
    return self._vr

  @property
  def address(self) -> dcm_util.DicomTagAddress:
    """Returns DICOM address for tag."""
    return self._tag_def.address

  @property
  def keyword(self) -> dcm_util.DicomKeyword:
    """Returns DICOM keyword for tag."""
    return self._tag_def.keyword

  def is_sq(self) -> bool:
    """Returns true if tag is Dicom SQ.

    SQ almost always have non-null sq_dataset
    """
    return 'SQ' in self._tag_def.vr

  @property
  def sq_dataset(self) -> Optional[DICOMDataset]:
    """Returns child DICOMDataset.

    Only defined for VR type tags SQ.
    """
    return self._child_dataset

  @sq_dataset.setter
  def sq_dataset(self, ds: DICOMDataset):
    """Set child DICOMDataset.

      Shold only be called for VR type tags SQ.

    Args:
      ds: DicomDataset

    Raises:
      DICOMSpecMetadataError if attempting to set sq_dataset on tag which
      does not have SQ VR type.
    """
    if not self.is_sq():
      raise dcm_util.DICOMSpecMetadataError(
          f'Cannot set sq dataset on for non sq tag; Tag: {self}'
      )
    self._child_dataset = ds

  def __str__(self) -> str:
    retired_str = ' Retired' if self.retired else ''
    return (
        f'{self.keyword} ({self.address}) {list(self.vr)}'
        f' {list(self.module_usage)} {list(self.required)}{retired_str}'
    )


@dataclasses.dataclass
class UnDecodedTableLine:
  requirement: DicomModuleUsageRequirement
  line: dcm_util.TableLine


class DICOMDataset:
  """Represents a given collection of DICOM tags at aribitrary position in IOD.

  Class handles:
    * decode json IOD def into single table definition
      (e.g. unrolling linked tables).
    * Merging dataset definitions across modules.
    * Accessing tags.
  """

  def __init__(
      self,
      iod_name: dcm_util.IODName,
      path: str,
      dicom_standard: dcm_util.DicomStandardIODUtil,
  ):
    self._iod_name = iod_name
    self._path = path  # path to dataset in dicom IOD
    self._dicom_standard = dicom_standard  # ref to dicom standard util.
    self._dataset = collections.OrderedDict()  # parsed dataset container.
    self.clear_level_line_queue()  # Internal queue used for parsing tags.

  @property
  def level_line_queue(self) -> List[UnDecodedTableLine]:
    return self._level_line_queue

  def clear_level_line_queue(self):
    """Clear level line queue."""
    self._level_line_queue = []

  def level_line_queue_append(
      self, lvl_que: Union[UnDecodedTableLine, List[UnDecodedTableLine]]
  ):
    """Append lines to the level line queue.

    Args:
      lvl_que: List of lines of lines to append.
    """
    if isinstance(lvl_que, List):
      self._level_line_queue += lvl_que
    else:
      self._level_line_queue.append(lvl_que)

  def add_tag(self, tag: DICOMTag):
    """Add/merges DICOM tag definition into the DICOM dataset.

       The same tag can be described in multiple modules.
       Tag requirements may not be the same. This code merges
       tag definitions.

    Args:
      tag: Tag to being added.

    Raises:
      DICOMSpecMetadataError if assumptions that iod
      module tags have same Address, VR, VM defitions is not met.
    """
    existing_tag = self._dataset.get(tag.address)
    if existing_tag is None:
      # new tag
      self._dataset[tag.address] = tag
    else:
      # merge tag definition into an existing definition
      # validate core assumptions match for both tags.
      if tag.address != existing_tag.address:
        raise dcm_util.DICOMSpecMetadataError(
            f'Merged tag address do not match; Tag 1:{existing_tag}; '
            f'Tag 2: {tag}'
        )
      for vr in tag.vr:
        if vr not in existing_tag.vr:
          raise dcm_util.DICOMSpecMetadataError(
              'Merged tag vr type definitions do not '
              f'match; Tag 1:{existing_tag}; '
              f'Tag 2: {tag}'
          )
      if tag.vm != existing_tag.vm:
        raise dcm_util.DICOMSpecMetadataError(
            'Merged tag vm multiplicty do not match; '
            f'Tag 1:{existing_tag}; Tag 2: {tag}'
        )
      for req in tag.required:
        existing_tag.required.add(req)
      for usage in tag.module_usage:
        existing_tag.module_usage.add(usage)
      if existing_tag.is_sq():
        # if dataset is a sequence merge sequence.
        if existing_tag.sq_dataset is None:
          existing_tag.sq_dataset = tag.sq_dataset
        else:
          existing_tag.sq_dataset.merge(tag.sq_dataset)  # pytype: disable=attribute-error  # always-use-return-annotations

  def merge(self, ds: DICOMDataset):
    """Merge another DICOM dataset into this one."""
    if ds is None:
      return
    # merge defined tags
    for tag in ds.values():
      self.add_tag(tag)
    # merge unparsed lines (etc. tags, table ref).
    self.level_line_queue_append(ds.level_line_queue)

  def _get_child_tag_sq_dataset(self, tag: DICOMTag) -> DICOMDataset:
    """Returns the dataset referenced by SQ tag.

    Creates dataset if not exist.

    Args:
      tag: Tag to get child dataset for.

    Returns:
      DicomDataset

    Raises:
      DICOMSpecMetadataError : if tag is not defined having SQ VR type.
    """
    dataset = tag.sq_dataset
    if dataset is None:
      if not tag.is_sq():
        raise dcm_util.DICOMSpecMetadataError(
            f'Can not dataset on for non sq tag; Tag: {tag}'
        )
      path = self._path + ':' + tag.keyword
      dataset = DICOMDataset(self._iod_name, path, self._dicom_standard)
      tag.sq_dataset = dataset
    return dataset

  @classmethod
  def _remove_child_level_indicator(
      cls, name: str, table_line: dcm_util.TableLine
  ) -> str:
    """Remove child level indicator from name.

    Args:
      name: string to remove child level indicator '>' from.
      table_line: table line def containing name

    Returns:
      child level name.

    Raises:
      DICOMSpecMetadataError if child level does not start with >
    """
    name = name.strip()
    if not name.startswith('>'):
      raise dcm_util.DICOMSpecMetadataError(
          f'Expected {name} to have > prefix indicator in'
          f'table line def {table_line}'
      )
    return name[1:]

  def get_module_tables(
      self, iod_module_list: List[dcm_util.ModuleDef]
  ) -> Iterator[List[UnDecodedTableLine]]:
    """Returns iterator which returns lines which define the module root tables."""
    for iod_module in iod_module_list:
      module_section_table_name = iod_module.module_section_table_name
      module_tables = self._dicom_standard.get_module_section_tables(
          module_section_table_name  # pytype: disable=wrong-arg-types
      )
      iod_module_usage = DicomModuleUsageRequirement(iod_module.usage[0])
      for module_table in module_tables:
        ref_table = self._dicom_standard.get_table(module_table.table_name)
        table_lines = [
            UnDecodedTableLine(iod_module_usage, line)
            for line in ref_table.table_lines
        ]
        yield table_lines

  def decode_dataset(self, line_stack: List[UnDecodedTableLine]):
    """Decode list of table lines to create the dicom dataset.

       Dicom tables define contents using a combination of
       inline and linked definitions.

       Example cut example from (table_name": "8.8-1",):
         {"type": "LinkedObject", "prefix": "Include", ...
         {"type": "InlineObject", "name": "Equivalent, ...
         {"type": "LinkedObject", "prefix": ">Include" ...

         The above are 3 lines from the table.
         Order of the rows matters!

         {"type": "LinkedObject", "prefix": "Include", ...
         First row indicates that a contents of another table
         is to be dumped directly in to the table being described.

         {"type": "InlineObject", "name": "Equivalent, ...
         Directly describe the definition of dicom tag describing a
         dicom sequence tag (VR=SQ). The structure of the leafs nodes
         described in the sequence is then described by the following line.

         {"type": "LinkedObject", "prefix": ">Include" ...
         This line again references another table.  It indicates that the
         tables content describes the sequence because the include is prefixed
         by a >,

         Tables can have links or sequences which are self recursive.

      This method decodes a target node of the IOD tree.
      the method works by maintaining a stack of nodes to decode.
      Nodes are poped from the stack.  Nodes applying to the decoded node
      are decoded and added to the node.  Nodes pertaining to child SQ or
      deeper levels are pushed to the child node and decoded only if the child
      is subsquently decoded.

    Args:
      line_stack: List of table lines to decode.
    """
    line_stack.reverse()
    last_tag = None
    while line_stack:
      line_item = line_stack.pop()  # get table line
      module_usage = line_item.requirement
      table_line = line_item.line
      if isinstance(table_line, dcm_util.InlineTableLine):
        # Inline Table definition.
        tag_table_level = table_line.name.count('>')
        if tag_table_level == 0:
          # line describes dicom tag in current dataset
          standard_tag_def = self._dicom_standard.get_tag(table_line.address)
          if standard_tag_def is None:
            raise dcm_util.DICOMSpecMetadataError(
                'Table line specifies tag not defined'
                ' in the parsed spec '
                f'{table_line.address}'
            )
          last_tag = DICOMTag(
              standard_tag_def, table_line.required, module_usage
          )
          self.add_tag(last_tag)
        else:
          if last_tag is None or not last_tag.is_sq():
            raise dcm_util.DICOMSpecMetadataError(
                'Cannot tag to Null or Non-SQ Tag'
            )
          # line describes dicom tag in child dataset
          # push tag definition to child. decode later if child is decoded.
          name = DICOMDataset._remove_child_level_indicator(
              table_line.name, table_line
          )
          new_line = dcm_util.InlineTableLine(
              name, table_line.address, table_line.required, table_line.comment
          )
          inner_ds = self._get_child_tag_sq_dataset(last_tag)
          inner_ds.level_line_queue_append(
              UnDecodedTableLine(module_usage, new_line)
          )

      elif isinstance(table_line, dcm_util.LinkedTableLine):
        # Line references another table.
        tag_table_level = table_line.prefix.count('>')
        if tag_table_level == 0:
          # table is inline reference. push inline tags into parse stack.
          if table_line.table_name == 'IODFunctionalGroupMacros':
            iod_module_list = (
                self._dicom_standard.get_iod_functional_group_modules(
                    self._iod_name
                )
            )
            for module_table_lines in self.get_module_tables(iod_module_list):
              module_table_lines = copy.copy(module_table_lines)
              module_table_lines.reverse()
              for line in module_table_lines:
                line_stack.append(line)
          else:
            lnk_tbl = self._dicom_standard.get_table(table_line.table_name)
            linked_tablelines = copy.copy(lnk_tbl.table_lines)
            linked_tablelines.reverse()
            # push tags onto decoding stack.
            for linked_line in linked_tablelines:
              line_stack.append(UnDecodedTableLine(module_usage, linked_line))
        else:
          if last_tag is None or not last_tag.is_sq():
            raise dcm_util.DICOMSpecMetadataError(
                'Cannot tag to Null or Non-SQ Tag'
            )
          # Line references another table which is included at a child level.
          # decode later.
          prefix = DICOMDataset._remove_child_level_indicator(
              table_line.prefix, table_line
          )
          new_line = dcm_util.LinkedTableLine(
              prefix, table_line.linked_table, table_line.usage
          )
          inner_ds = self._get_child_tag_sq_dataset(last_tag)
          inner_ds.level_line_queue_append(
              UnDecodedTableLine(module_usage, new_line)
          )

  def decode_sq_dataset(self):
    """Decodes deferred 'parsed' level lines."""
    if self.level_line_queue:
      self.decode_dataset(self.level_line_queue)
      self.clear_level_line_queue()

  def __len__(self) -> int:
    """Returns number of tags at dataset's level."""
    self.decode_sq_dataset()
    return len(self._dataset)

  @classmethod
  def _decode_tag_sq(cls, tag: DICOMTag):
    """If tag is SQ; Decodes tag child dataset.

    Args:
      tag: DICOMTag to decode.å
    """
    if tag is not None and tag.is_sq():
      ds = tag.sq_dataset
      if ds is not None:
        ds.decode_sq_dataset()

  def __getitem__(self, address: dcm_util.DicomPathEntry) -> Optional[DICOMTag]:
    """Returns Dicom tag referenced by address.

    Args:
      address: DICOM tag hex address or keyword.

    Returns:
      tag definition pointed to by address.
    """
    kw_address = self._dicom_standard.get_keyword_address(address)
    if kw_address is not None:
      address = kw_address
    if isinstance(address, str) and not address.startswith('0x'):
      address = f'0x{address}'
    self.decode_sq_dataset()
    tag = self._dataset.get(address)
    if tag is not None:
      DICOMDataset._decode_tag_sq(tag)
    return tag

  def get(
      self, address: dcm_util.DicomPathEntry, opt: Any = None
  ) -> Optional[DICOMTag]:
    val = self[address]
    if val is None:
      return opt
    return val

  def keys(self) -> Iterator[dcm_util.DicomTagAddress]:
    """Generator yields tag address defined in dataset.

    Yields:
      dcm_util.DicomTagAddress
    """
    self.decode_sq_dataset()
    for key in self._dataset:
      yield key

  def values(self) -> Iterator[DICOMTag]:
    """Generator yields tags defined in dataset.

    Yields:
      DICOMTag
    """
    self.decode_sq_dataset()
    for tag in self._dataset.values():
      DICOMDataset._decode_tag_sq(tag)
      yield tag

  def items(
      self,
  ) -> Iterator[Tuple[dcm_util.DicomTagAddress, DICOMTag]]:
    """Generator yields (address, tags) tuples defined in dataset.

    Yields:
      Tuple[dcm_util.DicomTagAddress, DICOMTag]
    """
    self.decode_sq_dataset()
    for item in self._dataset.items():
      _, tag = item
      DICOMDataset._decode_tag_sq(tag)
      yield item

  def __iter__(self):
    self._key_list = list(self._dataset)
    return self

  def __next__(self):
    if not self._key_list:
      raise StopIteration
    return self._key_list.pop()

  @property
  def path(self) -> str:
    """String path to dataset in IOD."""
    return self._path

  def __str__(self) -> str:
    """String representation of dicom dataset."""
    result = []
    result.append(f'DICOMDataset({self._path})')
    result.append('  Tags:')
    for tag in self.values():
      result.append(f'    {str(tag)}')
    return '\n'.join(result)


class DicomIODDatasetUtil:
  """Main class to extract arbitrary representations of IOD Tables."""

  def __init__(self, json_dir: Optional[str] = None):
    self._dicom_standard = dcm_util.DicomStandardIODUtil(json_dir=json_dir)

  @property
  def dicom_standard(self) -> dcm_util.DicomStandardIODUtil:
    return self._dicom_standard

  def _decode_table_lines(
      self,
      iod_name: dcm_util.IODName,
      table_lines: List[UnDecodedTableLine],
  ) -> DICOMDataset:
    """Converts list of UnDecodedTableLine to DICOMDataset.

    Args:
      iod_name: Name of DICOM IOD.
      table_lines: List of undecoded table lines.

    Returns:
      DICOMDataset
    """
    table = DICOMDataset(iod_name, 'root', self._dicom_standard)
    table.decode_dataset(table_lines)
    return table

  def _get_iod_root_table(
      self,
      iod_name: dcm_util.IODName,
      iod_module_list: List[dcm_util.ModuleDef],
  ) -> DICOMDataset:
    """Returns root level DICOMDataset for IOD.

    Args:
      iod_name: Name of DICOM IOD.
      iod_module_list: DICOM IOD module list.

    Returns:
      DICOMDataset

    Raises:
      DICOMSpecMetadataError : Invalid IOD Name
    """
    root_dataset = DICOMDataset(iod_name, 'root', self._dicom_standard)
    for module_table in root_dataset.get_module_tables(iod_module_list):
      dataset = self._decode_table_lines(iod_name, module_table)
      root_dataset.merge(dataset)
    return root_dataset

  def _get_iod_dataset_at_path(
      self,
      dataset_level: DICOMDataset,
      iod_name: dcm_util.IODName,
      iod_path: dcm_util.DicomPathType,
  ) -> Optional[DICOMDataset]:
    """Returns DicomDataset for path specified position in IOD.

    Args:
      dataset_level: Root DICOMDataset to iterate from.
      iod_name: Name of DICOM IOD.
      iod_path: List of tags to iterate over which define a path to a DICOM
        dataset.

    Returns:
      DICOMDataset

    Raises:
       DicomPathError: Invalid path.
    """
    for index, path in enumerate(iod_path):
      if path == 'root' and index == 0:
        continue
      tag = dataset_level[path]
      if tag is None:
        decoded_tag = '.'.join(iod_path[: index + 1])
        raise DicomPathError(
            (
                f'Dicom tag path specifies tag({decoded_tag}) not '
                f' in {iod_name} IOD.'
            ),
            iod_name,
            iod_path[: index + 1],
            iod_path,
        )
      # intermediate nodes of the must be sq tags.
      if tag.is_sq():
        dataset_level = tag.sq_dataset
      elif index + 1 != len(iod_path):
        # if node is not sq tag and node is not a leaf then path is incorrect.
        decoded_tag = '.'.join(iod_path[: index + 1])
        raise DicomPathError(
            (
                f'Dicom tag path specifies non-sq tag({decoded_tag}) in'
                ' middle  of path.'
            ),
            iod_name,
            iod_path[: index + 1],
            iod_path,
        )
    return dataset_level

  def get_iod_dicom_dataset(
      self,
      iod_name: dcm_util.IODName,
      iod_path: Optional[dcm_util.DicomPathType] = None,
      module_name_subset: Optional[Set[str]] = None,
      require_modules: Optional[Sequence[str]] = None,
  ) -> DICOMDataset:
    """Returns DicomDataset for path specified position in IOD.

       if iod_path specified.
         ignores 'root' if specified first element
         ignores 'non-sq' tag if last element.

    Args:
      iod_name: Name of DICOM IOD
      iod_path: Optional list of tags to iterate over which define a path to a
        DICOM dataset.
      module_name_subset: List of names of modules defined within the IOD to
        selectively return DICOM tags for.
      require_modules: Require listed IOD modules to be included regardless of
        Module IOD requirement level; e.g., treat listed modules with C or U
        usage requirement as having being mandatory.

    Returns:
      DICOMDataset

    Raises:
       DICOMSpecMetadataError : invalid iod name
       DicomPathError : invalid path
       IODDoesNotDefineModuleError: IOD does not define one or more modules in
         module_name_subset.
    """
    iod_name = self._dicom_standard.normalize_sop_class_name(iod_name)
    iod_module_list = self._dicom_standard.get_iod_modules(
        iod_name, require_modules=require_modules
    )
    if module_name_subset is not None:
      iod_modules = {str(module.name) for module in iod_module_list}
      missing_module_names = module_name_subset - iod_modules
      if missing_module_names:
        raise IODDoesNotDefineModuleError(
            f'IOD does not define module names: {missing_module_names}.',
            'iod_does_not_define_module_name',
        )
      iod_module_list = [
          module
          for module in iod_module_list
          if module.name in module_name_subset
      ]

    dataset_level = self._get_iod_root_table(iod_name, iod_module_list)
    if iod_path is not None:
      dataset_level = self._get_iod_dataset_at_path(
          dataset_level, iod_name, iod_path
      )
    return dataset_level

  def get_root_level_iod_tag_keywords(
      self,
      iod_name: dcm_util.IODName,
      module_name_subset: Optional[Set[str]] = None,
  ) -> List[str]:
    """Returns DICOM tag keywords defined at root level of IOD.

    Args:
      iod_name: Name of DICOM IOD.
      module_name_subset: Optional subset of IOD modules to return tags keywords
        for.
    """
    ds = self.get_iod_dicom_dataset(
        iod_name=iod_name,
        iod_path=None,
        module_name_subset=module_name_subset,
    )
    return [tag.keyword for tag in ds.values()]

  def get_iod_function_group_dicom_dataset(
      self,
      iod_name: dcm_util.IODName,
      iod_path: Optional[dcm_util.DicomPathType] = None,
  ) -> DICOMDataset:
    """Returns DicomDataset for path specified position in IOD.

       if iod_path specified.
         ignores 'root' if specified first element
         ignores 'non-sq' tag if last element.

    Args:
      iod_name: Name of DICOM IOD
      iod_path: Optional list of tags to iterate over which define a path to a
        DICOM dataset.

    Returns:
      DICOMDataset

    Raises:
       DICOMSpecMetadataError : invalid iod name
       DicomPathError : invalid path
    """
    iod_name = self._dicom_standard.normalize_sop_class_name(iod_name)
    iod_module_list = self._dicom_standard.get_iod_functional_group_modules(
        iod_name
    )
    dataset_level = self._get_iod_root_table(iod_name, iod_module_list)
    if iod_path is not None:
      dataset_level = self._get_iod_dataset_at_path(
          dataset_level, iod_name, iod_path
      )
    return dataset_level
