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
"""Util class and methods for dicom tags."""
import dataclasses


@dataclasses.dataclass
class DicomTag:
  """Wraps a dicom tag."""

  address: str
  keyword: str

  def get_address_hex(self) -> str:
    """Returns dicom tag address in hex format."""
    return f'0x{self.address}'
