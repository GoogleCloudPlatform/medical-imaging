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
"""Abstract interface for local and server based DICOM access."""
import abc
from typing import List, Mapping, Optional

from pathology.dicom_proxy import dicom_url_util
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import frame_retrieval_util
from pathology.dicom_proxy import icc_color_transform
from pathology.dicom_proxy import metadata_util
from pathology.dicom_proxy import render_frame_params
from pathology.dicom_proxy import user_auth_util


class DicomInstanceRequest(metaclass=abc.ABCMeta):
  """Abstract class defining downsampling DICOM instance source interface."""

  @abc.abstractmethod
  def download_instance(self, path: str) -> None:
    """Downloads DICOM instance from DICOM Store or copies local file to path.

    Args:
      path: File path to write DICOM instance to.

    Returns:
      None
    """

  @abc.abstractmethod
  def icc_profile(self) -> Optional[bytes]:
    """Returns ICC profile stored in DICOM instance or None."""

  @abc.abstractmethod
  def icc_profile_color_space(self) -> str:
    """Returns ICC profile colorspace."""

  @abc.abstractmethod
  def icc_profile_transform(
      self, transform: enum_types.ICCProfile
  ) -> Optional[icc_color_transform.IccColorTransform]:
    """Returns ICC Profile transformation used to convert instance to SRGB.

    Internally transform request is cached at DICOMStore/Study/Series to
    speed repeated requests for the same transformation.

    Args:
      transform: ICC Color transform to perfrom.

    Returns:
      ICC ColorProfile Transformation.
    """

  @property
  @abc.abstractmethod
  def dicom_sop_instance_url(self) -> dicom_url_util.DicomSopInstanceUrl:
    """Returns DICOMweb SOPInstance url or local file path for local instance."""

  @property
  @abc.abstractmethod
  def user_auth(self) -> user_auth_util.AuthSession:
    """Returns user authentication."""

  @property
  @abc.abstractmethod
  def metadata(self) -> metadata_util.DicomInstanceMetadata:
    """Returns select downsampling metadata DICOM instance."""

  @abc.abstractmethod
  def get_dicom_frames(
      self, params: render_frame_params.RenderFrameParams, frames: List[int]
  ) -> frame_retrieval_util.FrameImages:
    """Returns frame images in requested compression format.

       Frame images request is retrieved using thread pool to speed request and
       is cached internally to speed repeated requests for the same frame.

    Args:
      params: Rendered Parameters.
      frames: List/Set of frames numbers to return from instance.

    Returns:
      FrameImages
    """

  @abc.abstractmethod
  def get_raw_dicom_frames(
      self,
      params: render_frame_params.RenderFrameParams,
      frame_numbers: List[int],
  ) -> List[bytes]:
    """Returns list of bytes stored in frames in DICOM instance.

    DICOM store method is uncached and performs frame retrieval in a single
    HTTP request. Returned results stored in cache.

    Args:
      params: RenderFrameParameters for the request.
      frame_numbers: List of DICOM frame numbers to load; first frame index = 1.

    Returns:
      List of bytes stored in requested frames.
    """

  @property
  @abc.abstractmethod
  def url_args(self) -> Mapping[str, str]:
    """Arguments to url instance request."""

  @property
  @abc.abstractmethod
  def url_header(self) -> Mapping[str, str]:
    """Returns headers passed to HTTP request."""
