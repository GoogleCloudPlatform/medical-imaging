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
"""Test for render frame params."""
import dataclasses

from absl.testing import absltest

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import render_frame_params

# Types
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation


class RenderFrameParamsTest(absltest.TestCase):

  def test_render_dicom_frames_copy(self):
    param1 = render_frame_params.RenderFrameParams()
    param2 = param1.copy()
    self.assertEqual(dataclasses.asdict(param1), dataclasses.asdict(param2))

    # Validating that all attributes have been copied by value.
    param2.downsample += 1.0
    param2.interpolation = _Interpolation.LINEAR
    param2.compression = _Compression.PNG
    param2.quality += 1
    param2.icc_profile = proxy_const.ICCProfile.SRGB
    param2.enable_caching = cache_enabled_type.CachingEnabled(False)
    param2.embed_iccprofile = False
    param1 = dataclasses.asdict(param1)
    param2 = dataclasses.asdict(param2)
    for key in param1:
      self.assertNotEqual(param1[key], param2[key])


if __name__ == '__main__':
  absltest.main()
