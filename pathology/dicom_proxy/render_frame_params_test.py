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
from absl.testing import parameterized

from pathology.dicom_proxy import cache_enabled_type
from pathology.dicom_proxy import enum_types
from pathology.dicom_proxy import proxy_const
from pathology.dicom_proxy import render_frame_params

# Types
_Compression = enum_types.Compression
_Interpolation = enum_types.Interpolation


class RenderFrameParamsTest(parameterized.TestCase):

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
    param2.viewport = render_frame_params.Viewport(['1', '1'])
    param1 = dataclasses.asdict(param1)
    param2 = dataclasses.asdict(param2)
    for key in param1:
      self.assertNotEqual(param1[key], param2[key])

  @parameterized.named_parameters([
      dict(testcase_name='undefined_viewport', viewport=None, expected=False),
      dict(
          testcase_name='empty_viewport',
          viewport=render_frame_params.Viewport([]),
          expected=False,
      ),
      dict(
          testcase_name='defined',
          viewport=render_frame_params.Viewport(['1', '1']),
          expected=True,
      ),
      dict(
          testcase_name='missing_fields_viewport',
          viewport=render_frame_params.Viewport(['', '1', '1']),
          expected=False,
      ),
  ])
  def test_render_dicom_frames_viewport_defined(self, viewport, expected):
    param1 = render_frame_params.RenderFrameParams(viewport=viewport)
    self.assertEqual(param1.is_viewport_defined(), expected)

  def test_viewport_init_full(self):
    v = render_frame_params.Viewport(['10', '20', '30', '40', '50', '60'])
    self.assertEqual(v.vw(), 10)
    self.assertEqual(v.vh(), 20)
    self.assertEqual(v.sx(), 30)
    self.assertEqual(v.sy(), 40)
    self.assertEqual(v.sw(70), 50)
    self.assertEqual(v.sh(80), 60)
    self.assertTrue(v.is_defined())

  def test_viewport_init_min_1(self):
    v = render_frame_params.Viewport(['10', '20'])
    self.assertEqual(v.vw(), 10)
    self.assertEqual(v.vh(), 20)
    self.assertEqual(v.sx(), 0)
    self.assertEqual(v.sy(), 0)
    self.assertEqual(v.sw(70), 70)
    self.assertEqual(v.sh(80), 80)
    self.assertTrue(v.is_defined())

  def test_viewport_init_min_2(self):
    v = render_frame_params.Viewport(['10', '20', '', '', '', ''])
    self.assertEqual(v.vw(), 10)
    self.assertEqual(v.vh(), 20)
    self.assertEqual(v.sx(), 0)
    self.assertEqual(v.sy(), 0)
    self.assertEqual(v.sw(70), 70)
    self.assertEqual(v.sh(80), 80)
    self.assertTrue(v.is_defined())

  def test_viewport_undefined_self(self):
    v = render_frame_params.Viewport([])
    self.assertFalse(v.is_defined())


if __name__ == '__main__':
  absltest.main()
