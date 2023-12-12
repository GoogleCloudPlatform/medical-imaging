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
"""Tests for pydicom_util."""
from absl.testing import absltest
import pydicom

from transformation_pipeline.ingestion_lib.dicom_util import pydicom_util


class PydicomUtilTest(absltest.TestCase):

  def test_set_string_keyword(self):
    ds = pydicom.Dataset()
    ds.PatientName = 'Smith^Bob'
    self.assertEqual(ds.PatientName, 'Smith^Bob')

    pydicom_util.set_dataset_tag_value(ds, 'PatientName', 'Smith^Sarah')

    self.assertEqual(ds.PatientName, 'Smith^Sarah')

  def test_set_int_keyword_from_int(self):
    ds = pydicom.Dataset()
    ds.HighBit = 6
    self.assertEqual(ds.HighBit, 6)

    pydicom_util.set_dataset_tag_value(ds, 'HighBit', 7)

    self.assertEqual(ds.HighBit, 7)

  def test_set_float_keyword_from_float(self):
    ds = pydicom.Dataset()
    ds.ExaminedBodyThickness = 1.0
    self.assertEqual(ds.ExaminedBodyThickness, 1.0)

    pydicom_util.set_dataset_tag_value(ds, 'ExaminedBodyThickness', 1.5)

    self.assertEqual(ds.ExaminedBodyThickness, 1.5)

  def test_set_int_keyword_from_str(self):
    ds = pydicom.Dataset()
    ds.HighBit = 6
    self.assertEqual(ds.HighBit, 6)

    pydicom_util.set_dataset_tag_value(ds, 'HighBit', '7')

    self.assertEqual(ds.HighBit, 7)

  def test_set_float_keyword_from_str(self):
    ds = pydicom.Dataset()
    ds.ExaminedBodyThickness = 1.0
    self.assertEqual(ds.ExaminedBodyThickness, 1.0)

    pydicom_util.set_dataset_tag_value(ds, 'ExaminedBodyThickness', '1.5')

    self.assertEqual(ds.ExaminedBodyThickness, 1.5)

  def test_set_dataset_tag_value_if_undefined_not_set(self):
    ds = pydicom.Dataset()
    ds.Manufacturer = 'Foo'

    self.assertFalse(
        pydicom_util.set_dataset_tag_value_if_undefined(
            ds, 'Manufacturer', 'Bar'
        )
    )
    self.assertEqual(ds.Manufacturer, 'Foo')

  def test_set_dataset_tag_value_if_undefined_set(self):
    ds = pydicom.Dataset()

    self.assertTrue(
        pydicom_util.set_dataset_tag_value_if_undefined(
            ds, 'Manufacturer', 'Bar'
        )
    )
    self.assertEqual(ds.Manufacturer, 'Bar')


if __name__ == '__main__':
  absltest.main()
