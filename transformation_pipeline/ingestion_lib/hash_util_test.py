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
"""Tests for hash_utils."""
import os

from absl.testing import absltest

from transformation_pipeline.ingestion_lib import hash_util


class HashUtilsTest(absltest.TestCase):

  def test_md5_file_hash(self):
    """Tests MD5 hash computes expected value."""
    out_dir = self.create_tempdir()
    path = os.path.join(out_dir, 'test.txt')
    with open(path, 'wt') as outfile:
      outfile.write('1\n')
    self.assertEqual(
        'b026324c6904b2a9cb4b88d6d61c81d1', hash_util.md5hash(path)
    )

  def test_sha512_file_hash(self):
    """Tests Sha512 hash computes expected value."""
    out_dir = self.create_tempdir()
    path = os.path.join(out_dir, 'test.txt')
    with open(path, 'wt') as outfile:
      outfile.write('1\n')
    self.assertEqual(
        (
            '3abb6677af34ac57c0ca5828fd94f9d886c26ce59a8ce60ecf67780'
            '79423dccff1d6f19cb655805d56098e6d38a1a710dee59523eed751'
            '1e5a9e4b8ccb3a4686'
        ),
        hash_util.sha512hash(path),
    )


if __name__ == '__main__':
  absltest.main()
