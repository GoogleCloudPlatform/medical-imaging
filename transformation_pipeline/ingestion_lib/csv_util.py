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
"""CSV file reading utility."""
from typing import Any, Optional

import pandas


def _csv_header_lines(path: str) -> int:
  """Count commented header lines in CSV.

  Args:
    path: file path to read.

  Returns:
    number of commented header lines.
  """
  comment_header_lines = 0
  with open(path, 'rt') as infile:
    for line in infile:
      line = line.strip()
      if line and not line.startswith('#'):
        break
      comment_header_lines += 1
  return comment_header_lines


def read_csv(path: str, chunksize: Optional[int] = None) -> Any:
  """Opens a CSV file using pandas for reading.

  Args:
    path: path to file.
    chunksize: optional number of rows to read from csv in batch.

  Returns:
    Pandas CSV Reader Object
  """
  return pandas.read_csv(
      path,
      header=0,
      index_col=False,
      engine='python',
      chunksize=chunksize,
      dtype=str,
      comment=None,
      skiprows=_csv_header_lines(path),
      skip_blank_lines=True,
      escapechar='\\',
      skipinitialspace=True,
      quotechar='"',
  )
