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
"""Dataflow launcher for transformation pipeline."""

from absl import app
from absl import flags
import apache_beam as beam


_FULL_GCS_FILE_INGEST_LIST_FLG = flags.DEFINE_list(
    name='full_gcs_file_to_ingest_list',
    default=[],
    help='Full list of GCS files to ingest.',
)


class LaunchTransformationPipelineDoFn(beam.DoFn):
  """Launches the transformation pipeline."""

  def process(self, gcs_file: str, ingest_args: list[str]) -> None:
    """Launches the transformation pipeline for the given GCS file input.

    Args:
      gcs_file: GCS file to ingest.
      ingest_args: Args used in the transformation pipeline.
    """
    # pylint: disable=g-import-not-at-top, reimported, redefined-outer-name
    from absl import flags
    from pathology.shared_libs.logging_lib import cloud_logging_client
    from pathology.transformation_pipeline import ingest_flags
    from pathology.transformation_pipeline import gke_main

    # TODO: Update with the proper variables.
    ingest_args.extend([
        f'--{ingest_flags.GCS_FILE_INGEST_LIST_FLG.name}={gcs_file}',
        f'--{ingest_flags.GCS_SUBSCRIPTION_FLG.name}=mock-gcs-subscription',
        f'--{ingest_flags.PROJECT_ID_FLG.name}=mock-project-id',
    ])
    flags.FLAGS(ingest_args, known_only=True)
    try:
      gke_main.main(unused_argv=None)
      cloud_logging_client.info('Transformation pipeline done.')
    except Exception as exp:
      cloud_logging_client.critical(
          'Unexpected error running transformation pipeline', exp
      )
      raise


def run_transformation_pipeline(
    pipeline: beam.Pipeline,
    gcs_file_to_ingest_list: list[str],
    ingest_args: list[str],
) -> None:
  """Runs the transformation pipeline."""
  _ = (
      pipeline
      | 'GetGCSFilesToIngest' >> beam.Create(gcs_file_to_ingest_list)
      | 'LaunchTransformationPipeline'
      >> beam.ParDo(LaunchTransformationPipelineDoFn(), ingest_args=ingest_args)
  )


def main(argv: list[str]):
  if not _FULL_GCS_FILE_INGEST_LIST_FLG.value:
    raise app.UsageError(
        'Must specify --full_gcs_file_to_ingest_list with at least one file.'
    )

  beam_options = beam.options.pipeline_options.PipelineOptions()

  with beam.Pipeline(options=beam_options) as pipeline:
    run_transformation_pipeline(
        pipeline=pipeline,
        gcs_file_to_ingest_list=_FULL_GCS_FILE_INGEST_LIST_FLG.value,
        ingest_args=argv,
    )


if __name__ == '__main__':
  app.run(main, flags_parser=lambda args: flags.FLAGS(args, known_only=True))
