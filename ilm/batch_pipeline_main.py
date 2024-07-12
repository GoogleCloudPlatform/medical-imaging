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
#
# ==============================================================================
"""Image Lifecycle Management (ILM) Dataflow batch pipeline main."""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from ilm_lib import dicom_store_lib
from ilm_lib import heuristics
from ilm_lib import logs_lib
from ilm_lib import pipeline_util


def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--ilm_config_gcs_uri',
      dest='ilm_config_gcs_uri',
      default=None,
      help='GCS URI for ILM config.',
  )
  return parser.parse_known_args()


def run() -> None:
  """Main entry point; defines and runs the pipeline."""
  known_args, pipeline_args = parse_args()
  pipeline_options = PipelineOptions(pipeline_args)
  ilm_cfg = pipeline_util.read_ilm_config(known_args.ilm_config_gcs_uri)
  logging.info(
      'Starting pipeline with options %s and ILM config %s',
      pipeline_options.get_all_options(),
      json.dumps(json.loads(ilm_cfg.to_json()), indent=2),
  )

  with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read audit logs and count instance access.
    instances_to_access_count = (
        pipeline
        | 'ReadDataAccessLogs'
        >> beam.io.ReadFromBigQuery(
            query=logs_lib.get_data_access_logs_query(ilm_cfg),
            use_standard_sql=True,
        )
        | 'ParseAuditLogs'
        >> beam.ParDo(logs_lib.ParseDataAccessLogsDoFn(ilm_cfg))
        | 'GroupByInstance' >> beam.GroupByKey()
        | 'ComputeTotalAccessCount'
        >> beam.Map(logs_lib.compute_log_access_metadata)
    )

    # Read DICOM instances metadata.
    instances_to_metadata = (
        pipeline
        | 'ReadFromDicomStoreTable'
        >> beam.io.ReadFromBigQuery(
            query=dicom_store_lib.get_dicom_store_query(ilm_cfg),
            use_standard_sql=True,
        )
        | 'ParseDicomStoreMetadata'
        >> beam.Map(dicom_store_lib.parse_dicom_metadata)
        | 'FilterOutDisallowList'
        >> beam.Filter(pipeline_util.should_keep_instance, ilm_cfg)
    )

    # Merge {DICOM metadata, access count} and compute storage class changes.
    instances_to_merge = [instances_to_access_count, instances_to_metadata]
    instances_to_move = (
        instances_to_merge
        | 'MergeInstances' >> beam.CoGroupByKey()
        | 'IncludeAccessCountInMetadata'
        >> beam.ParDo(pipeline_util.include_access_count_in_metadata)
        | 'ComputeStorageClassChanges'
        >> beam.ParDo(heuristics.ComputeStorageClassChangesDoFn(ilm_cfg))
    )

    # Batch instances and write SetBlobStorageSettingsRequest filter files to
    # GCS.
    filter_files = (
        instances_to_move
        | 'KeyByNewStorageClass' >> beam.Map(lambda x: (x.new_storage_class, x))
        | 'BatchInstances'
        >> beam.GroupIntoBatches(
            batch_size=ilm_cfg.dicom_store_config.set_storage_class_max_num_instances,
        )
        | 'GenerateFilterFiles'
        >> beam.ParDo(dicom_store_lib.GenerateFilterFilesDoFn(ilm_cfg))
    )

    # Perform storage class updates in DICOM Store and generate report(s).
    _ = (
        filter_files
        | 'UpdateStorageClasses'
        >> beam.ParDo(dicom_store_lib.UpdateStorageClassesDoFn(ilm_cfg))
        | 'KeyBySingleKey' >> beam.Map(lambda x: (0, x))
        | 'CombineAllOperations' >> beam.GroupByKey()
        | 'WaitForLongRunningOperationsAndGenerateReport'
        >> beam.ParDo(dicom_store_lib.GenerateReportDoFn(ilm_cfg))
    )

if __name__ == '__main__':
  run()
