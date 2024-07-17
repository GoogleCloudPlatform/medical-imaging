# DICOM util

DICOM Standard is published at: https://www.dicomstandard.org/current

The standard is published in a variety of formats including XML.

The tool: dicom_iod_generator.py downloads xml from the standards website
and extracts, tags definitions and document model definitions from the standard
and outputs this data in json for input into dicom_standard.py and
dicom_iod_util.py to facilitate the determination of document model and tag
properties, validation of dicom, and auto-initialization of conditional type 2
dicom tags.

Implementation:
  dicom_iod_generation.py - main entry point and json generation.
  test_dicom_iod_generation.py - unit tests for parsers.
  lib -> dicom_iod_uid_xml_parser.py - generates iod x uid mapping json
      -> dicom_iod_xml_parser.py - generates: table def json
                                              module def json
                                              iod def json
      -> dicom_tag_xml_parser.py - generates: tag def json

      -> util -> dicom_abstract_parser.py  - base class for parsers
              -> dicom_iod_generator_exception.py - exception raised by parsers
              -> dicom_xml_core_parser.py - core methods used.
                                              - xml chapter extractor
                                              - table parser

To build against current dicom spec, run `dicom_iod_generator.py`
