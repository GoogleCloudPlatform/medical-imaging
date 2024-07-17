# Joining WSI Imaging with Metadata

## Purpose

This document describes how the Digital Pathology Transformation Pipeline joins
imaging with metadata.

## Background

The Digital Pathology Transformation pipeline is designed to facilitate high
throughput image ingestion. Transformation pipeline execution is triggered by
the arrival of imaging in the image ingestion bucket. The pipeline requires that
metadata be identified for all images (see reference[1]). Failure to identify
metadata associated with imaging will block image ingestion. Imaging is joined
with metadata by identifying a primary key value that maps the image to a single
row of in the metadata value table. The steps the pipeline uses to identify the
primary key value that maps the image to metadata are determined by the file
extension of the file that triggered pipeline execution.

The failed ingestion will be logged to cloud operations and the image file that
triggered the pipeline will be moved from the ingestion bucket to the failure
bucket. Ingestions failing due to inability of the pipeline to locate metadata
or decode a barcode can be manually corrected by adding the required metadata or
modifying the filename of the ingested image to include a parsable primary key
value, respectively. After correcting the error causing the condition the image
asset should be moved from the failure bucket to the ingestion bucket..

The flowchart below illustrates the steps used to identify the primary key for
[Openslide](https://openslide.org/) compatible whole slide imaging (svs). The
flow chart illustrates both successful and failed primary key identification.
Failed ingestions can be retried after the conditions causing the failure have
been corrected by moving the imaging from the failure bucket back into the
ingestion bucket.

![alt text](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/images/flowchart.png?raw=true)

## Image File Primary Key Detection

The file extension of the image that triggered transformation pipeline execution determines the steps used to determine the image's metadata primary key.

-  [Openslide compatible WSI formats](https://openslide.org/formats/) (Aspero[SVS, and TIF], Hamamatsu [VMS, VMU, NDPI], Leica[SCN], MIRAX[MRXS], Phillips[TIFF], Sakura[svsslide], Trestle[TIF], Ventana[BIF, TIF], Generic Tiled Tiff[TIFF]).
    1. Tested for primary key in filename (see Filename section below)
    2. Tested for primary key in label barcode image
    (see Label Barcode section below)

-  DICOM Files

    Digital Pathology DICOM files that describe images for the same slide scan
    (_e.g.,_ all instances have the same StudyInstanceUID and SeriesInstanceUID)
    should be ingested together by packaging the DICOM instances as an uncompressed
    zip archive. Digital pathology images described entirely by a single DICOM
    instance can be ingested without packaging by copying the DICOM instance (*.dcm)
    directly into the image ingestion bucket.

    1. Tested for primary key in DICOM Instance
    2. Tested for primary key in ingested filename (*.dcm) or archive (*.zip).
        Files within the zip archive are not tested.
    3. Tested for primary key in label barcode image. Requires that *.dcm file
        be a Label image or one of the files within the zip archive be a label image.

-  Flat Images (JPEG, PNG, and un-tiled TIFF images)
    1. Tested for primary key in filename

## Methods to Associate Imaging with Metadata

The Digital Pathology Transformation pipeline supports filename, barcode, and
DICOM tag based mechanisms to define a primary key for image metadata
association.

### Filename

Filename based identification is supported for all imaging formats.
Filenames are processed as follows:

1. Filenames are split into parts using the string defined by the ingestion
FILENAME_SLIDEID_SPLIT_STR environmental variable; default value = "_".
2. Filename parts are each tested against the regular expression defined by the
environmental variable  SLIDEID_REGEX, default value =
"^[a-zA-Z0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+)+".
3. File name parts identified in Step 1 that match the Regular expression
defined in Step 2 are searched against the metadata value primary key table. If
a match is found the matching row is used as the slides metadata. The first
successful match found is returned.
4. Optionally, if a metadata match is not found then the whole name, excluding
file extension, of the ingested slide can be tested as primary metadata key.
This functionality is enabled by setting the Transformation Pipeline
environmental variable _TEST_WHOLE_FILENAME_AS_SLIDEID = True_, by default the
environmental variable is False. The filename is not required to match the
SLIDEID_REGEX regular expression.

### Label Barcode

Slide label decoding is supported for whole slide images which are compatible
with the Openslide library and with DICOM images which include definitions for
label images. In brief, slide label images are searched for barcodes and the
identified barcodes are decoded. Decoded barcodes are tested as a metadata
primary key. The first successful match found is returned. The following barcode
formats are supported: UPC-A and UPC-E, EAN-8 and EAN-13, Code 39, 93, Code 128,
ITF, Codabar, RSS-14 (all variants), RSS Expanded (most variants), QR Code, Data
Matrix, Aztec, PDF 417, and MaxiCode. Barcode decoding can be disabled, by
setting the Transformation Pipeline environmental variable
`DISABLE_BARCODE_DECODER` = False.

### DICOM instances

The Barcode Value tag in DICOM instances is tested as a metadata primary key.

## References

[1] [HowTo: Transform Pipeline DICOM Metadata Values and Schema](https://github.com/GoogleCloudPlatform/Cloud-Pathology/blob/main/transformation_pipeline/docs/transform_pipeline_metadata_values_and_schema.md)