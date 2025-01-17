# Digital Pathology DICOM Proxy Server API


## All HTTP Methods

<span style="text-decoration:underline;">**Applies to all store requests**</span> (Base URL PATH Prefix)

<b><span style="color:orange">{URL_PATH_PREFIX}</span></b><span style="color:blue">/projects/<b>{GCP_PROJECT_ID}</b>/locations/<b>{DICOM_STORE_LOCATION}</b>/datasets/<b>{HEALTH_CARE_API_DATASET}</b>/dicomStores/<b>{DICOM_STORE}</b>/dicomWeb</span>

<span style="color:blue">(Blue)</span> Standard DICOMweb url path.<br>
<span style="color:orange">(Orange)</span> Digital Pathology DICOM Proxy Server API

The above base path should be used to to perform all transactions against the Proxy server.

**URL_PATH_PREFIX**= URL prefix path defined in Proxy server environment variable URL_PATH_PREFIX. This prefix should match the routing url prefix used by the load balancer.<br>
**GCP_PROJECT_ID**= GCP Project ID containing the target DICOM STORE.<br>
**HEALTH_CARE_API_DATASET**= Healthcare API dataset to conduct transactions against.<br>
**DICOM_STORE** = Name of DICOM Store.

## Downsampled frame retrieval

Returns frames from virtual downsampled views of DICOM  instances.

**Queries**:

***Frame retrieval:***

<b><span style="color:orange">{URL_PATH_PREFIX}</span></b><span style="color:blue">/projects/<b>{GCP_PROJECT_ID}</b>/locations/<b>{DICOM_STORE_LOCATION}</b>/datasets/<b>{HEALTH_CARE_API_DATASET}</b>/dicomStores/<b>{DICOM_STORE}</b>/dicomWeb/</span><span style="color:green">studies/<b>{StudyInstanceUID}</b>/series/<b>{SeriesInstanceUID}</b>/instances/<b>{SOPInstanceUID}</b>/frames/<b>{Frame_List}</b></span><span style="color:orange">?downsample=<b>{DownsamplingFactor}</b>&quality=<b>{quality}</b>&interpolation=<b>{Interpolation}</b>&iccprofile=<b>{iccprofile}&</b>disable_caching=<b>{disable_caching}</b>&</span><span style="color:purple">{Addtional Parameters}</span>


***Rendered Frame retrieval:***

<b><span style="color:orange">{URL_PATH_PREFIX}</span></b><span style="color:blue">/projects/<b>{GCP_PROJECT_ID}</b>/locations/<b>{DICOM_STORE_LOCATION}</b>/datasets/<b>{HEALTH_CARE_API_DATASET}</b>/dicomStores/<b>{DICOM_STORE}</b>/dicomWeb/</span><span style="color:green">studies/<b>{StudyInstanceUID}</b>/series/<b>{SeriesInstanceUID}</b>/instances/<b>{SOPInstanceUID}</b>/frames/<b>{Frame_List}</b>/rendered</span><span style="color:orange">?downsample=<b>{DownsamplingFactor}</b>&quality=<b>{quality}</b>&interpolation=<b>{Interpolation}</b>&iccprofile=<b>{iccprofile}&</b>disable_caching=<b>{disable_caching}</b>&</span><span style="color:purple">{Addtional Parameters}</b>

<span style="color:blue">(Blue)</span> Standard DICOMweb url path.<br>
<span style="color:green">(Green)</span> Standard DICOMweb study/series/instance path<br>
<span style="color:orange">(Orange)</span> Proxy server API<br>
<span style="color:purple">(Purple)</span> Additional DICOMweb query parameters.

**Accept header:**

* **'image/jpeg'** : return JPEG encoded image(s); Default if not specified; (Recommended)
* **'image/png'** : return PNG encoded image(s)
* **'image/webp'** : return webp encoded image(s)
* **'image/gif'** : return gif encoded image(s)
* **'image/jxl'**: return jpegxl encoded image(s)

**DICOM web parameter(s):**

* **Frame List**:comma separated value list of frames to return from the downsampled instance. Frame numbers are with respect to the position of the frame in the virtual fully tiled downsampled instance.  Frames are requested in parallel via thread pool.  Requesting batches of frames should be more performant than repeated requests for individual frames.  Frame requests are cached to minimize DICOM store transactions. Frame indexes start at 1 and increase.

**Digital Pathology Proxy Server API parameter(s):**

* **DownsamplingFactor**: Downsampling factor to apply to instance (value >= 1.0); undefined = 1.0; no change to instance dimensions. (floating point)

* **Quality**: Quality to encode downsampled imaging. Only applies to jpeg and webp encoded images.  Value (integer) 1 - 100, **default value = 95**. (integer). Image must be stored in DICOM using one of the following transfer syntaxes (JPEG Baseline 1.2.840.10008.1.2.4.50, JPEG2000 1.2.840.10008.1.2.4.90, or JPEG2000 1.2.840.10008.1.2.4.91).  If an instance is encoded using a transfer syntax other than those listed then the quality setting will have no effect and the image will be transcoded using the DICOM store.<p>DICOM images stored with JPEG encoded frames requested with downsampling factor == 1.0 (default) and ICC_Profile=No(default) are returned without decoding/re-encoding. Quality of image == quality setting used to generate the JPEG in the DICOM. 

* **interpolation**: Algorithm to use to perform image downsampling. Only applies if downsampling parameter > 1.0; (string);  downsampling implemented using OpenCV [(doc)](https://docs.opencv.org/3.4/da/d54/group__imgproc__transform.html); Possible values:

  * Area: inter area interpolation &lt;- **Default value**; (Recommended, moire'-free results)
  * Cubic: bicubic interpolation
  * Lanczos4: Lanczos4 interpolation
  * Linear: bilinear interpolation
  * Nearest: nearest neighbor interpolation

* [**Iccprofile**](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_8.3.5.html): Enable image color correction? (string); Possible values:

  * NO :  Do not perform color correction  &lt;- **Default value**
  * YES:  Do not perform color correction and embed ICC Profile stored in DICOM in the returned image.  If DICOM instance does not contain an ICC Profile same as NO.
  * SRGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to SRGBV4 color space and embed SRGBV4 color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.
  * ADOBERGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to ADOBERGB color space and embed ADOBERGB color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.
  * ROMMRGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to ROMMRGB color space and embed ROMMRGB color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.

* **embed_iccprofile**: Not part of DICOM standard. Flag enables ICCProfile to be omitted from returned image if rendered image iccprofile transform is requested (reduces bytes transferred/speed frame retrieval). Parameter has the greatest effect for SRGB, sizes of ICCProfiles: SRGB ICCProfile = ~61kb;  AdobeRGB = 0.6 kb; ROMMRGB = 0.9 kb.  Possible Values:

  * True:  Embed ICC profile bytes in rendered image if rendered image is generated with iccprofile=(yes|srgb|adobergb|rommrgb) &lt;- **Default value**
  * False: Transform image pixel values to requested colorspace but do not embed the ICCProfile of the colorspace transformed to in the image. The combination of  iccprofile=yes&embed_iccprofile=false is the same as iccprofile=no or omitting the iccprofile parameter entirely (pure-default).

* **disable_caching:** If set to true request will be fulfilled without using internal caches.  Use for testing.  (Caching is enabled by default);  Possible Values:
  * False &lt;- Default value; **(Recommended)**
  * True (Use for debugging purposes)

**Requirements:**

* DICOM instances must be WSI IOD DICOM.
* DICOM instances must have dimensional organization = TILED_FULL or have only one frame.
**Example:**

https://${DICOM_PROXY_URL}/projects/${PROJECT}/locations/${LOCATION}/datasets/${DATASET}/dicomStores/${DICOMSTORE}/dicomWeb/studies/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368218869027/series/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368221398029/instances/1.3.6.1.4.1.11129.5.7.0.1.440074136440.11879982.1661368272360294/frames/1,2,3,4/rendered?downsample=2&iccprofile=srgb

**Returns:**
Requested frame imaging in the requested imaging format (accept header).


## Rendered view of instance

Returns a single image of a wsi instance. The performance of this method will vary greatly with the size of the source image.

**Query**:
<b><span style="color:orange">{URL_PATH_PREFIX}</span></b><span style="color:blue">/projects/<b>{GCP_PROJECT_ID}</b>/locations/<b>{DICOM_STORE_LOCATION}</b>/datasets/<b>{HEALTH_CARE_API_DATASET}</b>/dicomStores/<b>{DICOM_STORE}</b>/dicomWeb/</span><span style="color:green">studies/<b>{StudyInstanceUID}</b>/series/<b>{SeriesInstanceUID}</b>/instances/<b>{SOPInstanceUID}</b>/rendered</span><span style="color:orange">?downsample=<b>{DownsamplingFactor}</b>&quality=<b>{quality}</b>&interpolation=<b>{Interpolation}</b>&iccprofile=<b>{iccprofile}&</b>disable_caching=<b>{disable_caching}&</b>embed_iccprofile=<b>{embed_iccprofile}</b>&</span><span style="color:purple"><b>{Addtional Parameters}</b></span>

**Accept header:**

* **'image/jpeg'** : return JPEG encoded image(s); Default if not specified; (Recommended)
* **'image/png'** : return PNG encoded image(s)
* **'image/webp'** : return webp encoded image(s)
* **'image/gif'** : return gif encoded image(s)
* **'image/jxl'**: return jpegxl encoded image(s)

**Digital Pathology Proxy Server API parameter(s):**

* **DownsamplingFactor**: Downsampling factor to apply to instance (value >= 1.0); undefined = 1.0; no change to instance dimensions. (floating point)

* **Quality**: Quality to encode downsampled imaging. Only applies to jpeg and webp encoded images.  Value (integer) 1 - 100, default value = 95. (integer)

* **interpolation**: Algorithm to use to perform image downsampling. Only applies if downsampling parameter > 1.0; (string);  downsampling implemented using OpenCV [(doc)](https://docs.opencv.org/3.4/da/d54/group__imgproc__transform.html); Possible values:

  * Area: inter area interpolation &lt;- **Default value**; (Recommended, moire'-free results)
  * Cubic: bicubic interpolation
  * Lanczos4: Lanczos4 interpolation
  * Linear: bilinear interpolation
  * Nearest: nearest neighbor interpolation

* [**Iccprofile**: Enable image color correction? (string); Possible values:](https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_8.3.5.html)

  * NO :  Do not perform color correction  &lt;- **Default value**
  * YES:  Do not perform color correction and embed ICC Profile stored in DICOM in the returned image.  If DICOM instance does not contain an ICC Profile same as NO.
  * SRGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to SRGBV4 color space and embed SRGBV4 color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.
  * ADOBERGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to ADOBERGB color space and embed ADOBERGB color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.
  * ROMMRGB: If DICOM instance contains an ICC Profile, convert RGB values in the returned image to ROMMRGB color space and embed ROMMRGB color space in the returned image. If DICOM instance does not contain an ICC Profile same as NO.

* **embed_iccprofile**: Not part of DICOM standard. Flag enables ICCProfile to be omitted from returned image if rendered image iccprofile transform is requested (reduces bytes transferred/speed frame retrieval). Parameter has the greatest effect for SRGB, sizes of ICCProfiles: SRGB ICCProfile = ~61kb;  AdobeRGB = 0.6 kb; ROMMRGB = 0.9 kb.  Possible Values:

  * True:  Embed ICC profile bytes in rendered image if rendered image is generated with iccprofile=(yes|srgb|adobergb|rommrgb) &lt;- **Default value**
  * False: Transform image pixel values to requested colorspace but do not embed the ICCProfile of the colorspace transformed to in the image. The combination of  iccprofile=yes&embed_iccprofile=false is the same as iccprofile=no or omitting the iccprofile parameter entirely (pure-default).

* **disable_caching:** If set to true request will be fulfilled without using internal caches.  Use for testing.  (Caching is enabled by default);  Possible Values:
  * False &lt;- Default value; **(Recommended)**
  * True (Use for debugging purposes)

**Requirements**:
* DICOM instances must be WSI IOD DICOM.
* DICOM instances must have dimensional organization = TILED_FULL or have only one frame.

**Returns:**
Requested imaging in the requested imaging format (accept header).

**Example:**
https://${DICOM_PROXY_URL}/projects/${PROJECT}/locations/${LOCATION}/datasets/${DATASET}/dicomStores/${DICOMSTORE}/dicomWeb/studies/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368218869027/series/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368221398029/instances/1.3.6.1.4.1.11129.5.7.0.1.440074136440.11879982.1661368272360294/render?downsample=2


## ICC Profile BulkData

DICOM metadata (JSON or XML) retrieved at the series or instance level that references WSI instances which contain an embedded ICCProfile will be supplemented with an BulkURI which can be used to retrieve the ICCProfile directly from the Digital Pathology Proxy Server.

Example JSON:

"00282000": {"vr": "OB", "BulkDataURI": "https://${DICOM_PROXY_URL}/projects/${PROJECT}/locations/${LOCATION}/datasets/${DATASET}/dicomStores/${DICOMSTORE}/dicomWeb/studies/1.3.6.1.4.1.11129.5.7.999.18649109954048068.141.1682349249232673/series/1.3.6.1.4.1.11129.5.7.0.1.448576058488.68814528.1682349272592176/instances/1.3.6.1.4.1.11129.5.7.0.1.448576058488.68814528.1682349279352178/bulkdata/OpticalPathSequence/0/ICCProfile"}}]}

The URL returned in the BulkDataURI parameter can then be used in an additional http transaction to retrieve the instance’s ICCProfile (Bytes).  Note URL in the bulk uri is instance specific and defines a request from the Digital Pathology Proxy Server that will return the ICC Profile stored in the instance.

## Downsampling Instance Search and Metadata retrieval

Returns JSON metadata for downsampled representations of the dimensions, frame numbers, and pixel spacing of DICOM instances. These functions are for convenience.

**Queries**

* **Metadata Query**
<span style="color:orange">**{URL_PATH_PREFIX}**</span><span style="color:blue">/projects/**{GCP_PROJECT_ID}**/locations/**{DICOM_STORE_LOCATION}**/datasets/**{HEALTH_CARE_API_DATASET}**/dicomStores/**{DICOM_STORE}**/dicomWeb/</span><span style="color:green">studies/**{StudyInstanceUID}**/series/**{SeriesInstanceUID}**/instances/**{SOPInstanceUID}**/metadata</span><span style="color:orange">?downsample=**{DownsamplingFactor}**&</span><span style="color:purple">**{Addtional Parameters}**</span>

* **Study/Series Instance Search**
<span style="color:orange">**{URL_PATH_PREFIX}**</span><span style="color:blue">/projects/**{GCP_PROJECT_ID}**/locations/**{DICOM_STORE_LOCATION}**/datasets/**{HEALTH_CARE_API_DATASET}**/dicomStores/**{DICOM_STORE}**/dicomWeb/</span><span style="color:green">studies/**{StudyInstanceUID}**/series/**{SeriesInstanceUID}**/instances</span><span style="color:orange">?downsample=**{DownsamplingFactor}**&</span><span style="color:purple">**{Addtional Parameters}**</span>

* **Study/ Instance Search**
<span style="color:orange">**{URL_PATH_PREFIX}**</span><span style="color:blue">/projects/**{GCP_PROJECT_ID}**/locations/**{DICOM_STORE_LOCATION}**/datasets/**{HEALTH_CARE_API_DATASET}**/dicomStores/**{DICOM_STORE}**/dicomWeb/</span><span style="color:green">studies/**{StudyInstanceUID}**/instances</span><span style="color:orange">?downsample=**{DownsamplingFactor}**&</span><span style="color:purple">**{Addtional Parameters}**</span>

* **Instance Search**
<span style="color:orange">**{URL_PATH_PREFIX}**</span><span style="color:blue">/projects/**{GCP_PROJECT_ID}**/locations/**{DICOM_STORE_LOCATION}**/datasets/**{HEALTH_CARE_API_DATASET}**/dicomStores/**{DICOM_STORE}**/dicomWeb/</span><span style="color:green">instances</span><span style="color:orange">?downsample=**{DownsamplingFactor}**&</span><span style="color:purple">**{Addtional Parameters}**</span>

<span style="color:blue">(Blue)</span> Standard DICOMweb url path.<br>
<span style="color:green">(Green)</span> Standard DICOMweb study/series/instance path<br>
<span style="color:orange">(Orange)</span> Digital Pathology Proxy Server API<br>
<span style="color:purple">(Purple)</span> Additional DICOMweb query parameters.

**Digital Pathology Proxy Server API parameter:**

* **DownsamplingFactor**: Downsampling factor to apply to instance; undefined = 1.0; no change to instance metadata. (floating point)

**Example:**

https://${DICOM_PROXY_URL}/projects/${PROJECT}/locations/${LOCATION}/datasets/${DATASET}/dicomStores/${DICOMSTORE}/dicomWeb/studies/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368218869027/series/1.3.6.1.4.1.11129.5.7.999.18649109954048068.401.1661368221398029/instances/1.3.6.1.4.1.11129.5.7.0.1.440074136440.11879982.1661368272360294/metadata?downsample=2

**Requirements**:
* Request must return content-type == application/dicom+json
* DICOM instances must be WSI IOD DICOM.
* DICOM metadata describing transformed values must be retrieved.
    * Transforming NumberOfFrames requires retrieval of:
        * NumberOfFrames
        * TotalPixelMatrixColumns
        * TotalPixelMatrixRows
        * Columns
        * Rows

**Returns**

DICOM instance JSON metadata unmodified with exception of:
* TotalPixelMatrixColumns = max(int(source dim / downsampling factor), 1)
* TotalPixelMatrixRows = max(int(source dim / downsampling factor), 1)
* NumberOfFrames = computed requires additional tags
* PixelSpacing = Source instance pixel spacing * downsampling factor


## Proxy

The Digital Pathology Proxy Server will perform a proxy pass through of all other requests to the DICOM store.

**Queries**
**<span style="color:orange">{URL_PATH_PREFIX}</span>**<span style="color:blue">/projects/**{GCP_PROJECT_ID}**/locations/**{DICOM_STORE_LOCATION}**/datasets/**{HEALTH_CARE_API_DATASET}**/dicomStores/**{DICOM_STORE}**/dicomWeb</span><span style="color:green">/studies/**{StudyInstanceUID}**/series/**{SeriesInstanceUID}**/instances</span><span style="color:orange">?downsample=**{DownsamplingFactor}**</span><span style="color:purple">&**{Addtional Parameters}**</span>


## Health check

Send HTTP GET or POST request to Digital Pathology DICOM Proxy Server Root root ‘/’

* **Expected responses**:

  * **Expected response if Proxy server is running**:<br>
  'Digital_Pathology_Proxy_Server-Blueprint-Health-Check'<br>
  Content-type= text/html; charset=utf-8<br>
  Status = 200

  * **If the Proxy server is running and configured (default) to use localhost Redis and Redis is not running.**<br>
    'Error connecting to local Redis instance.'<br>
    Content-type= text/html; charset=utf-8<br>
    Status = 400

If Proxy server is running and configured to use an external Redis store (e.g. managed Memorystore for Redis) then Proxy server health checks will not test the health of the Redis instance.  Redis health should be tested directly. Digital Pathology Proxy Server is robust to intermittent Redis availability. 
