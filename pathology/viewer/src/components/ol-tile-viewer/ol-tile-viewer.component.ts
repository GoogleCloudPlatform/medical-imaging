/**
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {CommonModule} from '@angular/common';
import {ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, OnChanges, Output, ViewChild} from '@angular/core';
import * as ol from 'ol';
import {Feature, ImageTile, ImageWrapper, Map, Tile, View} from 'ol';
import {FullScreen, MousePosition, OverviewMap, ScaleLine} from 'ol/control';
import {defaults as controlDefaults} from 'ol/control/defaults';
import {format as coordinateFormat} from 'ol/coordinate';
import {getCenter} from 'ol/extent';
import {Geometry, SimpleGeometry} from 'ol/geom';
import {DragRotateAndZoom, Draw, Link} from 'ol/interaction';
import ImageLayer from 'ol/layer/Image';
import TileLayer from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';
import {unByKey} from 'ol/Observable';
import {Projection} from 'ol/proj';
import {ImageStatic, TileDebug, Vector, XYZ} from 'ol/source';
import {Circle, Fill, RegularShape, Stroke, Style} from 'ol/style';
import TileGrid from 'ol/tilegrid/TileGrid';
import TileState from 'ol/TileState';
import {catchError, tap} from 'rxjs/operators';

import {environment} from '../../environments/environment';
import {AssociatedImageType} from '../../interfaces/dicom_descriptor';
import {Level, type SlideDescriptor, type SlideInfo} from '../../interfaces/slide_descriptor';
import {CompressionType, IccProfileType} from '../../interfaces/types';
import {DicomwebService} from '../../services/dicomweb.service';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';
import {metersToMagnification} from '../../utils/zoom_name_to_val';

/**
 * Stroke style for measure tool.
 */
export const MEASURE_STROKE_STYLE = new Style({
  fill: new Fill({
    color: 'transparent',
  }),
  stroke: new Stroke({
    color: 'red',
    lineDash: [10, 10],
    width: 2,
  }),
  image: new Circle({
    radius: 5,
    stroke: new Stroke({
      color: 'rgba(0, 0, 0, 0.7)',
    }),
    fill: new Fill({
      color: 'rgba(255, 255, 255, 0.2)',
    }),
  }),
});

/**
 * Stroke style for drawing tool.
 */
export const DRAW_STROKE_STYLE = new Style({
  fill: new Fill({
    color: 'transparent',
  }),
  stroke: new Stroke({
    color: 'yellow',
    width: 2,
  }),
  image: new RegularShape({
    fill: new Fill({
      color: 'transparent',
    }),
    stroke: new Stroke({
      color: 'yellow',
      width: 2,
    }),
    points: 4,
    radius: 5,
    angle: Math.PI / 4,
  }),
});

/**
 * Renders slide/image tiles using open layers and DICOM Proxy.
 */
@Component({
  selector: 'ol-tile-viewer',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './ol-tile-viewer.component.html',
  styleUrl: './ol-tile-viewer.component.scss'
})
export class OlTileViewerComponent implements OnChanges {
  readonly ENABLE_SERVER_INTERPOLATION: boolean =
      environment.ENABLE_SERVER_INTERPOLATION ?? false;

  @Input({required: true}) slideDescriptor!: SlideDescriptor;
  @Input({required: true}) slideInfo?: SlideInfo;
  @Input() isThumbnail = false;
  @Input() isLabelOrOverviewImage = false;

  @Output() readonly olMapLoaded = new EventEmitter<Map>();

  @ViewChild('olMapContainer', {static: true}) olMapContainer!: ElementRef;

  addedInteraction: Draw|null = null;

  caseId = '';
  olMap: Map|null = null;
  isLoaded = false;

  disableContextMenuEdit = false;
  disableContextMenuModifyShapes = false;
  slideOverviewOpened = true;
  showXYZCoordinates = false;

  magnificationLevel = '0';
  // Toggles magnification label based on overview map being open/closed.
  isOverviewMapOpened = false;
  iccProfile: IccProfileType = IccProfileType.NONE;

  constructor(
      private readonly imageViewerPageStore: ImageViewerPageStore,
      private readonly dicomwebService: DicomwebService,
      private readonly olTileViewerElementRef: ElementRef,
      private readonly cdRef: ChangeDetectorRef,
  ) {
    // Add a resize event listener to the window
    window.addEventListener('resize', () => {
      this.updateTextBoxWidth();  // Update width when the window is resized
    });

    this.olMapLoaded.subscribe(() => {
      this.isLoaded = true;
      this.cdRef.detectChanges();
    });

    if (this.ENABLE_SERVER_INTERPOLATION) {
      this.imageViewerPageStore.iccProfile$.subscribe((iccProfile) => {
        this.iccProfile = iccProfile ?? IccProfileType.NONE;
        this.setupViewer();
      });
    }

    this.imageViewerPageStore.showXYZCoordinates$.subscribe(
        (showXYZCoordinates) => {
          this.showXYZCoordinates = showXYZCoordinates;
          this.setupDebugLayerVisibilty();
        });
  }

  ngOnChanges() {
    if (this.slideDescriptor && this.slideInfo) {
      this.setupViewer();
    }
  }

  private setupViewer() {
    if (!this.slideDescriptor || !this.slideInfo) {
      return;
    }

    this.resetDisplay();
    if (!this.slideInfo) return;

    if (this.isLabelOrOverviewImage) {
      this.drawImgLabel(this.slideInfo);
      return;
    }


    if (this.slideInfo.isFlatImage) {
      this.flatImage();
      return;
    }

    if (this.isThumbnail) {
      this.drawImgThumbnail(this.slideInfo);
      return;
    }

    this.initializeOpenLayerSlideViewer(this.slideInfo);
  }

  flatImage() {
    if (!this.slideInfo) return;
    const slideInfo = this.slideInfo;
    // Map views always need a projection.  Here we just want to map image
    // coordinates directly to map coordinates, so we create a projection that
    // uses the image extent in pixels.
    const extent =
        [0, 0, this.slideInfo.size.x ?? 1024, this.slideInfo.size.y ?? 968];

    const projection = new Projection({
      code: 'flat-image',
      units: 'pixels',
      extent,
    });

    const imageLoadFunction =
        async (imageWrapper: ImageWrapper, src: string) => {
      const image = imageWrapper.getImage();
      const slideLevelsByZoom = [...slideInfo.levelMap];
      // OpenLayers most zoomed out layer is 0. Reversed of how levelMap
      // stores it.
      const slideLevel: Level = slideLevelsByZoom[0];

      // Frame number is computed by location at X (x+1 for offset), plus number
      // of tiles in previous rows (y*tilesPerWidth).
      const frame = slideLevel.properties[0].frames;
      const instanceUid = slideLevel?.properties[0].instanceUid ?? '';

      const path = (this.slideDescriptor?.id as string) ?? '';

      this.dicomwebService.getEncodedImageTile(path, instanceUid, frame)
          .subscribe((imageData: string) => {
            const imageUrl = this.imageDataToImageUrl(imageData);
            if (image instanceof HTMLImageElement) {
              image.src = imageUrl;
            }
          });
    };

    const flatImageSourceOptions = {
      url: '',
      projection,
      imageExtent: extent,
      imageLoadFunction,
    };

    const initialZoom = 0;
    // The arbitraryMaxZoom can be set to any value, which determines how far in
    // a flat image can be zoomed.
    const arbitraryMaxZoom = 8;
    const flatImageViewOptions = {
      extent,
      constrainOnlyCenter: true,
      center: getCenter(extent),
      zoom: initialZoom,
      minZoom: initialZoom,
      maxZoom: arbitraryMaxZoom,
      projection,
    };
    const flatImageView = new View(flatImageViewOptions);

    const slideOverviewControl = new OverviewMap({
      collapsed: false,
      view: new View(flatImageViewOptions),
      layers: [
        new ImageLayer({
          source: new ImageStatic(flatImageSourceOptions),
        }),
      ],
    });

    const loadedChangeListener =
        slideOverviewControl.getOverviewMap().on('loadend', () => {
          if (loadedChangeListener) {
            unByKey(loadedChangeListener);
          }
          const [, , x, y] = extent;
          this.setOverviewAspectRatio(`${x}/${y}`);
        });

    const flatImageLayer = new ImageLayer<ImageStatic>({
      source: new ImageStatic(flatImageSourceOptions),
      properties: {
        name: 'flat-image-layer',
        title: this.slideInfo?.slideName ?? '',
      }

    });
    if (this.isThumbnail) {
      flatImageView.setMaxZoom(initialZoom);

      this.olMap = new Map({
        layers: [
          flatImageLayer,
        ],
        controls: [],
        interactions: [],
        target: this.olMapContainer.nativeElement,
        view: flatImageView,
      });


    } else {
      this.olMap = new Map({
        layers: [
          flatImageLayer,
        ],
        controls: [slideOverviewControl],
        target: this.olMapContainer.nativeElement,
        view: flatImageView,
      });
    }

    this.olMap?.once('postrender', (event) => {
      this.olMapLoaded.emit(this.olMap!);
    });
  }

  drawImgLabel(slideInfo: SlideInfo) {
    let associatedThumbnailImage = slideInfo.associatedImages.find(
        (associatedImage) =>
            associatedImage.type === AssociatedImageType.LABEL);
    if (!associatedThumbnailImage) {
      associatedThumbnailImage = slideInfo.associatedImages.find(
          (associatedImage) =>
              associatedImage.type === AssociatedImageType.OVERVIEW);
    }
    if (!associatedThumbnailImage) {
      return;
    }

    const instanceUid = associatedThumbnailImage?.instanceUid ?? '';
    const path = (this.slideDescriptor?.id as string) ?? '';
    if (path && instanceUid) {
      const tempImage = new Image();
      tempImage.className = 'thumbnail-image';

      this.dicomwebService
          .getImageSecondaryCapture(
              path, instanceUid, CompressionType.DEFAULT, this.iccProfile)
          .subscribe((imageData) => {
            tempImage.src = this.imageDataToImageUrl(imageData);
          });

      this.olMapContainer.nativeElement.appendChild(tempImage);
      this.olMapLoaded.emit(this.olMap!);
      return;
    }
  }

  drawImgThumbnail(slideInfo: SlideInfo) {
    const associatedThumbnailImage = slideInfo.associatedImages.find(
        (associatedImage) =>
            associatedImage.type === AssociatedImageType.THUMBNAIL);

    if (!associatedThumbnailImage) {
      this.initializeOpenLayerSlideViewer(slideInfo);
      this.olMapLoaded.emit(this.olMap!);
      return;
    }
    const instanceUid = associatedThumbnailImage?.instanceUid ?? '';
    const path = (this.slideDescriptor?.id as string) ?? '';
    if (path && instanceUid) {
      const tempImage = new Image();
      tempImage.className = 'thumbnail-image';

      this.dicomwebService
          .getImageSecondaryCapture(
              path, instanceUid, CompressionType.DEFAULT, this.iccProfile)
          .subscribe((imageData) => {
            tempImage.src = this.imageDataToImageUrl(imageData);
          });

      this.olMapContainer.nativeElement.appendChild(tempImage);
      this.olMapLoaded.emit(this.olMap!);
      return;
    }
  }

  getAllCoordinates() {
    const allLayers = this.olMap?.getAllLayers() ?? [];
    if (allLayers.length !== 2) return;
    const drawLayer = allLayers.find((layer) => {
      return layer.get('name') === 'draw-layer';
    }) as VectorLayer<Vector<Feature<SimpleGeometry>>>|
        undefined;

    if (!drawLayer) return;

    const coordinates =
        drawLayer!.getSource()?.getFeatures()?.map((feature) => {
          const featureGeometry = feature.getGeometry()!;
          const coordinates = featureGeometry.getCoordinates();

          return coordinates;
        });
    return coordinates;
  }

  initializeOpenLayerSlideViewer(slideInfo: SlideInfo) {
    // Calculate image dimensions and resolutions for each level
    let resolutions: number[] = [];
    let tileSizes: number[] = [];
    slideInfo.levelMap.forEach(level => {
      const levelWidthMeters = level.width * level.pixelWidth! * 1e-3;

      resolutions.push(levelWidthMeters / level.width);
      tileSizes.push(level.tileSize);
    });
    resolutions = resolutions.reverse();
    tileSizes = tileSizes.reverse();

    // Calculate the extent based on the highest resolution level (zoom level 0)
    const baseLevel = slideInfo.levelMap[slideInfo.levelMap.length - 1];
    const size = {
      x: baseLevel.width * baseLevel.pixelWidth! * 1e-3,
      y: baseLevel.height * baseLevel.pixelHeight! * 1e-3,
    };
    const extent = [0, -size.y, size.x, 0];

    // Create the TileGrid
    const tileGrid =
        new TileGrid({extent, resolutions, tileSizes, origin: [0, 0]});

    const tileUrlFunction = () => {
      return '';
    };

    const tileLoadFunction = (imageTile: Tile) => {
      const [z, x, y] = imageTile.tileCoord;
      const slideLevelsByZoom = [...slideInfo.levelMap];
      // OpenLayers most zoomed out layer is 0. Reversed of how levelMap
      // stores it.
      const slideLevel: Level = slideLevelsByZoom.reverse()[z];
      const {tileSize} = slideLevel;

      const tilesPerWidth = Math.ceil(Number(slideLevel?.width) / tileSize);
      const tilesPerHeight = Math.ceil(Number(slideLevel?.height) / tileSize);

      // Frame number is computed by location at X (x+1 for offset), plus number
      // of tiles in previous rows (y*tilesPerWidth).
      const frame = (x + 1) + (tilesPerWidth * y);
      // Out of bound frames
      if (frame > tilesPerWidth * tilesPerHeight) {
        imageTile.setState(TileState.ERROR);
      }

      const instanceUid = slideLevel?.properties[0].instanceUid ?? '';
      const downSampleMultiplier = slideLevel.downSampleMultiplier ?? 0;
      const path = (this.slideDescriptor?.id as string) ?? '';

      const isYOutOfBound = (y + 1) === tilesPerHeight &&
          slideLevel.height % slideLevel.tileSize !== 0;
      const isXOutOfBound = (x + 1) === tilesPerWidth &&
          slideLevel.width % slideLevel.tileSize !== 0;
      this.dicomwebService
          .getEncodedImageTile(
              path, instanceUid, frame, downSampleMultiplier, this.iccProfile)
          .pipe(
              tap((imageData: string) => {
                const imageUrl = this.imageDataToImageUrl(imageData);
                if ((imageTile instanceof ImageTile)) {
                  if (!isYOutOfBound && !isXOutOfBound) {
                    (imageTile.getImage() as HTMLImageElement).src = imageUrl;
                  } else {
                    this.cropTile(
                        slideLevel, imageUrl, imageTile, isXOutOfBound,
                        isYOutOfBound);
                  }
                }
              }),
              catchError((e) => {
                imageTile.setState(TileState.ERROR);
                return e;
              }),
              )
          .subscribe();
    };

    const projection = new Projection({
      code: 'custom-image',
      units: 'm',
      extent,
    });

    const scalelineControl = new ScaleLine({
      units: 'metric',
      bar: true,
      text: true,
      minWidth: 140,
    });
    const slideSource = new XYZ({
      tileGrid,
      tileLoadFunction,
      tileUrlFunction,
      projection,
    });
    // Layer to show the tiles from DICOM Proxy for the whole slide.
    const slideLayer = new TileLayer({
      preload: 3,
      source: slideSource,
      properties: {
        name: 'slide-layer',
        title: this.slideInfo?.slideName ?? '',
      }
    });

    const slideOverviewControl = new OverviewMap({
      collapsed: false,
      view: new ol.View({
        resolutions: [tileGrid.getResolution(0)],
        extent,
        projection,
        maxZoom: 0,
        minZoom: 0,
        zoom: 0,
      }),
      layers: [
        new TileLayer({
          source: slideSource,
        }),
      ],
    });

    // Layer to show grid around the tiles, helpful in debugging.
    const debugLayer = new TileLayer({
      source: new TileDebug({
        tileGrid,
        projection,
      }),
      properties: {name: 'debug-layer'}
    });
    // Layer to show drawings
    const drawSource = new Vector<Feature<Geometry>>({wrapX: false});
    const drawLayer = new VectorLayer({
      source: drawSource,
      style: [DRAW_STROKE_STYLE],
      properties: {
        name: 'draw-layer',
        title: 'Annotations',
      }
    });
    const measureSource = new Vector({
      wrapX: false,
    });
    const measureLayer = new VectorLayer({
      source: measureSource,
      style: [
        MEASURE_STROKE_STYLE,
      ],
      properties: {
        name: 'measure-layer',
      }
    });


    const initialZoom = this.getInitialZoomByContainer(slideInfo.levelMap);

    let olMap: ol.Map|undefined = undefined;

    if (this.isThumbnail) {
      const thumbnailInitialZoom = 0;
      olMap = new ol.Map({
        target: this.olMapContainer.nativeElement,
        layers: [
          slideLayer,
        ],
        controls: [],
        interactions: [],
        view: new ol.View({
          resolutions,
          extent,
          constrainOnlyCenter: true,
          center: getCenter(extent),
          zoom: thumbnailInitialZoom,
          minZoom: thumbnailInitialZoom,
          maxZoom: thumbnailInitialZoom,
          projection,
        })
      });
      return;
    } else {
      olMap = new ol.Map({
        target: this.olMapContainer.nativeElement,
        controls:
            controlDefaults().extend([slideOverviewControl, scalelineControl]),
        layers: [
          slideLayer,
          debugLayer,
          drawLayer,
          measureLayer,
        ],
        view: new ol.View({
          resolutions,
          extent,
          constrainOnlyCenter: true,
          center: getCenter(extent),
          zoom: initialZoom,
          minZoom: 0,
          projection,
        })
      });
    }

    const mousePositionControl = new MousePosition({
      coordinateFormat: (coordinate) => {
        if (!coordinate) return '';
        // Convert meteres to millimetre.
        coordinate[0] = coordinate[0] * 1000;
        coordinate[1] = coordinate[1] * 1000;

        return coordinateFormat(coordinate, '{x}mm, {y}mm', 2);
      },
      projection,
    });

    const linkInteraction =
        new Link({replace: true, params: ['x', 'y', 'z', 'r']});
    olMap.addControl(mousePositionControl);
    olMap.addInteraction(linkInteraction);
    olMap.addInteraction(new DragRotateAndZoom());
    olMap.addControl(new FullScreen());

    this.olMap = olMap;

    this.olMap?.once('postrender', (event) => {
      this.olMapLoaded.emit(this.olMap!);

      const overviewMap = slideOverviewControl.getOverviewMap();
      const loadedChangeListener = overviewMap.on('loadend', (e) => {
        if (loadedChangeListener) {
          unByKey(loadedChangeListener);
        }

        this.setOverviewAspectRatio(
            `${slideInfo.levelMap[0].width}/${slideInfo.levelMap[0].height}`);

        this.handleOverviewMapExpanding(overviewMap);
      });

      let initialZoom = this.olMap?.getView().getResolution();
      if (initialZoom) {
        this.magnificationLevel =
            this.convertDecimalToFraction(metersToMagnification(initialZoom));
        this.cdRef.detectChanges();
      }

      this.olMap?.on('moveend', e => {
        const finalZoom = this.olMap?.getView().getResolution() ?? 0;
        if (initialZoom !== finalZoom || !this.magnificationLevel) {
          // this event has to do with a zoom in - zoom out
          initialZoom = finalZoom;
          this.magnificationLevel =
              this.convertDecimalToFraction(metersToMagnification(finalZoom));
          this.cdRef.detectChanges();
        }
      });
    });
    this.setupDebugLayerVisibilty();
  }

  private setupDebugLayerVisibilty() {
    if (!this.olMap) return;
    const olMapDebugLayer = this.olMap.getAllLayers().find(
        (layer) => layer.get('name') === 'debug-layer');
    olMapDebugLayer?.setVisible(this.showXYZCoordinates);
  }

  private convertDecimalToFraction(decimal: number): string {
    return this.decimalToFraction(decimal);
  }

  private getInitialZoomByContainer(levelMap: Level[]): number {
    const containerHeight = this.olMapContainer.nativeElement.offsetHeight;
    const containerWidth = this.olMapContainer.nativeElement.offsetWidth;
    const slideLevelsByZoom = [...levelMap].reverse();
    let initialZoom = 0;
    for (let i = 0; i < slideLevelsByZoom.length; i++) {
      const zoom = slideLevelsByZoom[i];
      if (zoom.height < containerHeight && zoom.width < containerWidth) {
        initialZoom = i;
      } else {
        break;
      }
    }

    return initialZoom;
  }

  decimalToFraction(decimal: number): string {
    let numerator = decimal;
    let denominator = 1;
    while (numerator % 1 !== 0) {
      numerator *= 10;
      denominator *= 10;
    }
    const gcd = this.greatestCommonDivisor(numerator, denominator);
    numerator /= gcd;
    denominator /= gcd;

    if (denominator === 1) return `${numerator}`;
    return `${numerator}/${denominator}`;
  }

  greatestCommonDivisor(a: number, b: number): number {
    if (b === 0) {
      return a;
    }
    return this.greatestCommonDivisor(b, a % b);
  }


  private cropTile(
      slideLevel: Level, imageData: string, imageTile: ImageTile,
      isXOutOfBound: boolean, isYOutOfBound: boolean): void {
    if (!isYOutOfBound && !isXOutOfBound) return;

    // Only do custom cropping of image if tile is on the edge of the image
    // and goes beyond the image limit. This is slower than the default.
    const newImageEl = new Image();
    newImageEl.crossOrigin = 'anonymous';
    const can = document.createElement('canvas');
    can.width = slideLevel.tileSize;
    can.height = slideLevel.tileSize;
    const ctx = can?.getContext('2d');

    newImageEl.onload = () => {
      // Compute the portion of the tile that is inside the image.
      let computedWidth = slideLevel.width;
      if (isXOutOfBound) {
        computedWidth = slideLevel.width % slideLevel.tileSize;
      }
      let computedHeight = slideLevel.height;
      if (isYOutOfBound) {
        computedHeight = slideLevel.height % slideLevel.tileSize;
      }

      ctx!.drawImage(
          newImageEl, 0, 0, computedWidth, computedHeight, 0, 0, computedWidth,
          computedHeight);

      imageTile.setImage(can);
    };

    newImageEl.src = imageData;
  }

  private handleOverviewMapExpanding(overviewMap: ol.Map) {
    this.updateTextBoxWidth();

    overviewMap.on('change:size', (e) => {
      const expandOverViewmapChevron =
          this.olTileViewerElementRef.nativeElement.querySelector(
              '.ol-overviewmap.ol-unselectable.ol-control');
      if (!expandOverViewmapChevron) return;

      this.isOverviewMapOpened =
          !expandOverViewmapChevron.classList.contains('ol-collapsed');

      if (this.isOverviewMapOpened) {
        this.updateTextBoxWidth();
      }
      this.cdRef.detectChanges();
    });
  }
  private updateTextBoxWidth() {
    const overviewMapElement =
        this.olTileViewerElementRef.nativeElement.querySelector(
            '.ol-overviewmap') as HTMLDivElement;
    const zoomLevelElement =
        this.olTileViewerElementRef.nativeElement.querySelector(
            '.zoom-level-label') as HTMLDivElement;
    if (!zoomLevelElement || !overviewMapElement) return;

    zoomLevelElement.style.width = `${overviewMapElement.offsetWidth - 2}px`;
    const top = `${
        overviewMapElement.clientHeight + overviewMapElement.offsetTop + 5}px`;
    zoomLevelElement.style.top = top;
    zoomLevelElement.style.display = 'grid';
  }

  private resetDisplay() {
    this.olMapContainer.nativeElement.innerHTML = '';
  }

  private setOverviewAspectRatio(ratio = '1/1') {
    const element = this.olTileViewerElementRef.nativeElement.querySelector(
        '.ol-overviewmap');
    if (!element) return;
    (element as HTMLElement).style.aspectRatio = ratio;
  }

  private imageDataToImageUrl(
      imageData: string, compressionType = CompressionType.WEBP) {
    return `data:${compressionType};base64,${imageData}`;
  }
}
