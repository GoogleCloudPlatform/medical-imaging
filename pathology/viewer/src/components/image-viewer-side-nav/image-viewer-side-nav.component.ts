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

import {COMMA, ENTER} from '@angular/cdk/keycodes';
import {Overlay, OverlayRef} from '@angular/cdk/overlay';
import {CommonModule} from '@angular/common';
import {Component, ElementRef, HostListener, OnDestroy, OnInit, TemplateRef, ViewChild, ViewContainerRef} from '@angular/core';
import {FormControl, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {MatAutocompleteModule, MatAutocompleteSelectedEvent} from '@angular/material/autocomplete';
import {MatButtonModule} from '@angular/material/button';
import {MatChipInputEvent, MatChipsModule,} from '@angular/material/chips';
import {MatDialogModule} from '@angular/material/dialog';
import {MatDividerModule} from '@angular/material/divider';
import {MatExpansionModule} from '@angular/material/expansion';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatSelectModule} from '@angular/material/select';
import {MatSnackBar} from '@angular/material/snack-bar';
import {MatTooltipModule} from '@angular/material/tooltip';
import {ActivatedRoute, Router} from '@angular/router';
import * as ol from 'ol';
import {Feature} from 'ol';
import {Coordinate} from 'ol/coordinate';
import {EventsKey} from 'ol/events';
import {Geometry, LineString, Point, Polygon} from 'ol/geom';
import {Type} from 'ol/geom/Geometry';
import {Modify} from 'ol/interaction';
import Draw, {createBox, GeometryFunction} from 'ol/interaction/Draw';
import Select, {SelectEvent} from 'ol/interaction/Select';
import Layer from 'ol/layer/Layer';
import {unByKey} from 'ol/Observable';
import LayerRenderer from 'ol/renderer/Layer';
import source, {Source, Vector} from 'ol/source';
import {getArea, getLength} from 'ol/sphere';
import {Fill, RegularShape, Stroke} from 'ol/style';
import Style, {StyleLike} from 'ol/style/Style';
import {combineLatest, Observable, ReplaySubject} from 'rxjs';
import {catchError, map, startWith, takeUntil, tap} from 'rxjs/operators';

import {ImageViewerPageParams} from '../../app/app.routes';
import {environment} from '../../environments/environment';
import {AnnotationGroup, DicomAnnotation, DicomAnnotationInstance, ReferencedSeries} from '../../interfaces/annotation_descriptor';
import {AssociatedImageType, DicomModel} from '../../interfaces/dicom_descriptor';
import {parseDICOMwebUrl} from '../../interfaces/dicomweb';
import {Case} from '../../interfaces/hierarchy_descriptor';
import {ImageOverlay} from '../../interfaces/image_overlay';
import {SlideDescriptor, SlideExtraMetadata, SlideInfo} from '../../interfaces/slide_descriptor';
import {AnnotationKey, DEFAULT_ANNOTATION_KEY, SideNavLayer} from '../../interfaces/types';
import {CohortService} from '../../services/cohort.service';
import {DialogService} from '../../services/dialog.service';
import {metersToPixelsWithYFlip} from '../../services/dicom-annotations.service';
import {SlideApiService} from '../../services/slide-api.service';
import {UserService} from '../../services/user.service';
import {WindowService} from '../../services/window.service';
import {ImageViewerPageStore} from '../../stores/image-viewer-page.store';
import {areFeaturesCovered, areFeaturesIntersecting, differenceFeatures, getFeatureAnnotationKey, getRightMostCoordinate, unionFeatures} from '../../utils/ol_helper';
import {InspectPageComponent} from '../inspect-page/inspect-page.component';
import {DRAW_STROKE_STYLE, MEASURE_STROKE_STYLE, OlTileViewerComponent} from '../ol-tile-viewer/ol-tile-viewer.component';
import {SlideMetadataComponentComponent} from '../slide-metadata-component/slide-metadata-component.component';

const ANNOTATION_DETAILS_OVERLAY_ID = 'annotationDetailsOverlay';
const CONTEXT_MENU_OVERLAY_ID = 'contextMenuOverlay';
const HELP_TOOLTIP_OVERLAY_ID = 'helpTooltipOverlay';
const MEASURE_TOOLTIP_OVERLAY_ID = 'measureTooltipOverlay';

const DEFAULT_DRAWING_STYLE = new Style({
  fill: new Fill({
    color: 'transparent',
  }),
  stroke: new Stroke({
    color: 'black',
    width: 3,
  }),
});

const SELECT_STYLE = new Style({
  fill: new Fill({
    color: 'transparent',
  }),
  stroke: new Stroke({
    color: 'yellow',
    width: 3,
  }),
  image: new RegularShape({
    fill: new Fill({
      color: 'transparent',
    }),
    stroke: new Stroke({
      color: 'yellow',
      width: 3,
    }),
    points: 4,
    radius: 5,
    angle: Math.PI / 4,
  }),
});

declare interface AnnotationLabel {
  name: string;
  color: string;
}
enum AnnotationOverlayAction {
  NONE,
  DELETE,
  SAVE,
}
enum ViewerMenuAction {
  SELECT,
  MEASURE,
  DRAW_POLYGON,
  DRAW_POINT,
  DRAW_BOX,
}

// If an annotation server is unavailable, the application switches to local annotation mode.
// In this mode, annotations are stored in memory and are not persistent. 
const LOCAL_ANNOTATION_MODE = !environment.ANNOTATIONS_DICOM_STORE_BASE_URL;

/**
 * Sidenav Layout component
 */
@Component({
  selector: 'image-viewer-side-nav',
  standalone: true,
  imports: [
    MatDialogModule, MatIconModule, MatDividerModule, FormsModule,
    ReactiveFormsModule, MatFormFieldModule, MatSelectModule,
    MatAutocompleteModule, MatChipsModule, MatTooltipModule, CommonModule,
    MatExpansionModule, OlTileViewerComponent, MatButtonModule, MatInputModule
  ],
  templateUrl: './image-viewer-side-nav.component.html',
  styleUrl: './image-viewer-side-nav.component.scss'
})
export class ImageViewerSideNavComponent implements OnInit, OnDestroy {
  @ViewChild('context_menu', {static: true})
  contextMenuOverlayTemplate!: ElementRef;
  @ViewChild('deleteAnnotationConfirmationDialogTemplate', {static: true})
  deleteAnnotationConfirmationDialogTemplate!: TemplateRef<{}>;
  @ViewChild('labelInput', {static: true})
  labelInput!: ElementRef<HTMLInputElement>;
  @ViewChild('overlayTemplate', {static: true})
  annotationInfoOverlayTemplate!: ElementRef;
  @ViewChild('quickviewImageDialogTemplate', {static: true})
  quickviewImageDialogTemplate!:
      TemplateRef<{slideDescriptor: SlideDescriptor}>;


  activatedRouteParams: ImageViewerPageParams = {};
  annotationInstances: DicomAnnotationInstance[] = [];
  annotationLabelCtrl = new FormControl('');
  annotationNoteCtrl = new FormControl('');
  annotationOverlayActionCurrent: AnnotationOverlayAction =
      AnnotationOverlayAction.NONE;
  contextMenuOverlay?: ol.Overlay = undefined;
  currentUser = '';
  debugMode = false;
  dicomAnnotationModel?: DicomModel;
  disableAnnotationTools = false;
  disableContextDelete = false;
  disableContextMenuAdoptShapes = false;
  disableContextMenuMergeShapes = false;
  disableLabelInput = true;
  // tslint:disable:no-any
  drawLayer: Layer<Source, LayerRenderer<any>>|undefined = undefined;
  // tslint:enable:no-any
  editModeSelectedAnnotationOverlay = false;
  enableAnnotationWritting = environment.ENABLE_ANNOTATION_WRITING;
  enableAnnotations = environment.ENABLE_ANNOTATIONS;
  enableModifyHandler = false;
  enablePointTool = false;
  enableSplitView = true;
  filteredLabels: Observable<AnnotationLabel[]>;
  hasAnnotationReadAccess = LOCAL_ANNOTATION_MODE;
  hasCaseIdInCohortCases = false;
  hasLabelOrOverviewImage = false;
  imageOverlays: ImageOverlay[] = [];
  initialChange = true;
  isAnnotationOverlayEditMode = false;
  isFlatImage = false;
  isOverlayTemplateFocused = false;
  labelOptions: AnnotationLabel[] = [
    {name: 'Gleason Grade 1', color: this.generateRandomColor()},
  ];
  labels = new Set<AnnotationLabel>();
  // tslint:disable:no-any
  layers: Array<Layer<Source, LayerRenderer<any>>> = [];
  // tslint:enable:no-any
  loadingCohorts = true;
  loadingDicomAnnotations = !LOCAL_ANNOTATION_MODE;
  measureDrawing?: ol.Feature<Geometry>;
  measureDrawingGeometryChangeListener: EventsKey|undefined = undefined;
  measureTooltip?: ol.Overlay = undefined;
  measureTooltipElement?: HTMLDivElement = undefined;
  olMap: ol.Map|undefined = undefined;
  overlayRef?: OverlayRef = undefined;
  private helpTooltipElement?: HTMLDivElement = undefined;
  private helpTooltipOverlay?: ol.Overlay = undefined;
  private readonly destroyed$ = new ReplaySubject<boolean>(1);
  protected activeInteraction: Draw|null = null;
  readonly viewerMenuAction = ViewerMenuAction;
  recentlyAddedFeatures: Array<ol.Feature<Geometry>> = [];
  recentlyRemovedFeatures: Array<ol.Feature<Geometry>> = [];
  selectFeatureAvailable = true;
  selectedAnnotationKey?: AnnotationKey = undefined;
  selectedAnnotationOverlay?: ol.Overlay = undefined;
  selectedAnnotations = new Set<ol.Feature<Geometry>>();
  selectedContextMenuFeatures: Array<ol.Feature<Geometry>> = [];
  selectedExtraMetaData?: SlideExtraMetadata = undefined;
  selectedFeature?: ol.Feature;
  selectedInstanceIds = new Set<string>();
  selectedPathologyCohortCases: Case[] = [];
  selectedSlideInfo?: SlideInfo;
  selectedSplitViewSlideDescriptor?: SlideDescriptor;
  selectedViewerAction: ViewerMenuAction = ViewerMenuAction.SELECT;
  separatorKeysCodes: number[] = [ENTER, COMMA];
  sideNavDrawLayers: SideNavLayer[] = [];
  toggleSideNavLayerStyleByLayer =
      new Map<ol.Feature<Geometry>, Style|StyleLike|undefined>();

  constructor(
      private readonly activatedRoute: ActivatedRoute,
      private readonly cohortService: CohortService,
      private readonly dialogService: DialogService,
      private readonly imageViewerPageStore: ImageViewerPageStore,
      private readonly imageViewerSideNavElementRef: ElementRef,
      private readonly router: Router,
      private readonly slideApiService: SlideApiService,
      private readonly userService: UserService,
      private readonly windowService: WindowService,
      public overlay: Overlay,
      public viewContainerRef: ViewContainerRef,
      private readonly snackBar: MatSnackBar,
  ) {
    this.filteredLabels = this.annotationLabelCtrl.valueChanges.pipe(
        startWith(null),
        map((label: string|null) =>
                ((label && typeof label === 'string') ?
                     this.filterLabelOptions(label) :
                     this.labelOptions.slice())),
    );

    this.setupannotationInstances();
  }

  goToCase(chosenCase: Case) {
    const newViewerQueryParams: ImageViewerPageParams = {
      series: chosenCase.slides[0].dicomUri,
      cohortName: this.activatedRouteParams.cohortName,
    };

    this.router.navigate(['/viewer'], {
      queryParams: newViewerQueryParams,
    });
  }

  private setupQueryParams() {
    this.activatedRoute.queryParams.subscribe((params) => {
      this.activatedRouteParams = params;
    });
  }

  private setupSelectedCohortCases() {
    this.cohortService.selectedPathologyCohortCases$
        .pipe(tap((selectedPathologyCohortCases) => {
          this.selectedPathologyCohortCases = selectedPathologyCohortCases;
          this.hasCaseIdInCohortCases =
              this.selectedPathologyCohortCases.some((a: Case) => {
                return a.accessionNumber === this.selectedExtraMetaData?.caseId;
              });
        }))
        .subscribe();
  }
  private setupannotationInstances() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.annotationInstancesBySlideDescriptorId$,
      this.imageViewerPageStore.selectedInstanceIdsBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  annotationInstancesBySlideDescriptorId,
                ]) => {
              if (!selectedSplitViewSlideDescriptor) return;
              const selectedSplitViewSlideDescriptorId =
                  selectedSplitViewSlideDescriptor.id as string;
              if (!selectedSplitViewSlideDescriptorId) return;
              const annotationInstances =
                  annotationInstancesBySlideDescriptorId.get(
                      selectedSplitViewSlideDescriptorId);

              this.annotationInstances = annotationInstances ?? [];
            }),
            )
        .subscribe();

    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.selectedInstanceIdsBySlideDescriptorId$
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  selectedInstanceIdsBySlideDescriptorId,
                ]) => {
              const selectedInstanceIds =
                  selectedInstanceIdsBySlideDescriptorId.get(
                      selectedSplitViewSlideDescriptor?.id as string) ??
                  new Set<string>();

              this.selectedInstanceIds = selectedInstanceIds;
            }),
            )
        .subscribe();

    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.sideNavLayersBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  sideNavLayersBySlideDescriptorId
                ]) => {
              this.sideNavDrawLayers =
                  sideNavLayersBySlideDescriptorId.get(
                      selectedSplitViewSlideDescriptor?.id as string) ??
                  [];
            }),
            )
        .subscribe();

    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.olMapBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  olMapBySlideDescriptorId,
                ]) => {
              const olMap = olMapBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor?.id as string);

              this.olMap = olMap;
              if (!olMap) return;

              this.handleOlMapChanged(olMap);
            }),
            )
        .subscribe();
  }

  handleOlMapChanged(olMap: ol.Map) {
    this.reselectTools();
    this.addOverlaysAndClickHandler(olMap);
    this.validateFlatImage(olMap);
  }

  private validateFlatImage(olMap: ol.Map) {
    const flatImageLayer = olMap.getAllLayers().find(
        (layer) => layer.get('name') === 'flat-image-layer');

    if (flatImageLayer) {
      this.enableAnnotations = false;
      this.isFlatImage = true;
    }
  }

  private addOverlaysAndClickHandler(olMap: ol.Map) {
    if (this.contextMenuOverlay) {
      olMap.addOverlay(this.contextMenuOverlay);
    }
    if (this.selectedAnnotationOverlay) {
      olMap.addOverlay(this.selectedAnnotationOverlay);
    }
    if (this.enableAnnotationWritting) {
      olMap.getViewport().addEventListener('contextmenu', (evt) => {
        if (!this.contextMenuOverlay) return;
        this.contextMenuOpened(evt);
      });
    }
    olMap.on('click', () => {
      this.selectedAnnotationOverlay?.setPosition(undefined);
      this.contextMenuOverlay?.setPosition(undefined);
    });
  }

  private reselectTools() {
    if (this.selectedViewerAction === ViewerMenuAction.SELECT) {
      this.onSelectTool();
    }
    if (this.selectedViewerAction === ViewerMenuAction.DRAW_POLYGON) {
      this.onDrawPolygonsTool(ViewerMenuAction.DRAW_POLYGON);
    }
  }

  @HostListener('document:keyup', ['$event'])
  handleKeyboardEvent(event: KeyboardEvent) {
    const pressedKey = event.key;
    if (pressedKey === 'Backspace' && !this.editModeSelectedAnnotationOverlay) {
      if (this.selectedFeature) {
        this.deleteSelectedAnnotation();
      }
    }

    const overLayPosition = this.selectedAnnotationOverlay?.getPosition();
    if (this.isOverlayTemplateFocused || overLayPosition) {
      return;
    }
    const olMap = this.olMap;
    if (!olMap) return;


    if (pressedKey === '+') {
      const currentZoom = olMap.getView().getZoom() ?? 0;
      olMap.getView().setZoom(currentZoom + 1);
    }
    if (pressedKey === '-') {
      const currentZoom = olMap.getView().getZoom() ?? 0;
      olMap.getView().setZoom(currentZoom - 1);
    }
    const pressedKeyLowerCase = pressedKey.toLowerCase();
    const isSelect = pressedKeyLowerCase === 's';
    const isMeasure = pressedKeyLowerCase === 'm';
    const isDraw = pressedKeyLowerCase === 'd';
    const isRectangle = pressedKeyLowerCase === 'r';
    const isPoint = pressedKeyLowerCase === 'p';


    if (environment.ENABLE_ANNOTATIONS) {
      if (isDraw) {
        this.onDrawPolygonsTool(ViewerMenuAction.DRAW_POLYGON);
      }
      if (isRectangle) {
        this.onDrawPolygonsTool(ViewerMenuAction.DRAW_BOX);
      }
      if (isPoint) {
        this.onDrawPolygonsTool(ViewerMenuAction.DRAW_POINT);
      }
      if (isMeasure) {
        this.onMeasureTool();
      }
    }

    if (isSelect) {
      this.onSelectTool();
    }
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  ngOnInit() {
    this.setupCurrentUser();
    this.setupSelectedSplitViewSlideDescriptor();
    this.setupSlideMetaData();
    this.setupSlideInfo();
    // Default initial pan tool
    this.onSelectTool();
    // Setup overlay for displaying details for selected annotation.
    this.setupOverlays();
    this.setupLoading();
    if (!LOCAL_ANNOTATION_MODE) {
      this.setupAnnotationAccess();
    }
    this.setupSelectedCohortCases();
    this.setupQueryParams();
    this.disableAnnotationsInMultiscreen();
  }

  private setupAnnotationAccess() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.hasAnnotationReadAccessBySlideDescriptorId$,
    ])
        .pipe(
            takeUntil(this.destroyed$),
            tap(([
                  selectedSplitViewSlideDescriptor,
                  hasAnnotationReadAccessBySlideDescriptorId
                ]) => {
              const selectedSplitViewSlideDescriptorId =
                  selectedSplitViewSlideDescriptor?.id as string ?? '';
              this.hasAnnotationReadAccess =
                  hasAnnotationReadAccessBySlideDescriptorId.get(
                      selectedSplitViewSlideDescriptorId) ??
                  false;
              this.disableAnnotationTools = !this.hasAnnotationReadAccess;
            }),
            )
        .subscribe();
  }

  private setupLoading() {
    this.cohortService.loading$.subscribe((loading) => {
      this.loadingCohorts = loading;
    });
    if (!LOCAL_ANNOTATION_MODE) {
      this.imageViewerPageStore.loadingDicomAnnotations$
          .pipe(tap((loading: boolean) => {
            this.loadingDicomAnnotations = loading;
          }))
          .subscribe();
    }
  }
  private setupOverlays() {
    this.selectedAnnotationOverlay = new ol.Overlay({
      id: ANNOTATION_DETAILS_OVERLAY_ID,
      element: this.annotationInfoOverlayTemplate.nativeElement,
      autoPan: {
        animation: {
          duration: 250,
        },
      },
    });
    this.contextMenuOverlay = new ol.Overlay({
      id: CONTEXT_MENU_OVERLAY_ID,
      element: this.contextMenuOverlayTemplate.nativeElement,
      autoPan: {
        animation: {
          duration: 250,
        },
      },
    });
  }
  private setupSelectedSplitViewSlideDescriptor() {
    this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.subscribe(
        (selectedSplitViewSlideDescriptor) => {
          this.selectedSplitViewSlideDescriptor =
              selectedSplitViewSlideDescriptor;
        });
  }

  private disableAnnotationsInMultiscreen() {
    this.imageViewerPageStore.multiViewScreens$.subscribe(
        (multiViewScreens) => {
          this.onSelectTool();  // exit any annotation mode when changing config
          if (multiViewScreens > 1) {
            // no annotation in multiview.
            this.enableAnnotationWritting = false;
            this.enableAnnotations = false;
          } else {
            this.enableAnnotationWritting = environment.ENABLE_ANNOTATION_WRITING;
            this.enableAnnotations = environment.ENABLE_ANNOTATIONS;
          }
        });
  }
  private setupCurrentUser() {
    (environment.ANNOTATION_HASH_STORED_USER_EMAIL ?
         this.userService.getCurrentUserHash$() :
         this.userService.getCurrentUser$())
        .subscribe((currentUser) => {
          this.currentUser = currentUser || '';
          DEFAULT_ANNOTATION_KEY.annotatorId = this.currentUser;
        });
  }

  addLabel(event: MatChipInputEvent): void {
    const value = (event.value || '').trim();

    // Add our label
    if (value) {
      this.labels.add({name: value, color: this.generateRandomColor()});
    }

    // Clear the input value
    event.chipInput.clear();

    this.annotationLabelCtrl.setValue(null);
  }

  adoptBorderAnnotations() {
    const olMap = this.olMap;

    if (!olMap) return;
    const features = this.getSelectedFeatures(olMap);
    // Add merged polygon
    if (features.length < 2) return;
    differenceFeatures(features[0], features[1], olMap);

    this.saveAnnotation();
    this.selectedAnnotationOverlay?.setPosition(undefined);
    this.contextMenuOverlay?.setPosition(undefined);
  }

  annotationSelected(selectEvent: SelectEvent) {
    if (!selectEvent.target) return;
    const selectInteraction: Select = selectEvent.target as Select;

    // Based on number of annotations selected show/hide annotation details
    // dialog and updated selectedAnnotations.
    const selectedFeatures = selectInteraction.getFeatures().getArray();
    if (!selectedFeatures.length) {
      this.selectedAnnotationOverlay?.setPosition(undefined);
      this.selectedAnnotations.clear();
      return;
    }
    const olMap = this.olMap;
    if (olMap) {
      this.imageViewerPageStore.cleanOldModifyInteractions(olMap);
    }

    if (selectedFeatures.length === 1) {
      const singleSelectedFeature = selectedFeatures[0];
      const annotationKey = getFeatureAnnotationKey(singleSelectedFeature);
      if (annotationKey.annotatorId === this.currentUser &&
          this.enableModifyHandler) {
        this.setupModifyHandler(selectInteraction);
      }
      this.selectFeature(singleSelectedFeature);
    }
    this.selectedAnnotations =
        new Set(selectInteraction.getFeatures().getArray());
  }


  contextMenuEditAnnotation() {
    const olMap = this.olMap;
    if (!olMap) return;

    const select = olMap.getInteractions().getArray().filter((interaction) => {
      return interaction instanceof Select;
    })[0] as Select;

    if (!select) return;

    const selectedFeatures = select.getFeatures().getArray();
    if (selectedFeatures.length === 1) {
      const selectedFeature: ol.Feature<Geometry> = selectedFeatures[0];
      this.contextMenuOverlay?.setPosition(undefined);
      this.openSelectedAnnotationOverlay(selectedFeature);
    }
  }

  contextMenuOpened(evt: MouseEvent) {
    const olMap = this.olMap;
    if (!olMap) return;

    const coordinate = olMap.getEventCoordinate(evt);
    if (!coordinate) return;
    const drawLayer = olMap.getAllLayers().find((layer) => {
      return layer.get('name') === 'draw-layer';
    });
    const drawSource: Vector<Feature<Geometry>>|undefined =
        drawLayer?.getSource() as Vector<Feature<Geometry>>;
    if (!drawSource) return;

    const featuresAtCoordinate = drawSource.getFeaturesAtCoordinate(coordinate);

    let select = olMap.getInteractions().getArray().filter((interaction) => {
      return interaction instanceof Select;
    })[0] as Select;
    if (!select) {
      select = new Select();
      olMap.addInteraction(select);
    }

    const selectedCollection = select.getFeatures();

    if (selectedCollection.getArray().length === 0) {
      selectedCollection.extend(featuresAtCoordinate);
    }
    if (selectedCollection.getArray().length === 1 &&
        featuresAtCoordinate.length) {
      selectedCollection.clear();
      selectedCollection.extend(featuresAtCoordinate);
    }

    this.disableContextDelete = true;
    this.disableContextMenuAdoptShapes = true;
    this.disableContextMenuMergeShapes = true;

    const features = this.getSelectedFeatures(olMap);
    this.selectedContextMenuFeatures = features ?? [];

    const allFeaturesByCurrentUser = features.every((feature) => {
      return getFeatureAnnotationKey(feature).annotatorId === this.currentUser;
    });
    if (allFeaturesByCurrentUser) {
      this.disableContextDelete = false;
    }

    this.contextMenuOverlay?.setPosition(coordinate);
    if (features.length < 2) {
      this.disableContextMenuAdoptShapes = true;
      this.disableContextMenuMergeShapes = true;
      return;
    }

    const feature1 = features[0];
    const feature2 = features[1];
    const featuresIntersecting: boolean =
        areFeaturesIntersecting(feature1, feature2, olMap);
    const featuresCovered: boolean =
        areFeaturesCovered(feature1, feature2, olMap);

    if (!featuresIntersecting) {
      this.disableContextMenuAdoptShapes = true;
      this.disableContextMenuMergeShapes = true;
      return;
    }

    if (features.length >= 2) {
      if (allFeaturesByCurrentUser) {
        this.disableContextMenuAdoptShapes = false;
        this.disableContextMenuMergeShapes = false;
      } else {
        const feature1AnnotationKey = getFeatureAnnotationKey(feature1);
        if (feature1AnnotationKey.annotatorId === this.currentUser) {
          this.disableContextMenuAdoptShapes = false;
          this.disableContextMenuMergeShapes = true;
        }
      }

      if (featuresCovered) {
        this.disableContextMenuAdoptShapes = true;
      }
      return;
    }
  }

  getSelectedSlideId() {
    return this.imageViewerPageStore.selectedSplitViewSlideDescriptor$.value
               ?.id as string;
  }

  getSlideInfo(): SlideInfo|undefined {
    const selectedSplitViewSlideDescriptorId = this.getSelectedSlideId();
    if (!selectedSplitViewSlideDescriptorId) return;
    return this.imageViewerPageStore.slideInfoBySlideDescriptorId$.value.get(
        selectedSplitViewSlideDescriptorId);
  }

  createOrUpdateAnnotation() {
    const slideInfo = this.getSlideInfo();
    if (!slideInfo) return;

    const sopInstanceUid = slideInfo.levelMap[0].properties[0].instanceUid;

    if (!sopInstanceUid) {
      return;
    }
    const annotations: SideNavLayer[] = [...this.sideNavDrawLayers];
    const annotationGroups =
        annotations
            .filter((annotation) => {
              return annotation.annotatorId === this.currentUser;
            })
            .map((annotation, index) => {
              const annotationKey = getFeatureAnnotationKey(annotation.feature);
              annotationKey.notes = annotationKey?.notes?.trim() ?? '';
              const coordinates =
                  ((((annotation.feature as Feature<Polygon>).getGeometry())
                        ?.getCoordinates() ??
                    [])[0] as Coordinate[]);
              // Set label to be the first 3 words of the comment otherwise ROI.
              const annotationLabel = (annotationKey.notes ?? '')
                                          .split(' ')
                                          .slice(0, 3)
                                          .join(' ')
                                          .substring(0, 15) ||
                  'ROI';

              const annotationGroup: AnnotationGroup = {
                idNumber: index + 1,
                annotationGroupUid: '',
                annotationGroupLabel: annotationLabel,
                annotationGroupDescription: annotationKey.notes ?? '',
                annotationGroupGenerationType: 'MANUAL',
                annotationPropertyCategoryCodeSequence: [],
                graphicType: 'POLYGON',
                pointCoordinatesData: coordinates,
                longPrimitivePointIndexList: [1],  // DICOM indices are 1 based
              };
              return annotationGroup;
            });

    const seriesId = this.selectedSplitViewSlideDescriptor?.id as string;

    const slidePath = parseDICOMwebUrl(seriesId).path;
    if (!slidePath.studyUID || !slidePath.seriesUID) return;

    // TODO: Move this to annotations store.
    const referencedSeries: ReferencedSeries =
        this.annotationInstances
            .find(
                (annotationInstance) =>
                    annotationInstance.annotatorId === this.currentUser &&
                    annotationInstance.referencedSeries.seriesInstanceUid ===
                        slidePath.seriesUID)
            ?.referencedSeries ??
        // If not found, create an ReferencedSeries based on slideInfo.
        {
          seriesInstanceUid: slidePath.seriesUID,
          referencedImage: {
            sopClassUid: slideInfo.sopClassUid,
            sopInstanceUid: slideInfo.levelMap[0].properties[0].instanceUid,
            dicomModel: slideInfo.levelMap[0].dicomModel,
            pixelSize: {
              width: slideInfo.levelMap[0].pixelWidth,
              height: slideInfo.levelMap[0].pixelHeight
            },
          }
        } as ReferencedSeries;

    const dicomAnnotation: DicomAnnotation = {
      instance: {
        path: {
          studyUID: slidePath.studyUID,
        },
        annotatorId: this.currentUser,
        referencedSeries,
      },
      annotationCoordinateType: '2D',
      annotationGroupSequence: annotationGroups,
    };

    this.imageViewerPageStore
        .createOrModifyDicomAnnotation(
            this.getSelectedSlideId(),
            dicomAnnotation,
            )
        .pipe(
            catchError((error) => {
              this.snackBar.open('Failed to write annotation', 'Dismiss', {
                duration: 3000,
              });
              // Remove recently added feature.
              this.handleRecentFeature();
              return error;
            }),
            )
        .subscribe();
  }

  private handleRecentFeature() {
    const olMap = this.olMap;
    if (!olMap) return;
    const drawLayer = olMap.getAllLayers().find((layer) => {
      return layer.get('name') === 'draw-layer';
    });
    if (!drawLayer) return;
    const drawSource = drawLayer.getSource() as Vector<Feature<Geometry>>;
    // remove recently added features.
    this.recentlyAddedFeatures.forEach((feature) => {
      drawSource.removeFeature(feature);
    });
    // add recently removed features.
    drawSource.addFeatures(this.recentlyRemovedFeatures);
    // reset any open overlays.
    this.selectedAnnotationOverlay?.setPosition(undefined);
    this.contextMenuOverlay?.setPosition(undefined);
  }

  deleteAnnotations(features: Array<ol.Feature<Geometry>>) {
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };
    return this.dialogService
        .openComponentDialog(this.deleteAnnotationConfirmationDialogTemplate, {
          ...DIALOG_CONFIG,
        })
        .afterClosed()
        .subscribe((confirm: boolean) => {
          if (confirm) {
            this.confirmedRemoveAnnotation(features);
          }
        });
  }

  deleteSelectedAnnotation() {
    const olMap = this.olMap;
    if (!olMap) return;
    const select = olMap.getInteractions().getArray().filter((interaction) => {
      return interaction instanceof Select;
    })[0] as Select;
    if (!select) {
      return;
    }

    const features = select.getFeatures().getArray();
    this.deleteAnnotations(features);
  }

  deleteSingleAnnotation(sideNavDrawLayer: SideNavLayer) {
    const featuresToDelete = [sideNavDrawLayer.feature];
    this.deleteAnnotations(featuresToDelete);
  }

  downloadAnnotations() {
    const downloadContent = this.sideNavDrawLayers.map((a) => {
      const feature = a.feature.getGeometry() as Polygon;
      const slideInfo = this.getSlideInfo();
      if (!slideInfo) return;
      const pixelSize = {
        width: slideInfo.levelMap[0].pixelWidth ?? 1,
        height: slideInfo.levelMap[0].pixelHeight ?? 1,
      };
      return {
        coordinates:
            (feature?.getCoordinates() ?? [])
                .map(
                    olCoordArray => metersToPixelsWithYFlip(
                        olCoordArray.map(olCoord => [olCoord[0], olCoord[1]]),
                        pixelSize)),
        annotationType: feature?.getType() ?? 'Invalid',
        annotatorId: a.annotatorId,
        names: a.names,
        notes: a.notes,
        pixelSize,
        sopInstanceUid: slideInfo.levelMap[0].properties[0].instanceUid,
      };
    });

    const sJson = JSON.stringify(downloadContent);
    const element = document.createElement('a');
    const fileName = this.getSelectedSlideId()
                         .replace(/.*studies/, 'studies')
                         .replace(/\//g, '_') +
        '-annotations';
    element.setAttribute(
        'href', 'data:text/json;charset=UTF-8,' + encodeURIComponent(sJson));
    element.setAttribute('download', fileName + '.json');
    element.style.display = 'none';
    document.body.appendChild(element);
    element.click();  // simulate click
    document.body.removeChild(element);
  }

  formatArea(polygon: Geometry|undefined) {
    if (!polygon) return;
    const area = getArea(polygon);            // meter^2
    let output = area * 1e6;                  // mm^2
    output = Math.round(output * 100) / 100;  // round to nearest 100rth
    return String(output);
  }

  formatLength(line: LineString|Polygon|Geometry|undefined) {
    if (!line) return;
    const length = getLength(line);           // in meters
    let output = length * 1000;               // in mm
    output = Math.round(output * 100) / 100;  // round to nearest 100rth
    return String(output);
  }

  getSelectedFeatures(olMap: ol.Map) {
    if (!olMap) return [];

    const selectInteraction =
        olMap.getInteractions().getArray().filter((interaction) => {
          return interaction instanceof Select;
        })[0] as Select;

    const featureCollection = selectInteraction.getFeatures();
    const features = [...new Set([...featureCollection.getArray()])];
    return features;
  }

  mergeAnnotations() {
    const olMap = this.olMap;
    if (!olMap) return;
    const features = this.getSelectedFeatures(olMap);
    // Add merged polygon
    if (features.length < 2) return;
    unionFeatures(features[0], features[1], olMap);

    this.saveAnnotation();
    this.selectedAnnotationOverlay?.setPosition(undefined);
    this.contextMenuOverlay?.setPosition(undefined);
  }

  onDrawPolygonsTool(action: ViewerMenuAction) {
    const olMap = this.olMap;
    if (!olMap) return;
    const drawLayer = olMap.getAllLayers().find((layer) => {
      return layer.get('name') === 'draw-layer';
    });
    if (!drawLayer) return;
    const drawSource = drawLayer.getSource() as Vector<Feature<Geometry>>;


    const currentUserAnnotationInstance =
        this.annotationInstances.find((annotationInstance) => {
          return annotationInstance.annotatorId === this.currentUser;
        });
    if (currentUserAnnotationInstance &&
        !this.selectedInstanceIds.has(
            currentUserAnnotationInstance.path.instanceUID ?? '')) {
      this.toggleAnnotationInstancesSelected(currentUserAnnotationInstance);
    }


    this.resetOtherTools();
    this.selectedViewerAction = ViewerMenuAction.DRAW_POLYGON;
    olMap.getTargetElement().style.cursor = 'cell';

    let drawType: Type = 'Polygon';
    let geometryFunction: GeometryFunction|undefined = undefined;

    if (action === ViewerMenuAction.DRAW_POINT) {
      this.selectedViewerAction = ViewerMenuAction.DRAW_POINT;
      drawType = 'Point';
    }
    if (action === ViewerMenuAction.DRAW_BOX) {
      this.selectedViewerAction = ViewerMenuAction.DRAW_BOX;
      drawType = 'Circle';
      geometryFunction = createBox();
    }
    // Draw.

    const drawInteraction = new Draw({
      source: drawSource,
      type: drawType,
      freehand: true,
      style: [DRAW_STROKE_STYLE],
      geometryFunction,
    });

    this.measurePointerMessageHandler();
    olMap.addInteraction(drawInteraction);

    drawInteraction.on('drawstart', () => {
      this.resetHelpTooltipOverlay();
    });
    drawInteraction.on('drawend', (event) => {
      const olMap = this.olMap;
      if (!olMap) return;

      const feature: ol.Feature<Geometry> = event.feature;
      let maxIndex = 0;
      drawSource.getFeatures().forEach((feature) => {
        const featureAnnotationKey = getFeatureAnnotationKey(feature);
        maxIndex = Math.max(maxIndex, featureAnnotationKey.index);
      });

      feature.setId(JSON.stringify({
        ...DEFAULT_ANNOTATION_KEY,
        names: `ROI`,
        index: maxIndex + 1,
      }));
      drawSource.addFeature(feature);
      this.recentlyAddedFeatures = [feature];

      if (action !== ViewerMenuAction.DRAW_POINT) {
        feature.setStyle(DEFAULT_DRAWING_STYLE);
      }

      // Avoid race condition with view selection.
      setTimeout(() => {
        this.saveAnnotation();
        this.selectFeature(feature, /*isNewAnnotation*/ true);
      });
    });
  }

  onMeasureTool() {
    const olMap = this.olMap;
    if (!olMap) return;

    this.resetOtherTools();
    this.selectedViewerAction = ViewerMenuAction.MEASURE;
    olMap.getTargetElement().style.cursor = 'cell';
    this.measurePointerMessageHandler();

    const value = 'LineString';

    const measureLayer = olMap.getAllLayers().find(
        (layer) => layer.get('name') === 'measure-layer');
    if (!measureLayer) return;

    const measureSource =
        measureLayer.getProperties()['source'] as Vector<Feature<Geometry>>;
    if (!measureSource) return;

    const measureInteraction = new Draw({
      source: measureSource,
      type: value,
      freehand: false,
      maxPoints: 2,
      style: MEASURE_STROKE_STYLE,
    });

    this.activeInteraction = measureInteraction;
    olMap.addInteraction(this.activeInteraction);
    this.createMeasureTooltip();

    // Set sketch
    measureInteraction.on('drawstart', (evt) => {
      // set sketch
      this.measureDrawing = evt.feature;

      const featureGeometry = this.measureDrawing.getGeometry();
      let tooltipCoord: Coordinate|undefined = undefined;

      this.measureDrawingGeometryChangeListener =
          this.measureDrawing?.getGeometry()?.on('change', () => {
            if (featureGeometry instanceof Polygon) {
              tooltipCoord =
                  featureGeometry.getInteriorPoint().getCoordinates();
            }
            if (featureGeometry instanceof LineString) {
              tooltipCoord = featureGeometry.getLastCoordinate();
            }

            // Set tooltip content
            let output;

            if (featureGeometry instanceof Polygon) {
              output = this.formatArea(featureGeometry);
            } else if (featureGeometry instanceof LineString) {
              output = this.formatLength(featureGeometry);
            }

            if (!this.measureTooltipElement || !output) {
              return;
            }

            this.windowService.safelySetInnerHtml(
                this.measureTooltipElement, `${output} mm`);


            this.measureTooltip?.setPosition(tooltipCoord);
          });
    });

    measureInteraction.on('drawend', (event) => {
      if (!this.measureTooltipElement) {
        return;
      }
      this.measureTooltipElement.className = 'ol-tooltip ol-tooltip-static';
      this.measureTooltip?.setOffset([0, -7]);

      this.measureDrawing = undefined;
      // unset tooltip so that a new one can be created
      this.measureTooltipElement = undefined;
      this.createMeasureTooltip();
      if (this.measureDrawingGeometryChangeListener) {
        unByKey(this.measureDrawingGeometryChangeListener);
      }


      const feature: ol.Feature<Geometry> = event.feature;
      let maxIndex = 0;
      if (this.drawLayer) {
        const drawSource =
            this.drawLayer.getSource() as source.Vector<Feature<Geometry>>;
        drawSource.getFeatures().forEach((feature) => {
          const featureAnnotationKey = getFeatureAnnotationKey(feature);
          maxIndex = Math.max(maxIndex, featureAnnotationKey.index);
        });
      }
      measureSource.getFeatures().forEach((feature) => {
        const featureAnnotationKey = getFeatureAnnotationKey(feature);
        maxIndex = Math.max(maxIndex, featureAnnotationKey.index);
      });

      feature.setId(JSON.stringify({
        ...DEFAULT_ANNOTATION_KEY,
        names: `MEASURE`,
        index: maxIndex + 1,
      }));
      measureSource.addFeature(feature);
    });
  }

  onSelectTool() {
    const olMap = this.olMap;
    if (!olMap) return;

    this.resetOtherTools();
    this.selectedViewerAction = ViewerMenuAction.SELECT;


    olMap.getTargetElement().style.cursor = 'pointer';

    this.setupSelectHandler(olMap);
  }

  openSlideData() {
    const DIALOG_CONFIG = {
      autoFocus: false,
      disableClose: false,
    };

    this.dialogService.openComponentDialog(InspectPageComponent, {
      ...DIALOG_CONFIG,
      panelClass: 'slide-data-dialog-panel',
    });
  }

  openSlideDetailsDialog() {
    const olMap = this.olMap;

    if (!this.selectedSplitViewSlideDescriptor || !this.selectedSlideInfo) {
      return;
    }
    const currentViewLevel = [
      ...this.selectedSlideInfo.levelMap
    ].reverse()[Math.floor(olMap?.getView().getZoom() ?? 0)];

    const currentViewLevelInstanceUid =
        currentViewLevel.properties[0].instanceUid;
    const currentLevelInstanceId =
        `${this.selectedSplitViewSlideDescriptor?.id as string}/instances/${
            currentViewLevelInstanceUid}`;

    this.slideApiService.getSlideExtraMetadata(currentLevelInstanceId)
        .subscribe((metaData) => {
          const metaDataDialog = this.dialogService.openComponentDialog<
              SlideMetadataComponentComponent, SlideExtraMetadata>(
              SlideMetadataComponentComponent, {
                data: metaData,
                autoFocus: false,
                disableClose: false,
              });

          metaDataDialog.afterClosed().subscribe((closeResponse) => {
            if (closeResponse?.openSlideData) {
              this.openSlideData();
            }
          });
        });
  }

  private confirmedRemoveAnnotation(features: Array<ol.Feature<Geometry>>) {
    this.annotationOverlayActionCurrent = AnnotationOverlayAction.DELETE;
    const currentUserFeatures =
        this.setupRemoveAnnotationAndGetCurrentFeatures(features) ?? [];

    if (currentUserFeatures.length) {
      this.createOrUpdateAnnotation();
      return;
    }

    const currentUserInstance = this.annotationInstances.find(
        (({annotatorId}) => annotatorId === this.currentUser));
    if (!currentUserInstance?.path.instanceUID) return;

    if (!LOCAL_ANNOTATION_MODE) {
      this.imageViewerPageStore.deleteDicomAnnotationsPath(currentUserInstance)
          .subscribe();
    }
  }

  private setupRemoveAnnotationAndGetCurrentFeatures(
      features: Array<ol.Feature<Geometry>>) {
    // remove feature from drawSource
    const olMap = this.olMap;
    if (!olMap) return;
    const drawLayer = olMap.getAllLayers().find((layer) => {
      return layer.get('name') === 'draw-layer';
    });
    if (!drawLayer) return;
    const drawSource =
        drawLayer.getSource() as source.Vector<Feature<Geometry>>;
    this.recentlyRemovedFeatures = [...features];
    features.forEach((feature) => {
      drawSource?.removeFeature(feature);
    });
    this.selectedFeature = undefined;

    // close open overlays
    this.selectedAnnotationOverlay?.setPosition(undefined);
    this.contextMenuOverlay?.setPosition(undefined);

    // get current user features
    const currentFeatures = drawSource?.getFeatures() ?? [];
    const currentUserFeatures = currentFeatures.filter((feature) => {
      const featureId = feature.getId() as string;
      if (!featureId) return false;

      const annotationKey: AnnotationKey = getFeatureAnnotationKey(feature);
      return annotationKey.annotatorId === this.currentUser;
    });

    return currentUserFeatures;
  }

  private createHelpTooltip() {
    const olMap = this.olMap;
    if (!olMap) return;

    if (this.helpTooltipElement) {
      this.helpTooltipElement.parentNode?.removeChild(this.helpTooltipElement);
    }
    this.helpTooltipElement = document.createElement('div');
    this.helpTooltipElement.className = 'ol-tooltip hidden';
    this.helpTooltipOverlay = new ol.Overlay({
      id: HELP_TOOLTIP_OVERLAY_ID,
      element: this.helpTooltipElement,
      offset: [15, 0],
      positioning: 'center-left',
    });
    olMap.addOverlay(this.helpTooltipOverlay);
  }

  private createMeasureTooltip() {
    const olMap = this.olMap;
    if (!olMap) return;

    this.measureTooltipElement = document.createElement('div');
    this.measureTooltipElement.className = 'ol-tooltip ol-tooltip-measure';

    this.measureTooltip = new ol.Overlay({
      id: MEASURE_TOOLTIP_OVERLAY_ID,
      element: this.measureTooltipElement,
      offset: [0, -15],
      positioning: 'bottom-center',
      stopEvent: false,
      insertFirst: false,
    });
    olMap.addOverlay(this.measureTooltip);
  }

  private filterLabelOptions(value: string): AnnotationLabel[] {
    if (!value) return [];
    const filterValue = value.toLowerCase();

    return this.labelOptions.filter((labelOption) => {
      return labelOption.name.toLowerCase().includes(filterValue);
    });
  }

  private generateRandomColor(isPastel = false): string {
    const r = Math.floor(Math.random() * 256);
    const g = Math.floor(Math.random() * 256);
    const b = Math.floor(Math.random() * 256);

    if (isPastel) {
      return `hsl(${360 * Math.random()}, ${25 + 70 * Math.random()}%, ${
          85 + 10 * Math.random()}%)`;
    }
    return `rgb(${r}, ${g}, ${b})`;
  }


  private measurePointerMessageHandler() {
    const olMap = this.olMap;
    if (!olMap) return;

    this.createHelpTooltip();
    olMap.on('pointermove', (evt) => {
      if (evt.dragging) {
        return;
      }
      const cursorMessageAllowListActions = [
        ViewerMenuAction.MEASURE,
        ViewerMenuAction.DRAW_POLYGON,
      ];
      if (cursorMessageAllowListActions.includes(this.selectedViewerAction)) {
        let helpMsg = 'Click to start drawing';

        if (this.measureDrawing) {
          const geom = this.measureDrawing.getGeometry();
          const continuePolygonMsg = 'Click to continue drawing the polygon';
          const continueLineMsg = 'Click to stop drawing the line';
          if (geom instanceof Polygon) {
            helpMsg = continuePolygonMsg;
          } else if (geom instanceof LineString) {
            helpMsg = continueLineMsg;
          }
        }
        if (!this.helpTooltipElement) return;

        if (this.selectedViewerAction === ViewerMenuAction.MEASURE) {
          helpMsg = helpMsg.replace('drawing', 'measuring');
        }

        this.windowService.safelySetInnerHtml(this.helpTooltipElement, helpMsg);

        this.helpTooltipOverlay?.setPosition(evt.coordinate);
        this.helpTooltipElement?.classList.remove('hidden');
        return;
      } else {
        if (this.helpTooltipOverlay && olMap) {
          this.resetHelpTooltipOverlay();
        }
      }
      this.helpTooltipElement?.classList.remove('hidden');
    });
    olMap.getViewport().addEventListener('mouseout', () => {
      this.helpTooltipElement?.classList.add('hidden');
    });
  }

  private openSelectedAnnotationOverlay(
      feature: ol.Feature, openOnSingleClick = true) {
    const olMap = this.olMap;

    if (!olMap || !this.selectedAnnotationOverlay) return;
    this.selectedFeature = feature;
    let coordinates: Coordinate[] = [];

    if (feature.getGeometry()?.getType() === 'Polygon') {
      const polygonFeature = feature as Feature<Polygon>;
      const featureCoordinates =
          (polygonFeature.getGeometry()?.getCoordinates() ?? []);
      coordinates = featureCoordinates[0];

      const isROI = getFeatureAnnotationKey(feature).names.includes('ROI');
      const isMeasure =
          getFeatureAnnotationKey(feature).names.includes('MEASURE');
      if (isROI || isMeasure) {
        coordinates = polygonFeature.getGeometry()?.getCoordinates()[0] ?? [];
      }
    }
    if (feature.getGeometry()?.getType() === 'Point') {
      const pointFeature = feature as Feature<Point>;
      const pointCoordinate = pointFeature.getGeometry()?.getCoordinates();
      coordinates = pointCoordinate ? [pointCoordinate] : [];
    }

    const coordinate = getRightMostCoordinate(coordinates);

    if (!coordinate?.length) return;

    this.selectedAnnotationOverlay.setPosition(coordinate);
    if (!openOnSingleClick) {
      const positionChangeListener =
          this.selectedAnnotationOverlay?.on('change:position', (position) => {
            const targetOverlay = position.target as ol.Overlay;
            const targetOverlayPosition: Coordinate|undefined =
                targetOverlay?.getPosition();
            if (this.selectedAnnotationOverlay && !targetOverlayPosition &&
                olMap) {
              olMap.removeOverlay(this.selectedAnnotationOverlay);
              this.selectedFeature = undefined;
            }
            if (positionChangeListener) {
              unByKey(positionChangeListener);
            }
          });
    }
  }

  private resetHelpTooltipOverlay() {
    const olMap = this.olMap;

    if (!olMap || !this.helpTooltipOverlay) return;

    olMap.removeOverlay(this.helpTooltipOverlay);
  }

  // Removes all measure overlays from map.
  private resetMeasureTooltips() {
    const olMap = this.olMap;
    if (!olMap) return;

    const measureLayer = olMap.getAllLayers().find(
        (layer) => layer.get('name') === 'measure-layer');
    if (!measureLayer) return;
    const measureSource =
        measureLayer.getProperties()['source'] as Vector<Feature<Geometry>>;
    if (!measureSource) return;
    measureSource.clear();

    const overlays = olMap.getOverlays().getArray();
    overlays
        .filter((overlay) => {
          return overlay.getId() === MEASURE_TOOLTIP_OVERLAY_ID;
        })
        .forEach((overlay) => {
          olMap?.removeOverlay(overlay);
        });
  }

  private setupSlideMetaData() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.slideMetaDataBySlideDescriptorId$
    ])
        .pipe(
            tap(([
                  selectedSplitViewSlideDescriptor,
                  slideMetaDataBySlideDescriptorId
                ]) => {
              if (!selectedSplitViewSlideDescriptor ||
                  !slideMetaDataBySlideDescriptorId) {
                return;
              }
              this.selectedExtraMetaData = slideMetaDataBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor.id as string);
            }),
            )
        .subscribe();
  }

  private setupSlideInfo() {
    combineLatest([
      this.imageViewerPageStore.selectedSplitViewSlideDescriptor$,
      this.imageViewerPageStore.slideInfoBySlideDescriptorId$
    ])
        .pipe(
            tap(([
                  selectedSplitViewSlideDescriptor, slideInfoBySlideDescriptorId
                ]) => {
              if (!selectedSplitViewSlideDescriptor ||
                  !slideInfoBySlideDescriptorId) {
                return;
              }

              const slideInfo = slideInfoBySlideDescriptorId.get(
                  selectedSplitViewSlideDescriptor.id as string);
              this.slideApiService.selectedSlideInfo$.next(slideInfo);
              this.selectedSlideInfo = slideInfo;
              if (slideInfo) {
                this.validateLabelOrOverviewImage(slideInfo);
              }
            }),
            )
        .subscribe();
  }

  private validateLabelOrOverviewImage(slideInfo: SlideInfo) {
    const hasLabelOrOverviewImage = slideInfo.associatedImages.some(
        (associatedImage) =>
            associatedImage.type === AssociatedImageType.LABEL ||
            associatedImage.type === AssociatedImageType.OVERVIEW);
    this.hasLabelOrOverviewImage = hasLabelOrOverviewImage;
  }

  removeAnnotationLabel(label: AnnotationLabel): void {
    this.labels.delete(label);
  }

  resetOtherTools() {
    const olMap = this.olMap;
    if (!olMap) return;

    this.resetMeasureTooltips();
    const removableInteractions =
        olMap.getInteractions().getArray().filter((interaction) => {
          return interaction instanceof Select ||
              interaction instanceof Modify || interaction instanceof Draw;
        });
    if (!removableInteractions.length) return;

    removableInteractions.forEach((interaction) => {
      olMap?.removeInteraction(interaction);
    });

    this.selectedAnnotationOverlay?.setPosition(undefined);
  }

  saveAnnotation() {
    // const labelValue = this.annotationLabelCtrl.getRawValue();
    this.annotationOverlayActionCurrent = AnnotationOverlayAction.SAVE;
    this.selectedAnnotationOverlay?.setPosition(undefined);

    this.createOrUpdateAnnotation();
  }

  saveAnnotationNote(feature: ol.Feature<Geometry>) {
    const featureAnnotationKey = getFeatureAnnotationKey(feature);
    featureAnnotationKey.notes = this.annotationNoteCtrl.value || '';
    feature.setId(JSON.stringify(featureAnnotationKey));

    this.saveAnnotation();
  }

  selectFeature(feature: ol.Feature, isNewAnnotation = false) {
    const olMap = this.olMap;
    if (!olMap) return;

    this.labels.clear();
    this.annotationNoteCtrl.reset();
    this.selectedAnnotationKey = undefined;
    this.editModeSelectedAnnotationOverlay = isNewAnnotation;

    const featureKey = feature.getId();
    if (featureKey && typeof featureKey === 'string') {
      const annotationKey: AnnotationKey = getFeatureAnnotationKey(feature);
      // Set labels
      const featureLabels: AnnotationLabel[] =
          annotationKey.names.split(', ').map((name) => {
            name = name.trim();
            const labelOption = this.labelOptions.find(
                (labelOption) => labelOption.name === name);
            return {
              ...labelOption,
              name,
              color: '',
            };
          });
      this.labels = new Set(featureLabels);
      // Set notes
      this.annotationNoteCtrl.setValue(annotationKey.notes);
      this.selectedAnnotationKey = annotationKey;
    }
    this.isAnnotationOverlayEditMode = true;
    this.selectedFeature = feature;

    this.openSelectedAnnotationOverlay(feature);
  }

  selected(event: MatAutocompleteSelectedEvent): void {
    this.labels.add(event.option.value);
    this.annotationLabelCtrl.setValue(null);
  }

  setupModifyHandler(selectInteraction: Select) {
    const olMap = this.olMap;
    if (!olMap) return;
    const modifyInteraction = new Modify({
      features: selectInteraction.getFeatures(),
    });
    modifyInteraction.on('modifyend', () => {
      this.saveAnnotation();
    });

    this.imageViewerPageStore.cleanOldModifyInteractions(olMap);
    olMap.addInteraction(modifyInteraction);

    return modifyInteraction;
  }

  setupSelectHandler(olMap: ol.Map): Select|undefined {
    let selectInteraction =
        olMap.getInteractions().getArray().filter((interaction) => {
          return interaction instanceof Select;
        })[0] as Select;
    if (!selectInteraction) {
      selectInteraction = new Select({
        style: SELECT_STYLE,
      });

      olMap.addInteraction(selectInteraction);
    }

    selectInteraction.on('select', (e: SelectEvent) => {
      this.annotationSelected(e);
    });

    return selectInteraction;
  }

  sideNavSelectFeature(feature: ol.Feature) {
    const olMap = this.olMap;
    if (!olMap) return;

    this.onSelectTool();
    const select = olMap?.getInteractions().getArray().filter((interaction) => {
      return interaction instanceof Select;
    })[0] as Select;

    this.selectFeature(feature);
    if (!select) return;

    const features = select.getFeatures();
    features.clear();
    features.push(feature);
    this.selectedAnnotations = new Set(features.getArray());
  }

  toggleAllSideNavLayerOpacity() {
    if (this.toggleSideNavLayerStyleByLayer.size <
        this.sideNavDrawLayers.length) {
      this.toggleSideNavLayerStyleByLayer.clear();
      this.sideNavDrawLayers.forEach((sideNavLayer) => {
        this.toggleSideNavLayerOpacity(sideNavLayer);
      });
    } else if (
        this.toggleSideNavLayerStyleByLayer.size ===
        this.sideNavDrawLayers.length) {
      this.sideNavDrawLayers.forEach((sideNavLayer) => {
        this.toggleSideNavLayerOpacity(sideNavLayer);
      });
    }
  }

  toggleAnnotationInstancesSelected(annotationInstance:
                                        DicomAnnotationInstance) {
    const selectedInstanceIdsBySlideDescriptorId =
        this.imageViewerPageStore.selectedInstanceIdsBySlideDescriptorId$.value;
    const selectedSplitViewSlideDescriptorId = this.getSelectedSlideId();
    const selectedInstanceIds = selectedInstanceIdsBySlideDescriptorId.get(
                                    selectedSplitViewSlideDescriptorId) ??
        new Set<string>();

    const annotationInstancePathInstanceUID =
        annotationInstance.path.instanceUID ?? '';
    if (selectedInstanceIds.has(annotationInstancePathInstanceUID)) {
      selectedInstanceIds.delete(annotationInstancePathInstanceUID);
    } else {
      selectedInstanceIds.add(annotationInstancePathInstanceUID);
    }
    selectedInstanceIdsBySlideDescriptorId.set(
        selectedSplitViewSlideDescriptorId, selectedInstanceIds);

    this.imageViewerPageStore.selectedInstanceIdsBySlideDescriptorId$.next(
        selectedInstanceIdsBySlideDescriptorId);

    this.selectedAnnotationOverlay?.setPosition(undefined);
    this.contextMenuOverlay?.setPosition(undefined);
  }

  toggleSideNavLayerOpacity(sideNavLayer: SideNavLayer) {
    if (!this.toggleSideNavLayerStyleByLayer.has(sideNavLayer.feature)) {
      // Store style of the annotation, to maintain stroke color based on label.
      this.toggleSideNavLayerStyleByLayer.set(
          sideNavLayer.feature, sideNavLayer.feature.getStyle());
      // Setting style to 'undefined' removes them from being viewable.
      sideNavLayer.feature.setStyle(new Style(undefined));
    } else {
      // Restore to original style.
      const originalStyle =
          this.toggleSideNavLayerStyleByLayer.get(sideNavLayer.feature)!;
      this.toggleSideNavLayerStyleByLayer.delete(sideNavLayer.feature);
      sideNavLayer.feature.setStyle(originalStyle);
    }
  }

  toggleSlideLayerOpacity() {
    const olMap = this.olMap;
    if (!olMap) return;

    const layerToModify = olMap.getAllLayers().find(
        (layer) => layer.get('name') === 'slide-layer');
    if (!layerToModify) return;

    layerToModify.setVisible(!layerToModify.getVisible());
  }

  scrollAnnotationsIntoView() {
    setTimeout(() => {
      const annotationLayersElement =
          this.imageViewerSideNavElementRef.nativeElement.querySelector(
              '.annotation-layers');

      if (!annotationLayersElement) return;
      annotationLayersElement.scrollIntoView({
        behavior: 'smooth',
      });
    }, 150);
  }
}
