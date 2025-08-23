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

import {HttpStatusCode} from '@angular/common/http';
import {Injectable, OnDestroy} from '@angular/core';
import {ActivatedRoute, NavigationStart, Router} from '@angular/router';
import * as ol from 'ol';
import {Feature} from 'ol';
import {Coordinate} from 'ol/coordinate';
import {Geometry, LineString, Point, Polygon} from 'ol/geom';
import {Modify} from 'ol/interaction';
import {Vector} from 'ol/source';
import {BehaviorSubject, combineLatest, EMPTY, forkJoin, Observable, of, ReplaySubject} from 'rxjs';
import {catchError, distinctUntilChanged, filter, map, switchMap, takeUntil, tap} from 'rxjs/operators';

import {CohortPageParams, DEFAULT_VIEWER_URL, ImageViewerPageParams} from '../app/app.routes';
import {environment} from '../environments/environment';
import {DicomAnnotation, DicomAnnotationInstance} from '../interfaces/annotation_descriptor';
import {DicomModel} from '../interfaces/dicom_descriptor';
import {addDicomStorePrefixIfMissing, dicomIdToDicomStore} from '../interfaces/dicom_store_descriptor';
import {constructDicomWebPath, parseDICOMwebUrl} from '../interfaces/dicomweb';
import {SlideDescriptor, SlideExtraMetadata, SlideInfo} from '../interfaces/slide_descriptor';
import {AnnotationKey, DEFAULT_ANNOTATION_KEY, IccProfileType, SideNavLayer} from '../interfaces/types';
import {CohortService} from '../services/cohort.service';
import {DialogService} from '../services/dialog.service';
import {DicomAnnotationsService} from '../services/dicom-annotations.service';
import {SlideApiService} from '../services/slide-api.service';
import {UserService} from '../services/user.service';

/**
 * Store to connect all children of the  Image Viewer page.
 */
@Injectable({ providedIn: 'root' })
export class ImageViewerPageStore implements OnDestroy {
    caseId = '';
    iccProfile$ = new BehaviorSubject<IccProfileType>(IccProfileType.ADOBERGB);
    private lastSeries?: string;


    isMultiViewSlidePicker$ = new BehaviorSubject<boolean>(false);
    multiViewScreenSelectedIndex$ = new BehaviorSubject<number>(0);
    multiViewScreens$ = new BehaviorSubject<number>(1);
    showXYZCoordinates$ = new BehaviorSubject<boolean>(false);
    syncLock$ = new BehaviorSubject<boolean>(false);
    loadingDicomAnnotations$ = new BehaviorSubject<boolean>(true);

    selectedSplitViewSlideDescriptor$ =
        new BehaviorSubject<SlideDescriptor | undefined>(undefined);
    slideInfoBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, SlideInfo>>(new Map<string, SlideInfo>());

    olMapBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, ol.Map>>(new Map<string, ol.Map>());
    olMapOriginalViewBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, ol.View>>(new Map<string, ol.View>());

    slideMetaDataBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, SlideExtraMetadata>>(
            new Map<string, SlideExtraMetadata>());
    splitViewSlideDescriptors$ =
        new BehaviorSubject<Array<SlideDescriptor | undefined>>([]);
    slideDescriptorsByCaseId$ =
        new BehaviorSubject<Map<string, SlideDescriptor[]>>(
            new Map<string, SlideDescriptor[]>());
    annotationInstancesBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, DicomAnnotationInstance[]>>(
            new Map<string, DicomAnnotationInstance[]>());
    currentUser = '';
    selectedInstanceIdsBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, Set<string>>>(
            new Map<string, Set<string>>());
    private readonly destroy$ = new ReplaySubject();
    annotationsDicomModelByAnnotationInstanceId$ =
        new BehaviorSubject<Map<string, DicomModel>>(
            new Map<string, DicomModel>());

    sideNavLayersBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, SideNavLayer[]>>(
            new Map<string, SideNavLayer[]>());
    hasAnnotationReadAccessBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, boolean>>(new Map<string, boolean>());
    hasAnnotationWriteAccessBySlideDescriptorId$ =
        new BehaviorSubject<Map<string, boolean>>(new Map<string, boolean>());

    constructor(
        private readonly activatedRoute: ActivatedRoute,
        private readonly cohortService: CohortService,
        private readonly dialogService: DialogService,
        private readonly dicomAnnotationsService: DicomAnnotationsService,
        private readonly userService: UserService,
        private readonly slideApiService: SlideApiService,
        private readonly router: Router,
    ) {
        this.setup();
    }

    ngOnDestroy() {
        this.destroy$.next(true);
        this.destroy$.complete();
    }

    private setup() {
        this.setupCurrentUser();
        this.setupRouteHandling();
        this.setupSplitViewSlideDescriptorsChanges();
        this.setupFetchingAnnotations();
        this.setupAnnotationLayers();
    }

    // Method to create or modify a DICOM annotation for a given slide.
    createOrModifyDicomAnnotation(
        slideDescriptorId: string,
        dicomAnnotation: DicomAnnotation,
    ) {
        this.hasAnnotationWriteAccessBySlideDescriptorId$.value.set(
            slideDescriptorId, true);
        this.hasAnnotationWriteAccessBySlideDescriptorId$.next(
            this.hasAnnotationWriteAccessBySlideDescriptorId$.value);

        return this.dicomAnnotationsService
            .createOrModifyDicomAnnotation(dicomAnnotation)
            .pipe(
                takeUntil(this.destroy$),
                tap((newDicomModel) => {
                    const dicomAnnotationInstances =
                        this.dicomAnnotationsService.dicomAnnotationInstances$.value;

                    const prevAnnotationInstance =
                        (this.annotationInstancesBySlideDescriptorId$.value.get(
                            slideDescriptorId) ??
                            []).find((a) => a.annotatorId === this.currentUser);
                    const newAnnotationInstance = dicomAnnotationInstances.find(
                        (a) => a.annotatorId === this.currentUser);

                    const annotationInstanceIds =
                        this.selectedInstanceIdsBySlideDescriptorId$.value.get(
                            slideDescriptorId) ??
                        new Set<string>();

                    if (prevAnnotationInstance?.path.instanceUID) {
                        annotationInstanceIds.delete(
                            prevAnnotationInstance.path.instanceUID);
                    }
                    if (newAnnotationInstance?.path.instanceUID) {
                        annotationInstanceIds.add(
                            newAnnotationInstance.path.instanceUID);
                    }

                    this.selectedInstanceIdsBySlideDescriptorId$.value.set(
                        slideDescriptorId, annotationInstanceIds);
                    this.annotationInstancesBySlideDescriptorId$.value.set(
                        slideDescriptorId, dicomAnnotationInstances);

                    if (newAnnotationInstance?.path.instanceUID) {
                        const annotationsDicomModelByAnnotationInstanceId =
                            this.annotationsDicomModelByAnnotationInstanceId$.value;
                        annotationsDicomModelByAnnotationInstanceId.set(
                            newAnnotationInstance.path.instanceUID, newDicomModel);
                        if (prevAnnotationInstance?.path.instanceUID) {
                            annotationsDicomModelByAnnotationInstanceId.delete(
                                prevAnnotationInstance?.path.instanceUID);
                        }

                        this.annotationsDicomModelByAnnotationInstanceId$.next(
                            annotationsDicomModelByAnnotationInstanceId);
                    }

                    this.selectedInstanceIdsBySlideDescriptorId$.next(
                        this.selectedInstanceIdsBySlideDescriptorId$.value);
                    this.annotationInstancesBySlideDescriptorId$.next(
                        this.annotationInstancesBySlideDescriptorId$.value);
                }),
                catchError((error) => {
                    let errorMessage = 'Error while creating Dicom annotation.';

                    if (error.status === HttpStatusCode.Forbidden) {
                        this.hasAnnotationWriteAccessBySlideDescriptorId$.value.set(
                            slideDescriptorId, false);
                        this.hasAnnotationWriteAccessBySlideDescriptorId$.next(
                            this.hasAnnotationWriteAccessBySlideDescriptorId$.value);

                        errorMessage = `"Permission required."
                    Your current access level doesn't allow you to create Dicom annotations`;
                    }

                    this.dialogService.error(errorMessage).subscribe();

                    return error;
                }),
            );
    }

    deleteDicomAnnotationsPath(currentUserInstance: DicomAnnotationInstance) {
        const selectedSplitViewSlideDescriptor =
            this.selectedSplitViewSlideDescriptor$.value;
        const slideInfo = this.slideInfoBySlideDescriptorId$.value.get(
            selectedSplitViewSlideDescriptor?.id as string);

        return this.dicomAnnotationsService
            .deleteDicomAnnotationsPath(currentUserInstance.path)
            .pipe(
                switchMap(() => {
                    if (!selectedSplitViewSlideDescriptor?.id || !slideInfo) {
                        return EMPTY;
                    }
                    return this.fetchAnnotationInstances(
                        selectedSplitViewSlideDescriptor.id as string, slideInfo);
                }),
                tap(() => {
                    if (!selectedSplitViewSlideDescriptor?.id) {
                        return;
                    }
                    this.handleSeriesDeletion(
                        currentUserInstance, selectedSplitViewSlideDescriptor);
                }),
            );
    }

    private handleSeriesDeletion(
        currentUserInstance: DicomAnnotationInstance,
        selectedSplitViewSlideDescriptor: SlideDescriptor) {
        const slideDescriptorId = selectedSplitViewSlideDescriptor?.id as string;
        if (!slideDescriptorId) return;

        // Update the list of annotators.
        const annotationInstancesBySlideDescriptorId =
            this.annotationInstancesBySlideDescriptorId$.value;
        let annotationInstances =
            annotationInstancesBySlideDescriptorId.get(slideDescriptorId) ?? [];
        annotationInstances = annotationInstances.filter((annotationInstance) => {
            return annotationInstance.annotatorId !== this.currentUser;
        });

        annotationInstancesBySlideDescriptorId.set(
            slideDescriptorId, annotationInstances);

        this.annotationInstancesBySlideDescriptorId$.next(
            annotationInstancesBySlideDescriptorId);

        // Update list of selected annotators.
        const selectedInstanceIdsBySlideDescriptorId =
            this.selectedInstanceIdsBySlideDescriptorId$.value;
        const selectedInstanceIds =
            selectedInstanceIdsBySlideDescriptorId.get(slideDescriptorId) ??
            new Set<string>();
        selectedInstanceIds.delete(currentUserInstance.path.instanceUID ?? '');
        this.selectedInstanceIdsBySlideDescriptorId$.next(
            selectedInstanceIdsBySlideDescriptorId);
    }

    private setupAnnotationLayers() {
        combineLatest([
            this.olMapBySlideDescriptorId$, this.selectedSplitViewSlideDescriptor$
        ])
            .pipe(
                takeUntil(this.destroy$),
                tap(([olMapBySlideDescriptorId, selectedSlideDescriptor]) => {
                    if (!selectedSlideDescriptor) return;
                    const selectedSlideDescriptorId =
                        selectedSlideDescriptor.id as string;
                    const olMap =
                        olMapBySlideDescriptorId.get(selectedSlideDescriptorId);
                    if (!olMap) return;
                    const drawLayer = olMap.getAllLayers().find((layer) => {
                        return layer.get('name') === 'draw-layer';
                    });
                    if (!drawLayer) return;
                    const drawSource =
                        drawLayer.getSource() as Vector<Feature<Geometry>>;
                    if (!drawSource) return;

                    // Set up a listener for changes in the draw source (like addition
                    // or modification of features).
                    drawSource?.on('change', () => {
                        const drawSourceFeatures = [...drawSource.getFeatures()];
                        const sortedDrawSourceFeatures =
                            drawSourceFeatures.sort((a, b) => {
                                const featureAnnotationKeyA =
                                    this.getFeatureAnnotationKey(a);
                                const featureAnnotationKeyB =
                                    this.getFeatureAnnotationKey(b);

                                return featureAnnotationKeyB.index -
                                    featureAnnotationKeyA.index;
                            });

                        // Map and sort the features to be used in a side navigation
                        // layer.
                        const sideNavDrawLayers =
                            sortedDrawSourceFeatures
                                .map((feature, index) => {
                                    const featureKey = feature.getId();
                                    let featureAnnotationKey: AnnotationKey | undefined =
                                        undefined;

                                    if (!featureKey) {
                                        featureAnnotationKey = {
                                            ...DEFAULT_ANNOTATION_KEY,
                                            names: 'ROI',
                                            annotatorId: this.currentUser,
                                        };
                                        feature.setId(JSON.stringify(featureAnnotationKey));
                                    } else {
                                        featureAnnotationKey = {
                                            ...DEFAULT_ANNOTATION_KEY,
                                            ...this.getFeatureAnnotationKey(feature),
                                        };
                                    }
                                    featureAnnotationKey.names =
                                        `${featureAnnotationKey.names}-${featureAnnotationKey.index}`;


                                    const sideNavLayer: SideNavLayer = {
                                        ...featureAnnotationKey,
                                        feature,
                                    };
                                    return sideNavLayer;
                                })
                                .sort((a, b) => {
                                    return b.index - a.index;
                                });
                        this.sideNavLayersBySlideDescriptorId$.value.set(
                            selectedSlideDescriptorId, sideNavDrawLayers);
                        this.sideNavLayersBySlideDescriptorId$.next(
                            this.sideNavLayersBySlideDescriptorId$.value);
                    });
                }))
            .subscribe();

        combineLatest([
            this.annotationsDicomModelByAnnotationInstanceId$,
            this.olMapBySlideDescriptorId$,
            this.selectedInstanceIdsBySlideDescriptorId$,
        ])
            .pipe(
                takeUntil(this.destroy$),
                tap(() => {
                    this.updateSideNavLayers();
                }),
            )
            .subscribe();
    }

    updateSideNavLayers() {
        const selectedInstanceIdsBySlideDescriptorId =
            this.selectedInstanceIdsBySlideDescriptorId$.value;
        const annotationsDicomModelByAnnotationInstanceId =
            this.annotationsDicomModelByAnnotationInstanceId$.value;
        const olMapBySlideDescriptorId = this.olMapBySlideDescriptorId$.value;
        const slideInfoBySlideDescriptorId =
            this.slideInfoBySlideDescriptorId$.value;
        const annotationInstancesBySlideDescriptorId =
            this.annotationInstancesBySlideDescriptorId$.value;
        const splitViewSlideDescriptors = this.splitViewSlideDescriptors$.value;

        const splitViewSlideDescriptorsFiltered =
            splitViewSlideDescriptors.filter((a): a is SlideDescriptor => !!a?.id);

        const sideNavLayersBySlideDescriptorId: Map<string, SideNavLayer[]> =
            this.generateSideNavLayersBySlideDescriptorId(
                splitViewSlideDescriptorsFiltered,
                slideInfoBySlideDescriptorId,
                annotationInstancesBySlideDescriptorId,
                selectedInstanceIdsBySlideDescriptorId,
                annotationsDicomModelByAnnotationInstanceId,
            );

        splitViewSlideDescriptorsFiltered.forEach((splitViewSlideDescriptor) => {
            const splitViewSlideDescriptorId = splitViewSlideDescriptor.id as string;
            const olMap = olMapBySlideDescriptorId.get(splitViewSlideDescriptorId);

            const sideNavLayers =
                sideNavLayersBySlideDescriptorId.get(splitViewSlideDescriptorId) ??
                [];
            if (!olMap) return;

            const drawLayer = olMap.getAllLayers().find((layer) => {
                return layer.get('name') === 'draw-layer';
            });
            if (!drawLayer) return;
            const drawSource =
                drawLayer.getSource() as Vector<Feature<Geometry>>;
            if (!drawSource) return;

            const existingFeatures = drawSource.getFeatures();
            if (existingFeatures.length === sideNavLayers.length) {
                return;
            }

            this.addAnnotationsToDrawLayer(sideNavLayers, olMap);
        });
    }

    generateSideNavLayersBySlideDescriptorId(
        splitViewSlideDescriptors: SlideDescriptor[],
        slideInfoBySlideDescriptorId: Map<string, SlideInfo>,
        annotationInstancesBySlideDescriptorId:
            Map<string, DicomAnnotationInstance[]>,
        selectedInstanceIdsBySlideDescriptorId: Map<string, Set<string>>,
        annotationsDicomModelByAnnotationInstanceId: Map<string, DicomModel>,
    ) {
        const sideNavLayersBySlideDescriptorId = new Map<string, SideNavLayer[]>();
        splitViewSlideDescriptors.forEach((splitViewSlideDescriptor) => {
            const splitViewSlideDescriptorId = splitViewSlideDescriptor.id as string;

            if (!splitViewSlideDescriptorId) return;

            const slideInfo =
                slideInfoBySlideDescriptorId.get(splitViewSlideDescriptorId);
            if (!slideInfo) return;
            let annotationInstances = annotationInstancesBySlideDescriptorId.get(
                splitViewSlideDescriptorId) ??
                [];

            const selectedInstanceIds = selectedInstanceIdsBySlideDescriptorId.get(
                splitViewSlideDescriptorId) ??
                new Set<string>();
            annotationInstances = annotationInstances.filter((annotator) => {
                return (
                    annotator.path.instanceUID &&
                    selectedInstanceIds.has(annotator.path.instanceUID));
            });
            if (!annotationInstances) return;
            let sideNavLayers: SideNavLayer[] =
                annotationInstances
                    .map((annotator) => {
                        if (!annotator.path.instanceUID) return;
                        const dicomAnnotationModel =
                            annotationsDicomModelByAnnotationInstanceId.get(
                                annotator.path.instanceUID);
                        if (!dicomAnnotationModel) return;
                        return this.dicomModelToSideNavLayers(
                            dicomAnnotationModel, slideInfo);
                    })
                    .filter(
                        (sideNavLayers):
                            sideNavLayers is SideNavLayer[] => {
                            return sideNavLayers !== undefined;
                        })
                    .flat();

            const uniqueUsers =
                annotationInstances.map(({ annotatorId }) => annotatorId)
                    .filter((annotatorId) => annotatorId !== this.currentUser);
            uniqueUsers.push(this.currentUser);

            sideNavLayers = sideNavLayers
                .sort((a: SideNavLayer, b: SideNavLayer) => {
                    return uniqueUsers.indexOf(a.annotatorId) -
                        uniqueUsers.indexOf(b.annotatorId);
                })
                .reverse()
                .map((slideNavLayer, index) => {
                    slideNavLayer.index = index + 1;
                    return slideNavLayer;
                })
                .reverse();
            sideNavLayersBySlideDescriptorId.set(
                splitViewSlideDescriptorId, sideNavLayers);
        });

        return sideNavLayersBySlideDescriptorId;
    }

    getFeatureAnnotationKey(feature: Feature<Geometry>):
        AnnotationKey {
        return JSON.parse(feature.getId() as string) as AnnotationKey;
    }

    addAnnotationsToDrawLayer(
        annotationSideNavLayers: SideNavLayer[], olMap: ol.Map) {
        if (!olMap) return;
        const drawLayer = olMap.getAllLayers().find((layer) => {
            return layer.get('name') === 'draw-layer';
        });
        if (!drawLayer) return;
        const drawSource =
            drawLayer.getSource() as Vector<Feature<Geometry>>;
        if (!drawSource) return;

        const annotationFeatures =
            annotationSideNavLayers.map((annotationSideNavLayer) => {
                // Set feature key based on side nav layer names and notes.
                const annotationFeatureKey: AnnotationKey = {
                    ...DEFAULT_ANNOTATION_KEY,
                    names: annotationSideNavLayer.names ?? `ROI`,
                    notes: annotationSideNavLayer.notes,
                    annotatorId: annotationSideNavLayer.annotatorId,
                    index: annotationSideNavLayer.index,
                };
                annotationSideNavLayer.feature.setId(
                    JSON.stringify(annotationFeatureKey));
                return annotationSideNavLayer.feature;
            });

        drawSource.clear();
        drawSource.addFeatures(annotationFeatures);
    }


    private dicomModelToSideNavLayers(
        dicomAnnotationModel: DicomModel,
        slideInfo: SlideInfo): Array<SideNavLayer | SideNavLayer[]> {
        const dicomAnnotation: DicomAnnotation =
            this.dicomAnnotationsService.dicomModelToAnnotations(
                dicomAnnotationModel, slideInfo);
        const annotationGroups = dicomAnnotation.annotationGroupSequence;
        const errorGroup = annotationGroups.find(a => a.error);
        if (errorGroup) {
            this.dialogService.error(errorGroup.error!);
        }
        const annotationSideNavLayer: Array<SideNavLayer | SideNavLayer[]> =
        annotationGroups.filter(annotationGroup => !annotationGroup.error)
            .map((annotationGroup, index) => {
              const annotatorId = this.dicomAnnotationsService
                                    .getAnnotatorIdByDicomAnnotationInstance(
                                        dicomAnnotationModel);
              // Common properties for all SideNavLayer objects
              const commonProps = {
                                names: annotationGroup.annotationGroupLabel,
                                notes: annotationGroup.annotationGroupDescription,
                                annotatorId,
                                index,
                            };
              switch (annotationGroup.graphicType) {
                case 'POLYLINE':
                  return {
                    ...commonProps,
                    feature: new Feature(new LineString(
                        annotationGroup.pointCoordinatesData)),
                  } as SideNavLayer;
                case 'RECTANGLE':
                  const rectangles: Coordinate[][] = [];

                  for (let i = 0;
                       i < annotationGroup.pointCoordinatesData.length;
                       i += 4) {
                    const rectangle =
                        annotationGroup.pointCoordinatesData.slice(i, i + 4);
                    rectangles.push(rectangle);
                        }
                  const rectangleSideNavs = rectangles.map((rectangle) => {
                    const [topLeft, topRight, bottomRight, bottomLeft] =
                        rectangle;
                    const coordinates = [
                      topLeft, topRight, bottomRight, bottomLeft, topLeft
                    ];  // Close the polygon
                    return {
                      ...{
                        ...commonProps,
                            feature: new Feature(
                                new Polygon([coordinates])),
                      }
                    } as SideNavLayer;
                  });

                  return rectangleSideNavs;
                case 'ELLIPSE':
                  const ellipses: Coordinate[][] = [];

                  for (let i = 0;
                       i < annotationGroup.pointCoordinatesData.length;
                       i += 4) {
                    const ellipse =
                        annotationGroup.pointCoordinatesData.slice(i, i + 4);
                    ellipses.push(ellipse);
                  }

                  const elliipseSideNavs =
                      ellipses.map((ellipsesCoordinates) => {
                        const ellipseFeature =
                            this.computeEllipseFeature(ellipsesCoordinates);
                        return {
                          ...commonProps,
                          feature: ellipseFeature,
                        } as SideNavLayer;
                                    });

                  return elliipseSideNavs;

                case 'POLYGON':
                  return {
                    ...commonProps,
                    feature: new Feature(new Polygon(
                        [annotationGroup.pointCoordinatesData])),
                  } as SideNavLayer;
                case 'POINT':
                  return annotationGroup.pointCoordinatesData.map(
                             (pointCoordinate, pointCoordinateIndex) => ({
                               ...commonProps,
                               feature: new Feature(
                                   new Point(pointCoordinate)),
                               index:
                                   pointCoordinateIndex,  // Use
                                                          // pointCoordinateIndex
                                                          // for POINTs
                             })) as SideNavLayer[];
                default:
                  break;  // Handle unknown graphic types
                        }
              return undefined;  // Return undefined for invalid or unsupported
                                 // cases
                    })
                .filter((entry): entry is SideNavLayer | SideNavLayer[] => !!entry);

        return annotationSideNavLayer.flat();
  }

  private computeEllipseFeature(ellipsesCoordinates:
                                    Coordinate[]) {
    const dicomCoordinates = ellipsesCoordinates;
    // Calculate ellipse parameters
    const majorAxisEndpoints = [dicomCoordinates[0], dicomCoordinates[1]];
    const minorAxisEndpoints = [dicomCoordinates[2], dicomCoordinates[3]];

    const center = [
      (majorAxisEndpoints[0][0] + majorAxisEndpoints[1][0]) / 2,
      (majorAxisEndpoints[0][1] + majorAxisEndpoints[1][1]) / 2
    ];

    const semiMajorAxisLength =
        Math.sqrt(
            Math.pow(majorAxisEndpoints[0][0] - majorAxisEndpoints[1][0], 2) +
            Math.pow(majorAxisEndpoints[0][1] - majorAxisEndpoints[1][1], 2)) /
        2;

    const semiMinorAxisLength =
        Math.sqrt(
            Math.pow(minorAxisEndpoints[0][0] - minorAxisEndpoints[1][0], 2) +
            Math.pow(minorAxisEndpoints[0][1] - minorAxisEndpoints[1][1], 2)) /
        2;

    const rotation = Math.atan2(
        majorAxisEndpoints[1][1] - majorAxisEndpoints[0][1],
        majorAxisEndpoints[1][0] - majorAxisEndpoints[0][0]);

    // Create OpenLayers ellipse geometry (approximate with a
    // Polygon)
    const ellipseCoordinates = [];
    const numberOfPoints = 64;  // Adjust for desired resolution
    for (let i = 0; i < numberOfPoints; i++) {
      const angle = (i / numberOfPoints) * 2 * Math.PI;
      const x = center[0] +
          semiMajorAxisLength * Math.cos(angle) * Math.cos(rotation) -
          semiMinorAxisLength * Math.sin(angle) * Math.sin(rotation);
      const y = center[1] +
          semiMajorAxisLength * Math.cos(angle) * Math.sin(rotation) +
          semiMinorAxisLength * Math.sin(angle) * Math.cos(rotation);
      ellipseCoordinates.push([x, y]);
    }
    const ellipseFeature =
        new Feature(new Polygon([ellipseCoordinates]));
    return ellipseFeature;
    }

    private setupCurrentUser() {
        (environment.ANNOTATION_HASH_STORED_USER_EMAIL ?
            this.userService.getCurrentUserHash$() :
            this.userService.getCurrentUser$()).subscribe((currentUser) => {
            this.currentUser = currentUser ?? '';
            DEFAULT_ANNOTATION_KEY.annotatorId = this.currentUser;
        });
    }

    private setupSplitViewSlideDescriptorsChanges() {
        this.splitViewSlideDescriptors$
            .pipe(
                takeUntil(this.destroy$),
                switchMap((splitViewSlideDescriptors) => {
                    const splitViewSlideDescriptorsFiltered =
                        splitViewSlideDescriptors.filter(
                            (a): a is SlideDescriptor => !!a?.id);
                    if (!splitViewSlideDescriptorsFiltered.length) return EMPTY;

                    const fetchObservables =
                        splitViewSlideDescriptorsFiltered
                            .map((slideDescriptor) => {
                                const slideDescriptorId = slideDescriptor.id as string;
                                return [
                                    this.fetchSlideInfo(slideDescriptorId)
                                        .pipe(
                                            switchMap((slideInfo) => {
                                                if (!slideInfo) return EMPTY;
                                                return this.fetchAnnotationInstances(
                                                    slideDescriptorId, slideInfo);
                                            }),
                                        ),
                                    this.fetchSlideExtraMetadata(slideDescriptorId),
                                ];
                            })
                            .flat()
                            .filter((fetchObservable) => {
                                return fetchObservable !== undefined;
                            });

                    return combineLatest(fetchObservables);
                }),
            )
            .subscribe();
    }

    private setupRouteHandling() {
        this.router.events
            .pipe(
                takeUntil(this.destroy$),
                filter((e): e is NavigationStart => e instanceof NavigationStart),
                tap((e) => {
                    if (!e.url.startsWith(DEFAULT_VIEWER_URL)) {
                        this.resetMultiViewState(true);
                    }
                }),
            )
            .subscribe();

        this.activatedRoute.queryParams
            .pipe(
                takeUntil(this.destroy$),
                distinctUntilChanged(),
                tap((params: ImageViewerPageParams) => {
                    const url = this.router.url;
                    if (!url.startsWith(DEFAULT_VIEWER_URL)) return;

                    let {cohortName, series} = params;
                    if (series) {
                      // Support legacy URLs
                      series = addDicomStorePrefixIfMissing(series);
                    }

                    if (!environment.IMAGE_DICOM_STORE_BASE_URL) {
                        this.router.navigate(['/config']);
                        return;
                    }

                    if (!series && !cohortName) {
                        this.router.navigate(['/search']);
                        return;
                    }

                    if (cohortName) {
                        if (!series) {
                            const queryParams: CohortPageParams = { cohortName };
                            this.router.navigate(['/cohorts'], { queryParams });
                            return;
                        }
                        this.cohortService.fetchPathologyCohort(cohortName);
                    }
                    if (!series) {
                        this.router.navigate(['/search']);
                        return;
                    }
                    // Check the base URL is one of the configured DICOM Stores.
                    try {
                        dicomIdToDicomStore(series);
                    } catch (e) {
                        this.dialogService.error(
                            'For your safety loading this slide was blocked. The DICOM store specified in your browser URL is not on the ones that were configured.')
                            .subscribe(() => {
                                this.router.navigate(['/search']);
                            });
                        return;
                    }

                    const indexSeries = series.indexOf('/series/');

                    let study: string =
                        indexSeries === -1 ? series : series.slice(0, indexSeries);

                    if (this.caseId !== study) {
                        this.caseId = '';
                    }
                    if (!this.caseId) {
                        this.caseId = study;
                        study = '';
                    }



                    if (this.caseId && this.caseId !== study) {
                        this.slideApiService.getSlidesForCase(this.caseId)
                            .subscribe((slides) => {
                                const slideDescriptorsByCaseId =
                                    this.slideDescriptorsByCaseId$.value;
                                slideDescriptorsByCaseId.set(this.caseId, slides);
                                this.slideDescriptorsByCaseId$.next(
                                    slideDescriptorsByCaseId);
                            });
                    }

                    if (series !== this.lastSeries) {
                        this.resetMultiViewState(false);
                    }

                    const splitViewSlideDescriptors = series.split(',').map((a => {
                        if (!a) return undefined;
                        return { id: a };
                    }));

                    if (splitViewSlideDescriptors.length === 3) {
                        splitViewSlideDescriptors.push(undefined);
                    }


                    this.multiViewScreens$.next(splitViewSlideDescriptors.length);
                    this.splitViewSlideDescriptors$.next(splitViewSlideDescriptors);
                    this.selectedSplitViewSlideDescriptor$.next(
                        splitViewSlideDescriptors[this.multiViewScreenSelectedIndex$
                            .value] ??
                        splitViewSlideDescriptors[0]);

                    // Track last series to detect slide/case changes
                    this.lastSeries = series;
                }),
            )
            .subscribe();
    }

    /**
     * Reset multiview UI state.
     * @param full When true, also reset screens and descriptors (leaving viewer).
     */
    private resetMultiViewState(full: boolean) {
        // Always close the picker and set selected index to 0
        if (this.isMultiViewSlidePicker$.value) {
            this.isMultiViewSlidePicker$.next(false);
        }
        if (this.multiViewScreenSelectedIndex$.value !== 0) {
            this.multiViewScreenSelectedIndex$.next(0);
        }

        if (full) {
            if (this.multiViewScreens$.value !== 1) {
                this.multiViewScreens$.next(1);
            }
            if (this.splitViewSlideDescriptors$.value.length) {
                this.splitViewSlideDescriptors$.next([]);
            }
            if (this.selectedSplitViewSlideDescriptor$.value) {
                this.selectedSplitViewSlideDescriptor$.next(undefined);
            }
        }
    }

    private fetchSlideInfo(slideDescriptorId: string) {
        if (this.slideInfoBySlideDescriptorId$.value.has(slideDescriptorId)) {
            return EMPTY;
        }

        return this.slideApiService.getSlideInfo(slideDescriptorId)
            .pipe(
                tap((selectedSlideInfo) => {
                    const slideInfoBySlideDescriptor =
                        this.slideInfoBySlideDescriptorId$.value;
                    slideInfoBySlideDescriptor.set(
                        slideDescriptorId, selectedSlideInfo);

                    this.slideInfoBySlideDescriptorId$.next(
                        slideInfoBySlideDescriptor);
                }),
            );
    }

    private fetchSlideExtraMetadata(slideDescriptorId: string) {
        if (this.slideMetaDataBySlideDescriptorId$.value.has(slideDescriptorId)) {
            return EMPTY;
        }

        return this.slideApiService.getSlideExtraMetadata(slideDescriptorId)
            .pipe(
                map((slideMetadata) => {
                    const slideMetaDataBySlideDescriptor =
                        this.slideMetaDataBySlideDescriptorId$.value;
                    slideMetaDataBySlideDescriptor.set(
                        slideDescriptorId, slideMetadata);

                    return slideMetaDataBySlideDescriptor;
                }),
                tap((slideMetaDataBySlideDescriptor) => {
                    this.slideMetaDataBySlideDescriptorId$.next(
                        slideMetaDataBySlideDescriptor);
                }));
    }

    private fetchAnnotationInstances(
        slideDescriptorId: string, slideInfo: SlideInfo) {
        if (!environment.ENABLE_ANNOTATIONS || !environment.ANNOTATIONS_DICOM_STORE_BASE_URL) return EMPTY;
        if (!slideInfo) return EMPTY;

        const slidePath = constructDicomWebPath(parseDICOMwebUrl(slideDescriptorId).path);
        this.hasAnnotationReadAccessBySlideDescriptorId$.value.set(
            slideDescriptorId, true);
        this.hasAnnotationReadAccessBySlideDescriptorId$.next(
            this.hasAnnotationReadAccessBySlideDescriptorId$.value);

        return this.dicomAnnotationsService
            .fetchDicomAnnotationInstances(slidePath, slideInfo)
            .pipe(
                map((annotationInstances) => {
                    annotationInstances =
                        this.dicomAnnotationsService
                            .getUniqueAnnotationInstances(annotationInstances)
                            .sort((a, b) => {
                                return a.annotatorId.localeCompare(b.annotatorId);
                            });

                    const annotationInstancesBySlideDescriptorId =
                        this.annotationInstancesBySlideDescriptorId$.value;
                    annotationInstancesBySlideDescriptorId.set(
                        slideDescriptorId, annotationInstances);

                    return annotationInstancesBySlideDescriptorId;
                }),
                tap((annotationInstancesBySlideDescriptorId) => {
                    this.annotationInstancesBySlideDescriptorId$.next(
                        annotationInstancesBySlideDescriptorId);
                }),
                catchError((error) => {
                    let errorMessage =
                        'Something went wrong when trying to retrieve Dicom annotations.';
                    if (error.status === HttpStatusCode.Forbidden) {
                        errorMessage = `"Permission required."
                    Your current access level doesn't allow you to retrieve Dicom annotations for:
                    ${slideInfo.slideName}`;

                        this.hasAnnotationReadAccessBySlideDescriptorId$.value.set(
                            slideDescriptorId, false);
                        this.hasAnnotationReadAccessBySlideDescriptorId$.next(
                            this.hasAnnotationReadAccessBySlideDescriptorId$.value);
                    }

                    this.dialogService.error(errorMessage).subscribe();
                    throw new Error(errorMessage);
                }),
            );
    }

    private setupFetchingAnnotations() {
        this.loadingDicomAnnotations$ =
            this.dicomAnnotationsService.loadingDicomAnnotations$;
        // setup intial selected users, inital selected users are current user.
        combineLatest([
            this.splitViewSlideDescriptors$,
            this.selectedInstanceIdsBySlideDescriptorId$,
            this.annotationInstancesBySlideDescriptorId$,
        ])
            .pipe(
                tap(([
                    splitViewSlideDescriptors,
                    selectedInstanceIdsBySlideDescriptorId,
                    annotationInstancesBySlideDescriptorId,
                ]) => {
                    splitViewSlideDescriptors
                        .filter((a): a is SlideDescriptor => !!a?.id)
                        .filter((slideDescriptor) => {
                            if (!slideDescriptor.id) return;
                            const slideDescriptorId = slideDescriptor.id as string;
                            return !selectedInstanceIdsBySlideDescriptorId.has(
                                slideDescriptorId);
                        })
                        .map((slideDescriptor) => {
                            const slideDescriptorId = slideDescriptor.id as string;
                            return slideDescriptorId;
                        })
                        .forEach((slideDescriptorId) => {
                            if (!slideDescriptorId) return;
                            if (selectedInstanceIdsBySlideDescriptorId.has(
                                slideDescriptorId)) {
                                return;
                            }

                            const instanceIds =
                                (annotationInstancesBySlideDescriptorId.get(
                                    slideDescriptorId) ??
                                    [])
                                    .filter((annotationInstance) => {
                                        return annotationInstance.annotatorId ===
                                            this.currentUser;
                                    })
                                    .map((annotationInstance) => {
                                        return annotationInstance.path.instanceUID;
                                    })
                                    .filter((instanceId): instanceId is string => {
                                        return instanceId !== undefined;
                                    });
                            if (!instanceIds.length) return;
                            selectedInstanceIdsBySlideDescriptorId.set(
                                slideDescriptorId, new Set<string>(instanceIds));
                            this.selectedInstanceIdsBySlideDescriptorId$.next(
                                selectedInstanceIdsBySlideDescriptorId);
                        });
                }),
            )
            .subscribe();

        combineLatest([
            this.splitViewSlideDescriptors$,
            this.selectedInstanceIdsBySlideDescriptorId$,
            this.annotationInstancesBySlideDescriptorId$,
        ])
            .pipe(
                takeUntil(this.destroy$),
                map(([
                    splitViewSlideDescriptors,
                    selectedInstanceIdsBySlideDescriptorId,
                    annotationInstancesBySlideDescriptorId,
                ]) => {
                    if (!splitViewSlideDescriptors.length ||
                        !selectedInstanceIdsBySlideDescriptorId.size ||
                        !annotationInstancesBySlideDescriptorId.size) {
                        return;
                    }

                    const selectedInstancesToFetch =
                        splitViewSlideDescriptors
                            .filter((a): a is SlideDescriptor => !!a?.id)
                            .map((splitViewSlideDescriptor) => {
                                const slideDescriptorId =
                                    splitViewSlideDescriptor.id as string;
                                const filteredAnnotationInstances =
                                    (annotationInstancesBySlideDescriptorId.get(
                                        slideDescriptorId) ??
                                        []).filter((annotationInstance) => {
                                            const selectedInstanceIds =
                                                selectedInstanceIdsBySlideDescriptorId.get(
                                                    slideDescriptorId);
                                            if (!selectedInstanceIds) return true;

                                            return annotationInstance.path.instanceUID &&
                                  selectedInstanceIds.has(
                                      annotationInstance.path.instanceUID);
                                        });
                                return {
                                    slideDescriptorId,
                                    filteredAnnotationInstances,
                                };
                            })
                            .filter(({ filteredAnnotationInstances }) => {
                                return filteredAnnotationInstances.length;
                            });

                    return selectedInstancesToFetch;
                }),
                switchMap((selectedInstancesToFetch) => {
                    if (!selectedInstancesToFetch ||
                        !selectedInstancesToFetch.length) {
                        return EMPTY;
                    }
                    return this.fetchAnnotationsDicomModels(selectedInstancesToFetch);
                }),
                map((annotationsDicomModels) => {
                    if (!Array.isArray(annotationsDicomModels)) return;
                    const annotationsDicomModelByAnnotationInstanceId =
                        this.annotationsDicomModelByAnnotationInstanceId$.value;
                    annotationsDicomModelByAnnotationInstanceId.clear();

                    annotationsDicomModels.forEach(
                        ({ annotationInstance, annotations }) => {
                            if (annotationInstance.path.instanceUID) {
                                annotationsDicomModelByAnnotationInstanceId.set(
                                    annotationInstance.path.instanceUID, annotations);
                            }
                        });

                    return annotationsDicomModelByAnnotationInstanceId;
                }),
                tap((annotationsDicomModelByAnnotationInstanceId) => {
                    if (!annotationsDicomModelByAnnotationInstanceId) return;
                    this.annotationsDicomModelByAnnotationInstanceId$.next(
                        annotationsDicomModelByAnnotationInstanceId);
                }),
                catchError(err => {
                    this.dialogService.error(err).subscribe();
                    return EMPTY;
                }))
            .subscribe();
    }

    private fetchAnnotationsDicomModels(
        selectedAnnotationInstances: Array<{
            slideDescriptorId: string;
            filteredAnnotationInstances: DicomAnnotationInstance[];
        }>,
    ) {
        if (!selectedAnnotationInstances) return of(EMPTY);

        const fetchAnnoationsObservables =
            selectedAnnotationInstances
                .map(({ slideDescriptorId, filteredAnnotationInstances }) => {
                    return filteredAnnotationInstances.map(
                        (filteredAnnotationInstance) => {
                            const annotationPath =
                                constructDicomWebPath(filteredAnnotationInstance.path);

                            return this.dicomAnnotationsService
                                .fetchDicomAnnotationModels(annotationPath)
                                .pipe(map((dicomAnnotationModel) => {
                                    return {
                                        annotationInstance: filteredAnnotationInstance,
                                        annotations: dicomAnnotationModel[0]
                                    };
                                }));
                        });
                })
                .flat()
                .filter(
                    (fetchAnnoationsObservables):
                        fetchAnnoationsObservables is (Observable<{
                            annotationInstance: DicomAnnotationInstance;
                            annotations: DicomModel;
                        }>) => {
                        return fetchAnnoationsObservables !== undefined;
                    });

        return forkJoin(fetchAnnoationsObservables);
    }

    // helpers

    cleanOldModifyInteractions(olMap: ol.Map) {
        // cleanup old interactions
        const oldModifyInteractions =
            olMap.getInteractions().getArray().filter((interaction) => {
                return interaction instanceof Modify;
            });
        oldModifyInteractions.forEach((oldModifyInteraction) => {
            olMap?.removeInteraction(oldModifyInteraction);
        });
    }
}
