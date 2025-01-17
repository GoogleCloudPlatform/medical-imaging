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
import {HttpErrorResponse} from '@angular/common/http';
import {ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit, Optional, ViewChild} from '@angular/core';
import {FormsModule} from '@angular/forms';
import {MatButtonModule} from '@angular/material/button';
import {MatDialogRef} from '@angular/material/dialog';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatSelectModule} from '@angular/material/select';
import {ActivatedRoute, Router} from '@angular/router';
import {forkJoin, from, of, ReplaySubject, throwError} from 'rxjs';
import {bufferCount, catchError, distinctUntilChanged, first, map, mergeMap, takeUntil, tap, toArray} from 'rxjs/operators';

import {InspectPageParams} from '../../app/app.routes';
import {DICOM_STORE, DicomStores} from '../../interfaces/dicom_store_descriptor';
import {SlideInfo} from '../../interfaces/slide_descriptor';
import {DicomwebService, IccProfileType} from '../../services/dicomweb.service';
import {SlideApiService} from '../../services/slide-api.service';
import {pixelSpacingToMagnification} from '../../utils/zoom_name_to_val';

const IMAGE_DICOM_STORE = DicomStores.IMAGE;
const SELECTED_DICOM_STORE = DICOM_STORE[IMAGE_DICOM_STORE];

interface Tile {
  instanceUid: string;
  frame: number;
  downsample: number;
  uid: string;
}

/**
 * Inspect page component.
 */
@Component({
  selector: 'inspect-page',
  standalone: true,
  imports: [
    CommonModule,
    MatFormFieldModule,
    MatInputModule,
    FormsModule,
    MatIconModule,
    MatSelectModule,
    MatButtonModule,
  ],
  templateUrl: './inspect-page.component.html',
  styleUrl: './inspect-page.component.scss'
})
export class InspectPageComponent implements OnInit, OnDestroy {
  urlSeriesUid = '';
  seriesUids: string[] = [];
  errorMsg = '';
  slideInfo?: SlideInfo;

  benchmark = {
    icc: true,
    disableCache: false,
    realLayersOnly: false,
    callComplete: 0,
    tilesComplete: 0,
    numTilesToLoadBaseLayer: 16,
    numFramesPerStride: 5,
    numTilesPerCall: 1,
    concurency: 5,
    latency: 0,
    realTime: 0,
    first10TilesTime: 0,
  };
  benchmarkRunning = false;
  enableDevOptions = false;

  readonly cleanDicomwebRegex = new RegExp('.*dicomWeb/');

  private readonly destroyed$ = new ReplaySubject<boolean>(1);

  @ViewChild('callTable', {static: true})
  callTable!: ElementRef<HTMLTableElement>;

  constructor(
      private readonly elRef: ElementRef<HTMLElement>,
      private readonly dicomwebService: DicomwebService,
      private readonly slideApiService: SlideApiService,
      private readonly ref: ChangeDetectorRef,
      private readonly route: ActivatedRoute,
      private readonly router: Router,
      @Optional() public dialogRef?: MatDialogRef<InspectPageComponent>,
  ) {
    this.enableDevOptions = !this.dialogRef;
  }

  ngOnInit() {
    this.route.queryParams
        .pipe(
            takeUntil(this.destroyed$),
            distinctUntilChanged(),
            )
        .subscribe((params) => {
          const urlSeriesUid = (params as InspectPageParams).series;
          if (urlSeriesUid) {
            this.urlSeriesUid = urlSeriesUid;
            this.load();
          }
        });
  }

  ngOnDestroy() {
    this.destroyed$.next(true);
    this.destroyed$.complete();
  }

  updateUrl() {
    this.router.navigate(['.'], {
      relativeTo: this.route,
      queryParams: {'series': this.urlSeriesUid},
      queryParamsHandling:
          'merge',  // remove to replace all query params by provided
    });
  }

  onSeriesUidChanged() {
    this.slideInfo = undefined;
    this.updateUrl();
  }

  drawCallTable(tilesToLoad: Tile[][]) {
    // Clear table
    const callTable = this.callTable.nativeElement;
    while (callTable.firstChild) {
      callTable.removeChild(callTable.firstChild);
    }

    // Draw table
    for (const [index, level] of tilesToLoad.entries()) {
      const th = document.createElement('th');
      th.appendChild(document.createTextNode(index.toString()));
      const tr = callTable.insertRow();
      tr.appendChild(th);
      for (const tile of level) {
        const div = document.createElement('div');
        div.appendChild(document.createTextNode(tile.frame.toString()));
        div.id = index.toString() + '_' + tile.frame.toString();
        const td = tr.insertCell();
        td.appendChild(div);
      }
    }
  }

  drawCallTileId(uid: string, state: 'called'|'complete'|'error') {
    const e = document.getElementById(uid);
    if (!e) {
      return;
    }
    switch (state) {
      case 'called':
        e.classList.add('called');
        break;
      case 'complete':
        e.classList.add('complete');
        break;
      default:
    }

    this.elRef.nativeElement.scrollTo(0, this.elRef.nativeElement.scrollHeight);
  }

  load() {
    this.benchmarkRunning = false;
    this.errorMsg = '';
    this.slideInfo = undefined;
    try {
      this.slideApiService.getSlideInfo(this.urlSeriesUid)
          .pipe(
              first(), catchError(val => {
                return throwError(val);
              }),
              tap(slideInfo => {
                this.slideInfo = slideInfo;
              }))
          .subscribe({
            error: (err) => {
              this.errorHandle(err);
            }
          });
    } catch (err) {
      this.errorHandle(err);
      return;
    }
  }

  start() {
    if (this.slideInfo === undefined || !this.slideInfo.levelMap) {
      this.errorMsg = 'Missing slide level map';
      return;
    }

    this.benchmarkRunning = true;
    this.benchmark.callComplete = 0;
    this.benchmark.tilesComplete = 0;
    this.errorMsg = '';
    this.ref.markForCheck();
    this.benchmark.latency = 0;
    const tilesToLoad: Tile[][] = [];

    let levelCount = 0;
    for (const level of this.slideInfo.levelMap) {
      const levelProp = level.properties[0];
      if (this.benchmark.realLayersOnly && level.downSampleMultiplier) continue;
      const numFramesAvailable = Math.ceil(
          levelProp.frames / Math.pow(level.downSampleMultiplier ?? 1, 2));
      const numFramesToLoadThisLayer = Math.min(
          Math.ceil(
              this.benchmark.numTilesToLoadBaseLayer / Math.pow(2, levelCount)),
          numFramesAvailable);
      const step = Math.floor(
          numFramesAvailable / this.benchmark.numFramesPerStride /
          numFramesToLoadThisLayer);
      // Add tiles in continous batches of numFramesPerStride.
      tilesToLoad[levelCount] = [];
      let currentFrame = 0;
      for (let tileCount = 0; tileCount < numFramesToLoadThisLayer;) {
        tilesToLoad[levelCount].push({
          instanceUid: levelProp.instanceUid,
          frame: currentFrame,
          downsample: level.downSampleMultiplier ?? 0,
          uid: levelCount.toString() + '_' + currentFrame.toString(),
        } as Tile);
        ++tileCount;
        currentFrame += tileCount % this.benchmark.numFramesPerStride ?
            1 :
            Math.max(step - this.benchmark.numFramesPerStride, 1);
      }
      ++levelCount;
    }

    this.drawCallTable(tilesToLoad);

    const startBenchmarkTime = performance.now();
    from(tilesToLoad)
        .pipe(
            mergeMap(
                arrOfTilesOfLevel =>
                    from(arrOfTilesOfLevel)
                        .pipe(bufferCount(this.benchmark.numTilesPerCall))),
            mergeMap(
                tilesToFetch => {
                  for (const tile of tilesToFetch) {
                    this.drawCallTileId(tile.uid, 'called');
                  }
                  return forkJoin({
                    startTime: of(performance.now()),
                    bytes: this.dicomwebService.getEncodedImageTiles(
                        this.urlSeriesUid, tilesToFetch[0].instanceUid,
                        tilesToFetch.map(t => t.frame + 1),
                        SELECTED_DICOM_STORE, tilesToFetch[0].downsample,
                        this.benchmark.icc ? IccProfileType.SRGB :
                                             IccProfileType.NONE,
                        this.benchmark.disableCache),
                    tileUids: of(tilesToFetch.map(tile => tile.uid)),
                  });
                },
                this.benchmark.concurency),
            map((result) => ({
                  tileUids: result.tileUids,
                  latency: performance.now() - result.startTime
                })),
            tap((out) => {
              ++this.benchmark.callComplete;
              this.benchmark.tilesComplete += out.tileUids.length;
              if (this.benchmark.first10TilesTime === 0 &&
                  this.benchmark.tilesComplete >= 10) {
                this.benchmark.first10TilesTime =
                    performance.now() - startBenchmarkTime;
              }
              for (const uid of out.tileUids) {
                this.drawCallTileId(uid, 'complete');
              }
            }),
            map(out => out.latency), toArray())
        .subscribe(
            (arr) => {
              this.benchmark.realTime = performance.now() - startBenchmarkTime;
              this.benchmarkRunning = false;
              this.benchmark.latency =
                  arr.reduce((a, b) => a + b) / this.benchmark.tilesComplete;
            },
            (err) => {
              this.errorHandle(err);
            });
  }

  errorHandle(err: unknown) {
    if (typeof err === 'string') {
      this.errorMsg = err;
    } else if (err instanceof Error || err instanceof HttpErrorResponse) {
      this.errorMsg = err.message + '\n' + JSON.stringify(err);
    } else {
      this.errorMsg = 'failed';
    }
    this.benchmarkRunning = false;
  }

  pixelSpacingToMagnification(ps: number) {
    return pixelSpacingToMagnification(ps / 10e5);
  }
}
