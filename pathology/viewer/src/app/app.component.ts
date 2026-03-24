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
import {AfterViewChecked, AfterViewInit, Component} from '@angular/core';
import {Router, RouterOutlet} from '@angular/router';

import {SideNavComponent} from '../components/side-nav/side-nav.component';
import {TopNavComponent} from '../components/top-nav/top-nav.component';
import {environment} from '../environments/environment';
import {AuthService} from '../services/auth.service';
import { ActivityMonitorService } from '../services/activity-monitor.service';

/**
 * The root component.
 */
@Component({
  selector: 'viewer',
  imports: [
    RouterOutlet,
    SideNavComponent,
    TopNavComponent
],
  templateUrl: './app.component.html',
  styleUrl: './app.component.scss'
})
export class AppComponent implements AfterViewInit, AfterViewChecked {
  title = 'viewer';
  isDicomStoreInitialized = false;
  constructor(
      private readonly authService: AuthService,
      private readonly activityMonitor: ActivityMonitorService,
      readonly router: Router,
  ) {}

  ngAfterViewInit(): void {
    this.authService.setupGoogleLogin();
    this.activityMonitor.start();
    this.initializeDarkMode();
  }

  private initializeDarkMode(): void {
    if (environment.ENABLE_DARK_MODE) {
      document.documentElement.classList.add('dark-mode-enabled');
    } else {
      document.documentElement.classList.remove('dark-mode-enabled');
    }
  }

  ngAfterViewChecked() {
    if (this.isDicomStoreInitialized !==
        !!environment.IMAGE_DICOM_STORE_BASE_URL) {
      // Schedule an update for the next event loop iteration to avoid
      // ExpressionChangedAfterItHasBeenCheckedError error.
      Promise.resolve().then(
          () => this.isDicomStoreInitialized =
              !!environment.IMAGE_DICOM_STORE_BASE_URL);
    }
  }
}
