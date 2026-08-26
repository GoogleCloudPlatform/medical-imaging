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

import {AfterViewInit, Component, OnInit} from '@angular/core';
import {ActivatedRoute} from '@angular/router';
import {AuthService} from '../../services/auth.service';

/**
 * Renders the Google Sign-In button on the login page.
 */
@Component({
    selector: 'login-page',
    imports: [],
    templateUrl: './login-page.component.html',
    styleUrl: './login-page.component.scss'
})
export class LoginPageComponent implements OnInit, AfterViewInit {
  constructor(
    private readonly route: ActivatedRoute,
    private readonly authService: AuthService,
  ) {}

  ngOnInit(): void {
    const returnUrl = this.route.snapshot.queryParamMap.get('returnUrl');
    if (returnUrl) {
      this.authService.saveReturnUrl(returnUrl);
    }
  }

  ngAfterViewInit(): void {
    // The Google button rendering is handled by AuthService.setupGoogleLogin()
    // which is called from AppComponent.ngAfterViewInit(). That method handles
    // both the case where Google library is already loaded and when it loads later.
    setTimeout(() => {
      if (typeof google !== 'undefined' && google.accounts && google.accounts.id) {
        const loginBtn = document.getElementById('loginBtn');
        if (loginBtn) {
          google.accounts.id.renderButton(
              loginBtn,
              {theme: 'outline', size: 'large', width: 200, type: 'standard'});
        }
      }
    }, 100);
  }
}
