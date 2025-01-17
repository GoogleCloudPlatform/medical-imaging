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
import {AfterViewInit, Component} from '@angular/core';

/**
 * Renders the Google Sign-In button on the login page.
 */
@Component({
  selector: 'login-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './login-page.component.html',
  styleUrl: './login-page.component.scss'
})
export class LoginPageComponent implements AfterViewInit {
  ngAfterViewInit(): void {
    // Ensure 'google' is available globally or consider importing it if it's
    // part of a library
    if (typeof google !== 'undefined' && google.accounts &&
        google.accounts.id) {
      google.accounts.id.renderButton(
          document.getElementById('loginBtn')!,
          {theme: 'outline', size: 'large', width: 200, type: 'standard'});
    } else {
      console.error('Google Identity Services library not loaded.');
    }
  }
}
