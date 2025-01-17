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
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, ReactiveFormsModule, Validators} from '@angular/forms';
import {Router} from '@angular/router';
import {catchError, filter, of, switchMap, tap} from 'rxjs';

import {ImageViewerPageParams} from '../../app/app.routes';
import {environment} from '../../environments/environment';
import {resetDicomStores} from '../../interfaces/dicom_store_descriptor';
import {constructDicomWebUrl, parseDICOMwebUrl} from '../../interfaces/dicomweb';
import {AuthService} from '../../services/auth.service';

/**
 * Component for configuring the DICOM endpoint.
 */
@Component({
  selector: 'app-dicom-endpoint-form',
  templateUrl: './config.component.html',
  styleUrl: './config.component.scss',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule]
})
export class ConfigComponent implements OnInit {
  dicomForm!: FormGroup;
  isLoading = false;
  testResult: 'success'|'error'|null = null;
  errorMessage = '';

  constructor(
      private formBuilder: FormBuilder, private http: HttpClient,
      private readonly authGuard: AuthService,
      private readonly router: Router) {}

  ngOnInit() {
    // Initialize the DICOM endpoint form with validation.
    this.dicomForm = this.formBuilder.group({
      endpointUrl: [
        environment.IMAGE_DICOM_STORE_BASE_URL || '',
        [Validators.required, Validators.pattern(/./)]
      ],
      annotationsUrl: [environment.ANNOTATIONS_DICOM_STORE_BASE_URL || '']
    });
  }

  copyEndpointToAnnotations() {
    const endpointUrlValue = this.dicomForm.get('endpointUrl')!.value;
    this.dicomForm.get('annotationsUrl')!.setValue(
        endpointUrlValue.replace(/dicomWeb.*/i, 'dicomWeb'));
  }

  onSubmit() {
    // Form submission handler.
    if (this.dicomForm.valid) {
      this.isLoading = true;   // Start loading indicator
      this.testResult = null;  // Reset test result
      this.errorMessage = '';  // Clear any existing error message

      const endpointUrl =
          this.dicomForm.value.endpointUrl.replaceAll('%2F', '/');
      // Parse the base URL and parent from the DICOMweb URL.
      const dicomwebUrlConfig = parseDICOMwebUrl(endpointUrl);
      if (!dicomwebUrlConfig.baseUrl) {
        this.errorMessage = 'Failed to parse URL.';
        this.isLoading = false;
        this.testResult = 'error';
        return;
      }
      // Check if the base URL is relative or missing the domain, use standard
      // GCP endpoint.
      // https://cloud.google.com/healthcare-api/docs/reference/rest
      if (dicomwebUrlConfig.baseUrl.startsWith('/project') ||
          dicomwebUrlConfig.baseUrl.startsWith('project')) {
        dicomwebUrlConfig.baseUrl =
            'https://healthcare.googleapis.com/v1/' + dicomwebUrlConfig.baseUrl;
        if (!dicomwebUrlConfig.baseUrl.endsWith('/dicomWeb')) {
          dicomwebUrlConfig.baseUrl += '/dicomWeb';
        }
      }
      const url = constructDicomWebUrl(dicomwebUrlConfig);

      // Obtain OAuth token and then make a test request to the DICOM endpoint.
      this.authGuard.getOAuthToken()
          .pipe(
              switchMap(bearerToken => this.http.get(`${url}/instances`, {
                responseType: 'json',
                params: {limit: 1},
                ...(bearerToken && {
                  'headers': new HttpHeaders(
                      {'Authorization': `Bearer ${bearerToken}`})
                })
              })),
              tap(() => {
                this.testResult = 'success';  // Set success status
                {
                  environment.IMAGE_DICOM_STORE_BASE_URL =
                      dicomwebUrlConfig.baseUrl!;
                  environment.ENABLE_SERVER_INTERPOLATION =
                      !environment.IMAGE_DICOM_STORE_BASE_URL.includes(
                          'healthcare.googleapis.com/v1');
                }
              }),

              // Handle errors during the request.
              catchError(
                  error => {
                    console.log(error);  // CONSOLE LOG OK
                    const errorText = error?.error?.[0]?.error?.message ??
                        error?.message ?? error?.statusText ?? error;
                    this.testResult = 'error';  // Set error status
                    this.errorMessage = `${errorText} ${error?.status}`;

                    // Provide more specific error message for common issues.
                    if (error.status === 0 && !error.message) {
                      this.errorMessage = 'CORS or other HTTP error';
                    }

                    return of(null);
                  },
                  ),
              filter(value => value !== null),
              tap(() => {
                environment.ENABLE_ANNOTATION_WRITING = false;
                if (!this.dicomForm.value.annotationsUrl) return;
                const annotationsUrl =
                    parseDICOMwebUrl(this.dicomForm.value.annotationsUrl)
                        .baseUrl;
                if (!annotationsUrl) return;
                environment.ANNOTATIONS_DICOM_STORE_BASE_URL = annotationsUrl;
                environment.ANNOTATIONS_DICOM_STORE_PARENT = '';
                environment.ENABLE_ANNOTATION_WRITING = true;
              }),
              tap(() => {
                resetDicomStores();
                if (dicomwebUrlConfig.path.seriesUID) {
                  const newViewerQueryParams: ImageViewerPageParams = {
                    series: constructDicomWebUrl(dicomwebUrlConfig),
                  };
                  this.router.navigate(['/viewer'], {
                    queryParams: newViewerQueryParams,
                  });
                } else {
                  this.router.navigate(['/search']);
                }
              }),
              )
          .subscribe({
            complete: () => this.isLoading = false  // Stop loading indicator
          });
    }
  }
}
