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
import {Component, Inject, Input, OnInit} from '@angular/core';
import {MatButtonModule} from '@angular/material/button';
import {MAT_DIALOG_DATA, MatDialogModule, MatDialogRef} from '@angular/material/dialog';
import {MatIconModule} from '@angular/material/icon';

import {Attribute, DicomModel, isDicomModels, isNumbers, isPersonNames, isStrings, PersonName} from '../../interfaces/dicom_descriptor';
import {type SlideExtraMetadata} from '../../interfaces/slide_descriptor';
import {DICOM_TAGS_SPEC} from '../../utils/dicom_tags_spec';

/**
 * This is the DICOM standard JSON interface type that the DICOM_TAGS_SPEC is
 * internally structured as.
 * DICOM Standard is published at: https://www.dicomstandard.org/current
 **/
declare interface DicomTagsSpec {
  header: {dicom_standard_version: {Version: string}};
  dicom_tags: {
    main: {[key: string]: DicomTag},
  };
}

/**
 * The fields descriribing a dicom tag based on the Dicom standard.
 * http://dicom.nema.org/medical/Dicom/2017e/output/chtml/part06/chapter_6.html
 */
declare interface DicomTag {
  tag: string;
  keyword: string;
  vr: string;
  vm: string;
  name: string;
  retired: string;
}

/** Result of the slide extra metadata dialog action. */
export enum SlideExtraMetadataResult {
  SUCCESS,
}

/**
 * Given a key fetch a dicom tag description from the spec.
 **/
export function dicomTagDescription(key: string): string {
  return (DICOM_TAGS_SPEC as DicomTagsSpec).dicom_tags.main['0x' + key]?.name ||
      '';
}

interface MetadataNode {
  dicomKey: string;
  description: string;
  valueText: string;
  childNodes?: MetadataNode[];
}

/**
 * Zero or more list items representing the tags and values in a DicomModel.
 */
@Component({
  selector: 'metadata-tag-component',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './metadata-tag-component.component.html',
  styleUrl: './metadata-tag-component.component.scss'
})
export class MetadataTagComponent {
  @Input() rootNode?: MetadataNode;
}

/**
 * A modal that displays all the Slide More Info entries at once.
 * Closes when clicking on the overlay background.
 */
@Component({
  selector: 'slide-metadata-component',
  standalone: true,
  imports: [
    MatIconModule,
    MetadataTagComponent,
    CommonModule,
    MatButtonModule,
    MatDialogModule,
  ],
  templateUrl: './slide-metadata-component.component.html',
  styleUrl: './slide-metadata-component.component.scss'
})
export class SlideMetadataComponentComponent implements OnInit {
  @Input() hidden = true;

  rootNodes: MetadataNode[] = [];

  constructor(
      @Inject(MAT_DIALOG_DATA) readonly slideExtraMetadata: SlideExtraMetadata,
      private readonly dialogRef: MatDialogRef<SlideMetadataComponentComponent>,
  ) {}

  ngOnInit() {
    this.rootNodes = this.generateNodes(this.slideExtraMetadata.rawValue);
  }

  openSlideData() {
    this.dialogRef.close({openSlideData: true});
  }

  private generateNodes(rootModel: DicomModel): MetadataNode[] {
    return Object.keys(rootModel)
        .map((dicomKey) => this.generateNode(dicomKey, rootModel[dicomKey]))
        .sort((a, b) => a.dicomKey.localeCompare(b.dicomKey));
  }

  private generateNode(dicomKey: string, attribute: Attribute) {
    const attributeValue = attribute.Value;
    const description = dicomTagDescription(dicomKey);

    let valueText = '';
    let childNodes: MetadataNode[] = [];
    if (isNumbers(attributeValue) || isStrings(attributeValue)) {
      valueText = attributeValue.join();
    } else if (isPersonNames(attributeValue)) {
      childNodes = attributeValue.map(
          personName => this.makeNodeFromPersonName(personName));
    } else if (isDicomModels(attributeValue)) {
      childNodes =
          attributeValue.flatMap(dicomModel => this.generateNodes(dicomModel));
    }

    return {dicomKey, description, valueText, childNodes};
  }

  private makeNodeFromPersonName(personName: PersonName): MetadataNode {
    // Note: At present, we only show the alphabetic representation.
    return {
      dicomKey: 'Alphabetic',
      description: '',
      valueText: personName.Alphabetic
    };
  }
}