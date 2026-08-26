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

import { Injectable } from '@angular/core';
import { Style, Stroke, Fill, RegularShape } from 'ol/style';
import { environment } from '../environments/environment';

/**
 * Service to manage annotation style configurations.
 * Provides Style objects for different annotation states based on environment settings.
 */
@Injectable({
  providedIn: 'root'
})
export class AnnotationStyleService {
  private defaultDrawingStyle: Style;
  private selectStyle: Style;
  private contextSelectStyle: Style;
  private measureDeselectedStyle: Style;
  private measureSelectedStyle: Style;
  private measureContextSelectStyle: Style;
  private drawStrokeStyle: Style;

  constructor() {
    this.defaultDrawingStyle = this.createDefaultDrawingStyle();
    this.selectStyle = this.createSelectStyle();
    this.contextSelectStyle = this.createContextSelectStyle();
    this.measureDeselectedStyle = this.createMeasureDeselectedStyle();
    this.measureSelectedStyle = this.createMeasureSelectedStyle();
    this.measureContextSelectStyle = this.createMeasureContextSelectStyle();
    this.drawStrokeStyle = this.createDrawStrokeStyle();
  }

  /**
   * Gets the default drawing style for annotations.
   */
  getDefaultDrawingStyle(): Style {
    return this.defaultDrawingStyle;
  }

  /**
   * Gets the style for selected annotations.
   */
  getSelectStyle(): Style {
    return this.selectStyle;
  }

  /**
   * Gets the style for context menu selected annotations.
   */
  getContextSelectStyle(): Style {
    return this.contextSelectStyle;
  }

  /**
   * Gets the style for deselected measure annotations.
   */
  getMeasureDeselectedStyle(): Style {
    return this.measureDeselectedStyle;
  }

  /**
   * Gets the style for selected measure annotations.
   */
  getMeasureSelectedStyle(): Style {
    return this.measureSelectedStyle;
  }

  /**
   * Gets the style for context menu selected measure annotations.
   */
  getMeasureContextSelectStyle(): Style {
    return this.measureContextSelectStyle;
  }

  /**
   * Gets the style for active drawing tool.
   */
  getDrawStrokeStyle(): Style {
    return this.drawStrokeStyle;
  }

  private createDefaultDrawingStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_STROKE_COLOR,
        width: 2,
      }),
    });
  }

  private createSelectStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_SELECT_COLOR,
        width: 3,
      }),
      image: new RegularShape({
        fill: new Fill({
          color: 'transparent',
        }),
        stroke: new Stroke({
          color: environment.ANNOTATION_SELECT_COLOR,
          width: 3,
        }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }

  private createContextSelectStyle(): Style {
    return new Style({
      fill: new Fill({ color: 'transparent' }),
      stroke: new Stroke({ color: environment.ANNOTATION_CONTEXT_SELECT_COLOR, width: 3 }),
      image: new RegularShape({
        fill: new Fill({ color: 'transparent' }),
        stroke: new Stroke({ color: environment.ANNOTATION_CONTEXT_SELECT_COLOR, width: 3 }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }

  private createMeasureDeselectedStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_MEASURE_DESELECTED_COLOR,
        lineDash: [10, 10],
        width: 2,
      }),
      image: new RegularShape({
        fill: new Fill({
          color: 'transparent',
        }),
        stroke: new Stroke({
          color: environment.ANNOTATION_MEASURE_DESELECTED_COLOR,
          lineDash: [10, 10],
          width: 2,
        }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }

  private createMeasureSelectedStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_MEASURE_SELECTED_COLOR,
        lineDash: [10, 10],
        width: 3,
      }),
      image: new RegularShape({
        fill: new Fill({
          color: 'transparent',
        }),
        stroke: new Stroke({
          color: environment.ANNOTATION_MEASURE_SELECTED_COLOR,
          lineDash: [10, 10],
          width: 3,
        }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }

  private createMeasureContextSelectStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_MEASURE_CONTEXT_SELECT_COLOR,
        lineDash: [10, 10],
        width: 3,
      }),
      image: new RegularShape({
        fill: new Fill({
          color: 'transparent',
        }),
        stroke: new Stroke({
          color: environment.ANNOTATION_MEASURE_CONTEXT_SELECT_COLOR,
          lineDash: [10, 10],
          width: 3,
        }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }

  private createDrawStrokeStyle(): Style {
    return new Style({
      fill: new Fill({
        color: 'transparent',
      }),
      stroke: new Stroke({
        color: environment.ANNOTATION_STROKE_COLOR,
        width: 2,
      }),
      image: new RegularShape({
        fill: new Fill({
          color: 'transparent',
        }),
        stroke: new Stroke({
          color: environment.ANNOTATION_STROKE_COLOR,
          width: 2,
        }),
        points: 4,
        radius: 5,
        angle: Math.PI / 4,
      }),
    });
  }
}
