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

import * as turf from '@turf/turf';

import { Feature, Map } from 'ol';
import { Geometry, Polygon, SimpleGeometry } from 'ol/geom';

import { AnnotationKey } from '../interfaces/types';
import { Coordinate } from 'ol/coordinate';
import { Vector } from 'ol/source';

/**
 * Unions/merges two features into one.
 */
export function unionFeatures(
    feature1: Feature<Geometry>,
    feature2: Feature<Geometry>,
    olMap: Map
): Feature<Geometry> | undefined {
    if (!olMap) return;

    const feature1AnnotationKey = getFeatureAnnotationKey(feature1);
    const feature2AnnotationKey = getFeatureAnnotationKey(feature2);

    const mergedAnnotationKey: AnnotationKey = {
        names: `${feature1AnnotationKey.names ?? ''}`,
        notes: `${feature1AnnotationKey.notes ?? ''} ${feature2AnnotationKey.notes ?? ''}`
            .trim(),
        annotatorId: feature1AnnotationKey.annotatorId,
        index: feature1AnnotationKey.index,
    };
    const feature1Geometry = feature1.getGeometry() as SimpleGeometry;
    const feature2Geometry = feature2.getGeometry() as SimpleGeometry;
    if (!(feature1Geometry instanceof SimpleGeometry && feature2Geometry instanceof SimpleGeometry)) {
        return;
    }

    const turfFeature1 = turf.polygon(feature1Geometry.getCoordinates() ?? []);
    const turfFeature2 = turf.polygon(feature2Geometry.getCoordinates() ?? []);

    const turfUnionFeature = turf.union(turfFeature1, turfFeature2);


    const drawLayer = olMap.getAllLayers().find((layer) => {
        return layer.get('name') === 'draw-layer';
    });
    if (!drawLayer) return;
    const drawSource =
        drawLayer.getSource() as Vector<Feature<Geometry>>;


    const unionCoordinates: turf.helpers.Position[][] | turf.helpers.Position[][][] = turfUnionFeature?.geometry.coordinates ?? [];
    const unionPolygon = new Polygon(unionCoordinates as unknown as Coordinate[][]);
    feature1.setGeometry(unionPolygon);
    feature1.setId(JSON.stringify(mergedAnnotationKey));

    drawSource.removeFeature(feature2);
    return feature1;
}

/**
 * Differences two features and updates the first one.
 */
export function differenceFeatures(
    feature1: Feature<Geometry>,
    feature2: Feature<Geometry>,
    olMap: Map,
) {

    const feature1Geometry = feature1.getGeometry() as SimpleGeometry;
    const feature2Geometry = feature2.getGeometry() as SimpleGeometry;
    if (!(feature1Geometry instanceof SimpleGeometry && feature2Geometry instanceof SimpleGeometry)) {
        return;
    }

    const turfFeature1 = turf.polygon(feature1Geometry.getCoordinates() ?? []);
    const turfFeature2 = turf.polygon(feature2Geometry.getCoordinates() ?? []);

    const turfDifference = turf.difference(turfFeature1, turfFeature2);

    const drawLayer = olMap.getLayers().getArray().find((layer) => {
        return layer.get('name') === 'draw-layer';
    });
    if (!drawLayer) return;

    const diffCoordinates: turf.helpers.Position[][] | turf.helpers.Position[][][] = turfDifference?.geometry.coordinates ?? [];
    const diffPolygon = new Polygon(diffCoordinates as unknown as Coordinate[][]);

    feature1.setGeometry(diffPolygon);
    return feature1;
}

/**
 * Validates if feature1 covers feature2
 */
export function areFeaturesCovered(
    feature1: Feature<Geometry>,
    feature2: Feature<Geometry>,
    olMap: Map,
): boolean {
    if (!olMap) return false;

    const feature1Geometry = feature1.getGeometry() as SimpleGeometry;
    const feature2Geometry = feature2.getGeometry() as SimpleGeometry;
    if (!(feature1Geometry instanceof SimpleGeometry && feature2Geometry instanceof SimpleGeometry)) {
        return false;
    }

    const turfFeature1 = turf.polygon(feature1Geometry.getCoordinates() ?? []);
    const turfFeature2 = turf.polygon(feature2Geometry.getCoordinates() ?? []);

    const isCovered = turf.booleanOverlap(turfFeature1, turfFeature2);

    return isCovered;
}

/**
 * Validates if feature1 intersects feature2
 */
export function areFeaturesIntersecting(
    feature1: Feature<Geometry>,
    feature2: Feature<Geometry>,
    olMap: Map,
): boolean {
    if (!olMap) return false;

    const feature1Geometry = feature1.getGeometry() as SimpleGeometry;
    const feature2Geometry = feature2.getGeometry() as SimpleGeometry;
    if (!(feature1Geometry instanceof SimpleGeometry && feature2Geometry instanceof SimpleGeometry)) {
        return false;
    }

    const turfFeature1 = turf.polygon(feature1Geometry.getCoordinates() ?? []);
    const turfFeature2 = turf.polygon(feature2Geometry.getCoordinates() ?? []);
    const isIntersecting = !!turf.intersect(turfFeature1, turfFeature2)?.type;

    return isIntersecting;
}

/**
 * Gets the feature id for a feature.
 */
export function getFeatureAnnotationKey(feature: Feature<Geometry>):
    AnnotationKey {
    return JSON.parse(feature.getId() as string) as AnnotationKey;
}


/**
 * Given polygon coordinates, returns the right most coordinate.
 */
export function getRightMostCoordinate(coordinates: Coordinate[]):
    Coordinate {
    if (!Array.isArray(coordinates)) {
        return coordinates;
    }

    let coordinate = coordinates[0];
    if (coordinates[0].constructor === Array) {
        coordinates.forEach((coord) => {
            const [x] = coord;
            const [maxX] = coordinate;
            if (maxX < x) {
                // sets coordinate to right most point
                coordinate = coord;
            }
        });
    } else if (coordinates[0].constructor === Number) {
        coordinates.forEach((coord) => {
            if (coordinate < coord) {
                // sets coordinate to right most point
                coordinate = coord;
            }
        });
    }
    return coordinate;
}