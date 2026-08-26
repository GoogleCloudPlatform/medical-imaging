declare module '@turf/turf' {
    export function point(x: number, y: number, properties?: any): any;
    export function featureCollection(features: any[]): any;
    export function bbox(poly: any): number[];
    export function area(poly: any): number;
    export function distance(pt1: any, pt2: any, options?: any): number;
    export function polygon(coordinates: any, properties?: any): any;
    export function union(poly1: any, poly2: any): any;
    export function difference(poly1: any, poly2: any): any;
    export function booleanContains(feature1: any, feature2: any): boolean;
    export function booleanEqual(feature1: any, feature2: any): boolean;
    export function intersect(poly1: any, poly2: any): any;

    export namespace helpers {
        // Turf's Position type is: [number, number] | [number, number, number]
        export type Position = number[]; // Simplified, or use: [number, number] | [number, number, number]
    }
}