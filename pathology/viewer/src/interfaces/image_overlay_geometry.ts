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

/**
 * Interface for an x-y coordinate.
 */
export interface Coordinate {
    x: number;
    y: number;
  }
  
  /**
   * Interface for a vector
   */
  export interface VectorInterface extends Coordinate {
    equals(v: VectorInterface): boolean;
    add(v: VectorInterface): VectorInterface;
    subtract(v: VectorInterface): VectorInterface;
    expand(v: VectorInterface): VectorInterface;
    contract(v: VectorInterface): VectorInterface;
    scale(n: number): VectorInterface;
    applyFn(fn: (pos: number) => number): VectorInterface;
    round(): VectorInterface;
    length(): number;
  }
  
  /**
   * Class that encodes basic behavior for vector arithmetic.
   */
  export abstract class VectorBase<V extends VectorInterface> {
    constructor(public x = 0, public y = 0) {}
  
    abstract copy(x: number, y: number): V;
  
    setCoordinates(x: number, y: number) {
      this.x = x;
      this.y = y;
    }
  
    equals(v: V): boolean {
      return this.x === v.x && this.y === v.y;
    }
  
    add(v: V) {
      return this.copy(this.x + v.x, this.y + v.y);
    }
  
    subtract(v: V) {
      return this.copy(this.x - v.x, this.y - v.y);
    }
  
    /**
     * Element-wise maximum of the two vectors.
     */
    expand(v: V) {
      return this.copy(Math.max(this.x, v.x), Math.max(this.y, v.y));
    }
  
    dotProduct(v: V) {
      return this.x * v.x + this.y * v.y;
    }
  
    /**
     * Element-wise minimum of the two vectors.
     */
    contract(v: V) {
      return this.copy(Math.min(this.x, v.x), Math.min(this.y, v.y));
    }
  
    scale(n: number) {
      return this.copy(this.x * n, this.y * n);
    }
  
    applyFn(fn: (pos: number) => number) {
      return this.copy(fn(this.x), fn(this.y));
    }
  
    round() {
      return this.applyFn(Math.round);
    }
  
    length() {
      return Math.sqrt(this.x * this.x + this.y * this.y);
    }
  }
  
  /**
   * A unit-less vector.
   */
  export class Vector extends VectorBase<Vector> {
    constructor(public override x = 0, public override y = 0) {
      super(x, y);
    }
    copy(): Vector;
    copy(x?: number, y?: number): Vector {
      if (x !== undefined && y !== undefined) {
        return new Vector(x, y);
      } else {
        return new Vector(this.x, this.y);
      }
    }
  }
  
  /**
   * A vector in screen pixels.
   */
  export class ScreenVector extends VectorBase<ScreenVector> {
    constructor(public override x = 0, public override y = 0) {
      super(x, y);
    }
    copy(): ScreenVector;
    copy(x?: number, y?: number): ScreenVector {
      if (x !== undefined && y !== undefined) {
        return new ScreenVector(x, y);
      } else {
        return new ScreenVector(this.x, this.y);
      }
    }
    toImageVector(zoom: number) {
      const next = new ImageVector(this.x, this.y);
      return next.scale(1 / zoom);
    }
  }
  
  /**
   * A vector in native image pixels.
   */
  export class ImageVector extends VectorBase<ImageVector> {
    constructor(public override x = 0, public override y = 0) {
      super(x, y);
    }
    copy(): ImageVector;
    copy(x?: number, y?: number): ImageVector {
      if (x !== undefined && y !== undefined) {
        return new ImageVector(x, y);
      } else {
        return new ImageVector(this.x, this.y);
      }
    }
    toScreenVector(zoom: number) {
      const next = new ScreenVector(this.x, this.y);
      return next.scale(zoom);
    }
  }
  
  
  
  class RectangleBase<V extends VectorBase<V>> {
    constructor(public corner: V, public size: V) {}
  
    getCenter() {
      return this.corner.add(this.size.scale(0.5));
    }
  
    contains(position: V) {
      const {x, y} = position;
      const withinX = x >= this.corner.x && x <= this.corner.x + this.size.x;
      const withinY = y >= this.corner.y && y <= this.corner.y + this.size.y;
      return withinX && withinY;
    }
  
    get left() {
      return this.corner.x;
    }
  
    get right() {
      return this.corner.x + this.size.x;
    }
  
    get top() {
      return this.corner.y;
    }
  
    get bottom() {
      return this.corner.y + this.size.y;
    }
  
    get width() {
      return this.size.x;
    }
  
    get height() {
      return this.size.y;
    }
  
    get area() {
      return this.size.x * this.size.y;
    }
  }
  
  /**
   * A unit-less rectangle.
   */
  export class Rectangle extends RectangleBase<Vector> {
    constructor(
        corner: Coordinate = {x: 0, y: 0}, size: Coordinate = {x: 0, y: 0}) {
      const cornerVector = new Vector(corner.x, corner.y);
      const sizeVector = new Vector(size.x, size.y);
      super(cornerVector, sizeVector);
    }
  }
  
  /**
   * A rectangle in screen pixels.
   */
  export class ScreenRectangle extends RectangleBase<ScreenVector> {
    constructor(
        corner: Coordinate = {x: 0, y: 0}, size: Coordinate = {x: 0, y: 0}) {
      const cornerVector = new ScreenVector(corner.x, corner.y);
      const sizeVector = new ScreenVector(size.x, size.y);
      super(cornerVector, sizeVector);
    }
  }
  
  /**
   * A rectangle in native image pixels.
   */
  export class ImageRectangle extends RectangleBase<ImageVector> {
    constructor(
        corner: Coordinate = {x: 0, y: 0}, size: Coordinate = {x: 0, y: 0}) {
      const cornerVector = new ImageVector(corner.x, corner.y);
      const sizeVector = new ImageVector(size.x, size.y);
      super(cornerVector, sizeVector);
    }
    copy(): ImageRectangle {
      return new ImageRectangle(this.corner, this.size);
    }
  }
  
  /** A zoom with an absolute and leveled value. */
  export class Zoom {
    /**
     * @param absolute The absolute (e.g. 0.5) zoom
     * @param level The leveled (e.g. 8) zoom
     */
    constructor(public absolute: number, public level: number) {}
  }