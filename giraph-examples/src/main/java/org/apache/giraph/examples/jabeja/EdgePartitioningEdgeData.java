/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.examples.jabeja;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class EdgePartitioningEdgeData extends BaseWritable {
  /**
   * A unique ID of the edge consisting of the vertex id and an index can be
   * calculated as follows:
   * index * numberOfVertices + vertexId
   * (vertexId should be between 0 and numberOfVertices)
   */
  private long edgeId;

  /**
   * The color of the current node
   */
  private int edgeColor;

  /**
   * Flag, which indicates if the color has changed since it has been reset
   * the last time
   */
  private boolean hasColorChanged;

  /**
   * Default constructor for reflection
   */
  public EdgePartitioningEdgeData() {
    super();
  }

  public long getEdgeId() {
    return edgeId;
  }

  public void setEdgeId(long edgeId) {
    this.edgeId = edgeId;
  }

  public int getEdgeColor() {
    return edgeColor;
  }

  /**
   * Sets the new node color and checks if it has changed,
   * in that case it also sets the flag {@code hasColorChanged}
   *
   * @param edgeColor the new color of the current vertex
   */
  public void setEdgeColor(int edgeColor) {
    if (this.edgeColor != edgeColor) {
      this.edgeColor = edgeColor;
      this.hasColorChanged = true;
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.edgeId = input.readLong();
    this.edgeColor = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(this.edgeId);
    output.writeInt(this.edgeColor);
  }

  @Override
  public String toString() {
    return Integer.toString(this.edgeColor);
  }

  public boolean getHasColorChanged() {
    return this.hasColorChanged;
  }

  /**
   * resets the {@code hasColorChanged}-flag and sets it back to false
   */
  public void resetHasColorChanged() {
    this.hasColorChanged = false;
  }
}
