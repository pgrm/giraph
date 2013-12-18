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
import java.util.HashMap;
import java.util.Map;

/**
 * Vertex data for the NodePartitioning solution.
 */
public class NodePartitioningVertexData extends VertexData {
  /**
   * The color of the current node
   */
  private int nodeColor;

  /**
   * Flag, which indicates if the color has changed since it has been reset
   * the last time
   */
  private boolean hasColorChanged;
  /**
   * The key is the color, the value is how often the color exists.
   * this property is calculated
   */
  private Map<Integer, Integer> neighboringColorRatio;

  /**
   * Default constructor for reflection
   */
  public NodePartitioningVertexData() {
    super();
  }

  public int getNodeColor() {
    return nodeColor;
  }

  /**
   * Sets the new node color and checks if it has changed,
   * in that case it also sets the flag {@code hasColorChanged}
   *
   * @param nodeColor the new color of the current vertex
   */
  public void setNodeColor(int nodeColor) {
    if (this.nodeColor != nodeColor) {
      this.nodeColor = nodeColor;
      this.hasColorChanged = true;
    }
  }

  /**
   * Calculates the energy of the node according to <code>the number of
   * neighbors - the number of neighbors in the same color</code>
   *
   * @return the energy of the current node (how many neighbors are of a
   * different color)
   */
  public int getNodeEnergy() {
    return super.getNumberOfNeighbors() -
           getNumberOfNeighborsWithCurrentColor();
  }

  /**
   * Simply calls <code>getNumberOfNeighbors</code> with the parameter of the
   * current color.
   *
   * @return the number of neighbors which have the same color as the current
   * node.
   */
  public int getNumberOfNeighborsWithCurrentColor() {
    return getNumberOfNeighbors(getNodeColor());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);

    this.nodeColor = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);

    output.writeInt(this.nodeColor);
  }

  @Override
  public String toString() {
    return Integer.toString(this.nodeColor);
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

  /**
   * @return data for a histogram for the colors of all neighbors.
   * How often each of the colors is represented between the neighbors.
   * If a color isn't represented, it's not in the final Map.
   */
  public Map<Integer, Integer> getNeighboringColorRatio() {
    if (this.neighboringColorRatio == null) {
      initializeNeighboringColorRatio();
    }

    return this.neighboringColorRatio;
  }

  /**
   * Check if the color already exists in the neighboringColorRatio-map. If
   * not, create a new entry with the count 1, if yes than update the count +1
   *
   * @param color the color of one neighboring item
   */
  private void addColorToNeighboringColoRatio(int color) {
    Integer numberOfColorAppearances = this.neighboringColorRatio.get(color);

    if (numberOfColorAppearances == null) {
      numberOfColorAppearances = 1;
    } else {
      numberOfColorAppearances++;
    }

    this.neighboringColorRatio.put(color, numberOfColorAppearances);
  }

  /**
   * Gets the number of neighbors in a specific color
   *
   * @param color color of the neighbors
   * @return the number of neighbors in the color <code>color</code>
   */
  public int getNumberOfNeighbors(int color) {
    Integer numberOfNeighborsInColor =
      getNeighboringColorRatio().get(color);

    if (numberOfNeighborsInColor == null) {
      return 0;
    } else {
      return numberOfNeighborsInColor.intValue();
    }
  }

  /**
   * Initializes the histogram for colors of all neighbors.
   * How often each of the colors is represented between the neighbors.
   * If a color isn't represented, it's not in the final Map.
   */
  private void initializeNeighboringColorRatio() {
    this.neighboringColorRatio = new HashMap<Integer, Integer>();

    for (Map.Entry<Long, NeighborInformation> item :
      super.getNeighborInformation().entrySet()) {

      addColorToNeighboringColoRatio(item.getValue().getColor());
    }
  }
}
