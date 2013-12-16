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
 * Defines the base structure for all date necessary for vertices, for both
 * Edge- and Node-partitioning.
 */
public class VertexData extends BaseWritable {
  /**
   * The key is the id of the neighbor, the value is the color.
   * this property is stored
   */
  private final Map<Long, NeighborInformation> neighborInformation =
    new HashMap<Long, NeighborInformation>();

  /**
   * The key is the color, the value is how often the color exists.
   * this property is calculated
   */
  private Map<Integer, Integer> neighboringColorRatio;

  /**
   * Flag which indicates, if the colored degrees have changed since it has
   * been reset the last time.
   */
  private boolean haveColoredDegreesChanged;

  /**
   * Default constructor for reflection
   */
  public VertexData() {
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
   * Returns a list of IDs of all the neighbors.
   *
   * @return a list of IDs of all the neighbors.
   */
  public Iterable<Long> getNeighbors() {
    return this.neighborInformation.keySet();
  }

  public Map<Long, NeighborInformation> getNeighborInformation() {
    return neighborInformation;
  }

  /**
   * Update the neighborInformation map with a new or updated entry of a
   * neighbor and its color and set the flag
   * {@code haveColoredDegreesChanged} to true
   *
   * @param neighborId    the id of the neighbor (could be node or edge)
   * @param neighborColor the color of the neighbor
   */
  public void setNeighborWithColor(long neighborId, int neighborColor) {
    NeighborInformation info = this.neighborInformation.get(neighborId);

    if ((info != null && info.getColor() != neighborColor) || (info == null)) {
      if (info == null) {
        info = new NeighborInformation();
        this.neighborInformation.put(neighborId, info);
      }

      info.setColor(neighborColor);
      this.haveColoredDegreesChanged = true;
    }
  }

  /**
   * Updates the neighboring color ratio for the neighbor with the id
   * {@code neighborId}
   *
   * @param neighborId            the id of the neighbor (could be node or edge)
   * @param neighboringColorRatio the neighboring color ratio of this neighbor
   */
  public void setNeighborWithColorRatio(
    long neighborId, Map<Integer, Integer> neighboringColorRatio) {

    NeighborInformation info = this.neighborInformation.get(neighborId);

    if (info == null) {
      info = new NeighborInformation();
      this.neighborInformation.put(neighborId, info);
    }

    info.setNeighboringColorRatio(neighboringColorRatio);
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
   * Returns the number of neighbors
   *
   * @return the number of neighbors
   */
  public int getNumberOfNeighbors() {
    return this.neighborInformation.size();
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

  @Override
  public void readFields(DataInput input) throws IOException {
    readNeighboringColors(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    writeNeighboringColors(output);
  }

  /**
   * Initializes the histogram for colors of all neighbors.
   * How often each of the colors is represented between the neighbors.
   * If a color isn't represented, it's not in the final Map.
   */
  private void initializeNeighboringColorRatio() {
    this.neighboringColorRatio = new HashMap<Integer, Integer>();

    for (Map.Entry<Long, NeighborInformation> item : this.neighborInformation
      .entrySet()) {
      addColorToNeighboringColoRatio(item.getValue().getColor());
    }
  }

  /**
   * since the vertex doesn't have any information about its neighbors,
   * all the information needs to be stored in the vertex data. This
   * includes the colors of all the neighbors, to be able to compute the
   * algorithm for changing the color at all times
   *
   * @param input DataInput from readFields
   */
  private void readNeighboringColors(DataInput input) throws IOException {
    readMap(input, this.neighborInformation, LONG_VALUE_READER,
      new ValueReader<NeighborInformation>() {
        @Override
        public NeighborInformation readValue(DataInput input)
          throws IOException {
          NeighborInformation info = new NeighborInformation();

          info.setColor(input.readInt());
          readMap(input, info.getNeighboringColorRatio(),
            INTEGER_VALUE_READER, INTEGER_VALUE_READER);

          return info;
        }
      });
  }

  /**
   * Opposite to readNeighboringColors this method is to store the
   * color information of all the neighbors.
   *
   * @param output DataOutput from write
   */
  private void writeNeighboringColors(DataOutput output) throws IOException {
    writeMap(output, neighborInformation, LONG_VALUE_WRITER,
      new ValueWriter<NeighborInformation>() {
        @Override
        public void writeValue(
          DataOutput output, NeighborInformation value)
          throws IOException {
          output.writeInt(value.color);
          writeMap(output, value.getNeighboringColorRatio(),
            INTEGER_VALUE_WRITER, INTEGER_VALUE_WRITER);
        }
      });
  }

  public boolean getHaveColoredDegreesChanged() {
    return this.haveColoredDegreesChanged;
  }

  /**
   * Resets the {@code haveColoredDegreesChanged}-flag back to false
   */
  public void resetHaveColoredDegreesChanged() {
    this.haveColoredDegreesChanged = false;
  }

  /**
   * The value type of the map which stores the neighbor with their IDs and
   * additional information ({@code NeighborInformation}
   */
  public static class NeighborInformation {
    /**
     * The color of the neighbor
     */
    private int color;

    /**
     * The ratio of the neighboring colors of the neighbor
     */
    private Map<Integer, Integer> neighboringColorRatio =
      new HashMap<Integer, Integer>();

    public int getColor() {
      return color;
    }

    public void setColor(int color) {
      this.color = color;
    }

    public Map<Integer, Integer> getNeighboringColorRatio() {
      return neighboringColorRatio;
    }

    public void setNeighboringColorRatio(
      Map<Integer, Integer> neighboringColorRatio) {

      this.neighboringColorRatio = neighboringColorRatio;
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
  }
}
