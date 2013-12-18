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
   * The key is the id of the neighbor, the value is the color and further
   * information.
   * This property is stored
   */
  private final Map<Long, NeighborInformation> neighborInformation =
    new HashMap<Long, NeighborInformation>();

  /**
   * For a random overlay network on top of the graph to find even more nodes
   * to exchange the color with.
   * The key is the id of the random neighbor, the value is the color and
   * further information.
   * This property is stored
   */
  private final Map<Long, NeighborInformation> randomNeighborInformation =
    new HashMap<Long, NeighborInformation>();
  /**
   * Id of the vertex which has been chosen to exchange the color with
   */
  private long chosenPartnerIdForExchange;

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
   * Returns a list of IDs of all the neighbors.
   *
   * @return a list of IDs of all the neighbors.
   */
  public Iterable<Long> getNeighbors() {
    return this.neighborInformation.keySet();
  }

  /**
   * Returns a list of IDs of all the random neighbors.
   *
   * @return a list of IDs of all the random neighbors.
   */
  public Iterable<Long> getRandomNeighbors() {
    return this.randomNeighborInformation.keySet();
  }

  public Map<Long, NeighborInformation> getNeighborInformation() {
    return neighborInformation;
  }

  public Map<Long, NeighborInformation> getRandomNeighborInformation() {
    return randomNeighborInformation;
  }

  /**
   * Checks if a neighborId is random, if it returns false it doesn't mean
   * that it is a normal neighbor, just that it isn't a random one.
   *
   * @param neighborId the ID of the neighbor to be checked
   * @return true if it exists in the randomNeighborInformation-map,
   * false otherwise
   */
  public boolean isRandomNeighbor(long neighborId) {
    return this.randomNeighborInformation.containsKey(neighborId);
  }

  /**
   * Update the neighborInformation map with a new or updated entry of a
   * neighbor and its color and set the flag
   * {@code haveColoredDegreesChanged} to true
   *
   * @param neighborId       the id of the neighbor (could be node or edge)
   * @param neighborColor    the color of the neighbor
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   */
  public void setNeighborWithColor(
    long neighborId, int neighborColor, boolean isRandomNeighbor) {

    Map<Long, NeighborInformation> neighborsMap;

    if (isRandomNeighbor) {
      neighborsMap = this.randomNeighborInformation;
    } else {
      neighborsMap = this.neighborInformation;
    }

    NeighborInformation info = neighborsMap.get(neighborId);

    if ((info != null && info.getColor() != neighborColor) || (info == null)) {
      if (info == null) {
        info = new NeighborInformation();
        neighborsMap.put(neighborId, info);
      }

      info.setColor(neighborColor);

      if (!isRandomNeighbor) {
        this.haveColoredDegreesChanged = true;
      }
    }
  }

  /**
   * Updates the neighboring color ratio for the neighbor with the id
   * {@code neighborId}
   *
   * @param neighborId            the id of the neighbor (could be node or edge)
   * @param neighboringColorRatio the neighboring color ratio of this neighbor
   * @param isRandomNeighbor      flag if it is a regular neighbor or one
   *                              from the random overlay
   */
  public void setNeighborWithColorRatio(
    long neighborId, Map<Integer, Integer> neighboringColorRatio,
    boolean isRandomNeighbor) {

    Map<Long, NeighborInformation> neighborsMap;

    if (isRandomNeighbor) {
      neighborsMap = this.randomNeighborInformation;
    } else {
      neighborsMap = this.neighborInformation;
    }

    NeighborInformation info = neighborsMap.get(neighborId);

    if (info == null) {
      info = new NeighborInformation();
      neighborsMap.put(neighborId, info);
    }

    info.setNeighboringColorRatio(neighboringColorRatio);
  }

  /**
   * Returns the number of neighbors
   *
   * @return the number of neighbors
   */
  public int getNumberOfNeighbors() {
    return this.neighborInformation.size();
  }

  public long getChosenPartnerIdForExchange() {
    return chosenPartnerIdForExchange;
  }

  public void setChosenPartnerIdForExchange(long chosenPartnerIdForExchange) {
    this.chosenPartnerIdForExchange = chosenPartnerIdForExchange;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    readNeighboringColors(input, this.neighborInformation);
    readNeighboringColors(input, this.randomNeighborInformation);
    this.chosenPartnerIdForExchange = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    writeNeighboringColors(output, this.neighborInformation);
    writeNeighboringColors(output, this.randomNeighborInformation);
    output.writeLong(this.chosenPartnerIdForExchange);
  }

  /**
   * since the vertex doesn't have any information about its neighbors,
   * all the information needs to be stored in the vertex data. This
   * includes the colors of all the neighbors, to be able to compute the
   * algorithm for changing the color at all times
   *
   * @param input     DataInput from readFields
   * @param neighbors the map of neighbors into which data will be parsed
   */
  private void readNeighboringColors(
    DataInput input,
    Map<Long, NeighborInformation> neighbors) throws IOException {

    readMap(input, neighbors, LONG_VALUE_READER,
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
   * @param output    DataOutput from write
   * @param neighbors the map of neighbors from which data will be serialized
   */
  private void writeNeighboringColors(
    DataOutput output,
    Map<Long, NeighborInformation> neighbors) throws IOException {

    writeMap(output, neighbors, LONG_VALUE_WRITER,
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
