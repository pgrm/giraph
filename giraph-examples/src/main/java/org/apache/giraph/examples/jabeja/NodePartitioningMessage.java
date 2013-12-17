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
 * Structure of messages sent between vertices
 */
public class NodePartitioningMessage extends BaseMessage {
  /**
   * The neighboring color ratio represents how many neighbors (value) have a
   * specific color (key)
   */
  private Map<Integer, Integer> neighboringColorRatio =
    new HashMap<Integer, Integer>();

  /**
   * The color of the vertex sending the message.
   */
  private int color;

  /**
   * The value calculated by JaBeJa-sum function
   * {@code NodePartitioningComputation.getJaBeJaSum()}
   */
  private double improvedNeighboringColorsValue = 0;

  /**
   * Default constructor for reflection
   */
  public NodePartitioningMessage() {
  }

  /**
   * Initializes the message for sending the current vertex color
   *
   * @param sourceId         the id of the vertex sending the message
   * @param color            the color of the vertex sending the message
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   */
  public NodePartitioningMessage(
    long sourceId, int color, boolean isRandomNeighbor) {
    super(sourceId, isRandomNeighbor, Type.ColorUpdate);

    this.color = color;
  }

  /**
   * Initialize the message for sending neighboring color ratios
   *
   * @param sourceId              the id of the vertex sending the message
   * @param neighboringColorRatio the neighboring color ratio of the vertex
   *                              sending this message
   * @param isRandomNeighbor      flag if it is a regular neighbor or one
   *                              from the random overlay
   */
  public NodePartitioningMessage(
    long sourceId, Map<Integer, Integer> neighboringColorRatio,
    boolean isRandomNeighbor) {
    super(sourceId, isRandomNeighbor, Type.DegreeUpdate);

    this.neighboringColorRatio = neighboringColorRatio;
  }

  /**
   * Initialize message for color exchange initialization
   *
   * @param sourceId                       the id of the vertex sending the
   *                                       message
   * @param improvedNeighboringColorsValue the new value calculated by
   *                                       JaBeJa-sum
   * @param isRandomNeighbor               flag if it is a regular neighbor
   *                                       or one from the random overlay
   */
  public NodePartitioningMessage(
    long sourceId, double improvedNeighboringColorsValue,
    boolean isRandomNeighbor) {
    super(sourceId, isRandomNeighbor, Type.ColorExchangeInitialization);

    this.improvedNeighboringColorsValue = improvedNeighboringColorsValue;
  }

  /**
   * Initialize the message with the source vertex id for color exchange
   * initialization
   *
   * @param sourceId         the id of the vertex sending the message
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   */
  public NodePartitioningMessage(long sourceId, boolean isRandomNeighbor) {
    super(sourceId, isRandomNeighbor, Type.ConfirmColorExchange);
  }

  public int getColor() {
    return color;
  }

  public double getImprovedNeighboringColorsValue() {
    return improvedNeighboringColorsValue;
  }

  public Map<Integer, Integer> getNeighboringColorRatio() {
    return neighboringColorRatio;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);

    switch (super.getMessageType()) {
    case ColorUpdate:
      this.color = dataInput.readInt();
      break;
    case DegreeUpdate:
      readNeighboringColorRatio(dataInput);
      break;
    case ColorExchangeInitialization:
      this.improvedNeighboringColorsValue = dataInput.readDouble();
      break;
    default:
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);

    switch (super.getMessageType()) {
    case ColorUpdate:
      dataOutput.writeInt(this.color);
      break;
    case DegreeUpdate:
      writeNeighboringColorRatio(dataOutput);
      break;
    case ColorExchangeInitialization:
      dataOutput.writeDouble(this.improvedNeighboringColorsValue);
      break;
    default:
    }
  }

  /**
   * read the neighboring color ratio map from the dataInput
   *
   * @param dataInput the input from {@code readFields}
   * @throws IOException the forwarded IOException from
   *                     {@code dataInput.readX()}
   */
  private void readNeighboringColorRatio(DataInput dataInput)
    throws IOException {

    super.readMap(dataInput, neighboringColorRatio, super.INTEGER_VALUE_READER,
      super.INTEGER_VALUE_READER);
  }

  /**
   * write the neighboring color ratio map to the dataOutput
   *
   * @param dataOutput the output from {@code write}
   * @throws IOException the forwarded IOException from
   *                     {@code dataOutput.writeX()}
   */
  private void writeNeighboringColorRatio(DataOutput dataOutput)
    throws IOException {

    super
      .writeMap(dataOutput, neighboringColorRatio, super.INTEGER_VALUE_WRITER,
        super.INTEGER_VALUE_WRITER);
  }

}
