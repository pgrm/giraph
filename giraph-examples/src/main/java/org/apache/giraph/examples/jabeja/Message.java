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
public class Message extends BaseWritable {
  /**
   * The neighboring color ratio represents how many neighbors (value) have a
   * specific color (key)
   */
  private Map<Integer, Integer> neighboringColorRatio =
    new HashMap<Integer, Integer>();

  /**
   * The type of this current message
   */
  private Type messageType = Type.Undefined;

  /**
   * Id of the source vertex sending the message. Necessary for replies or
   * knowing your neighbors.
   */
  private long vertexId;

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
  public Message() {
  }

  /**
   * Initializes the message for sending the current vertex color
   *
   * @param vertexId the id of the vertex sending the message
   * @param color    the color of the vertex sending the message
   */
  public Message(long vertexId, int color) {
    this(vertexId);

    this.color = color;
    this.messageType = Type.ColorUpdate;
  }

  /**
   * Initialize the message for sending neighboring color ratios
   *
   * @param vertexId              the id of the vertex sending the message
   * @param neighboringColorRatio the neighboring color ratio of the vertex
   *                              sending this message
   */
  public Message(long vertexId, Map<Integer, Integer> neighboringColorRatio) {
    this(vertexId);

    this.neighboringColorRatio = neighboringColorRatio;
    this.messageType = Type.DegreeUpdate;
  }

  /**
   * Initialize message for color exchange initialization
   *
   * @param vertexId                       the id of the vertex sending the
   *                                       message
   * @param improvedNeighboringColorsValue the new value calculated by
   *                                       JaBeJa-sum
   */
  public Message(long vertexId, double improvedNeighboringColorsValue) {
    this(vertexId);

    this.improvedNeighboringColorsValue = improvedNeighboringColorsValue;
    this.messageType = Type.ColorExchangeInitialization;
  }

  /**
   * Initialize the message with the source vertex id for color exchange
   * initialization
   *
   * @param vertexId the id of the vertex sending the message
   */
  public Message(long vertexId) {
    this.messageType = Type.ConfirmColorExchange;
    this.vertexId = vertexId;
  }

  public long getVertexId() {
    return vertexId;
  }

  public Type getMessageType() {
    return messageType;
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
    this.vertexId = dataInput.readLong();
    int typeValue = dataInput.readInt();

    this.messageType = Type.convertToType(typeValue);
    switch (this.messageType) {
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
    dataOutput.writeLong(this.vertexId);
    dataOutput.writeInt(this.messageType.getValue());

    switch (this.messageType) {
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

  /**
   * The possible types of this message
   */
  public enum Type {
    /**
     * Initial value
     */
    Undefined(-1),

    /**
     * Contains update about the nodes color
     */
    ColorUpdate(1),

    /**
     * Contains update about the nodes different colored degrees
     */
    DegreeUpdate(2),

    /**
     * Is initializing a color exchange
     */
    ColorExchangeInitialization(4),

    /**
     * To confirm an initialized color exchange
     */
    ConfirmColorExchange(8);

    /**
     * the int representation of the type, necessary for serialization
     */
    private final int value;

    /**
     * Private constructor of the type with the representing int value
     *
     * @param value the representing integer value of the enum type
     */
    private Type(int value) {
      this.value = value;
    }

    public int getValue() {
      return this.value;
    }

    /**
     * Converts the provided integer into the specific type,
     * necessary for parsing this enum
     *
     * @param value the representing integer value of the enum type
     * @return the actual enum type
     */
    public static Type convertToType(int value) {
      for (Type t : Type.values()) {
        if (t.getValue() == value) {
          return t;
        }
      }

      throw new IllegalArgumentException("The provided value doesn't have a " +
                                         "valid Type integer");
    }
  }
}
