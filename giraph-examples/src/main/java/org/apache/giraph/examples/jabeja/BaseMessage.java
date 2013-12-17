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
public abstract class BaseMessage extends BaseWritable {
  /**
   * Id of the source vertex sending the message. Necessary for replies or
   * knowing your neighbors.
   */
  private long sourceId;
  /**
   * The type of this current message
   */
  private Type messageType = Type.Undefined;
  /**
   * Flag to set for the initialization to see if the message comes from a
   * normal or random neighbor
   */
  private boolean isRandomNeighbor;

  /**
   * Default constructor for reflection
   */
  public BaseMessage() {
  }

  /**
   * Initialize the message with the source vertex id for color exchange
   * initialization
   *
   * @param sourceId         the id of the vertex sending the message
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   * @param messageType      The type of the message transferred
   */
  public BaseMessage(
    long sourceId, boolean isRandomNeighbor, Type messageType) {

    this.sourceId = sourceId;
    this.isRandomNeighbor = isRandomNeighbor;
    this.messageType = messageType;
  }

  public long getSourceId() {
    return sourceId;
  }

  public Type getMessageType() {
    return messageType;
  }

  public boolean isRandomNeighbor() {
    return isRandomNeighbor;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(this.sourceId);
    dataOutput.writeInt(this.messageType.getValue());
    dataOutput.writeBoolean(this.isRandomNeighbor);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.sourceId = dataInput.readLong();
    int typeValue = dataInput.readInt();
    this.isRandomNeighbor = dataInput.readBoolean();

    this.messageType = Type.convertToType(typeValue);
  }

  /**
   * The possible types of this message
   */
  public static enum Type {
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
