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
   * Id of the vertex which has been chosen to exchange the color with
   */
  private long chosenPartnerIdForExchange;

  /**
   * Default constructor for reflection
   */
  public NodePartitioningVertexData() {
    super();
  }

  public int getNodeColor() {
    return nodeColor;
  }

  public long getChosenPartnerIdForExchange() {
    return chosenPartnerIdForExchange;
  }

  public void setChosenPartnerIdForExchange(long chosenPartnerIdForExchange) {
    this.chosenPartnerIdForExchange = chosenPartnerIdForExchange;
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
    return super.getNumberOfNeighbors(getNodeColor());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);

    this.nodeColor = input.readInt();
    this.chosenPartnerIdForExchange = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);

    output.writeInt(this.nodeColor);
    output.writeLong(this.chosenPartnerIdForExchange);
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
}
