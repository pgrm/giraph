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

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Map;

/**
 *
 */
public class EdgePartitioningComputation extends
  GraphPartitioningComputation<EdgePartitioningVertexData,
    EdgePartitioningEdgeData, EdgePartitioningMessage> {

  @Override
  protected void initializeColor() {
    int numberOfColors = super.conf.getNumberOfColors();
    long index = 1;
    long edgeId;
    int edgeColor;

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      this.vertex.getEdges()) {

      edgeId = index * super.getTotalNumVertices() + super.vertex.getId().get();
      edgeColor = (int) getRandomNumber(numberOfColors);
      edge.getValue().setEdgeId(edgeId);
      edge.getValue().setEdgeColor(edgeColor);
      index++;

      LOG.trace("Chose color " + edgeColor + " out of " + numberOfColors +
                " colors and ID " + edgeId);
    }
  }

  @Override
  protected void initializeRandomNeighbors() {
    int numberOfRandomNeighbors = super.conf.getNumberOfRandomNeighbors();

    if (numberOfRandomNeighbors > 0) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void announceInitialColor() {
    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      this.vertex.getEdges()) {

      super.sendMessage(edge.getTargetVertexId(), new EdgePartitioningMessage
        (this.vertex.getId().get(), false, this.vertex.getEdges()));
    }
  }

  @Override
  protected void storeColorsOfNodes(
    Iterable<EdgePartitioningMessage> messages) {

    for(EdgePartitioningMessage msg : messages) {
      for(Edge<LongWritable, >)
    }
  }

  @Override
  protected void announceColorToNewNeighbors(
    Iterable<EdgePartitioningMessage> messages) {

  }

  @Override
  protected void announceColoredDegreesIfChanged() {

  }

  @Override
  protected void storeColoredDegreesOfNodes(
    Iterable<EdgePartitioningMessage> messages) {

  }

  @Override
  protected Map.Entry<Long, Double> findPartner() {
    return null;
  }

  @Override
  protected void initiateColoExchangeHandshake(
    Map.Entry<Long, Double> partner, boolean isRandomNeighbor) {

  }

  @Override
  protected void continueColorExchange(
    int mode, Iterable<EdgePartitioningMessage> messages) {

  }

  public static class SimpleEdge<I extends WritableComparable,
    E extends Writable> implements Edge<I, E> {

    private I targetVertexId;
    private E value;

    @Override
    public I getTargetVertexId() {
      return this.targetVertexId;
    }

    @Override
    public E getValue() {
      return this.value;
    }

    public void setTargetVertexId(I targetVertexId) {
      this.targetVertexId = targetVertexId;
    }

    public void setValue(E value) {
      this.value = value;
    }
  }
}
