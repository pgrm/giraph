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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * structure of messages sent between vertices, to communicate about nodes
 */
public class EdgePartitioningMessage extends BaseMessage {
  /**
   * id of the remote partner edge which shall switch the color
   */
  private long partnerEdgeId;

  /**
   * the value by which the local ratios would be improved
   */
  private double improvedNeighboringColorsValue;

  /**
   * the color ratios of the edge in which name the message is sent
   */
  private Map<Integer, Integer> neighboringColorRatio =
    new HashMap<Integer, Integer>();

  /**
   * the edges connected to the source vertex to update them in the local
   * neighbor collection
   */
  private List<Edge<LongWritable, EdgePartitioningEdgeData>> connectedEdges =
    new ArrayList<Edge<LongWritable, EdgePartitioningEdgeData>>();

  /**
   * Default constructor for reflection
   */
  public EdgePartitioningMessage() {
  }

  /**
   * Constructor to send an update of the edges to neighbors
   *
   * @param sourceId         the id of the vertex sending the message
   * @param isRandomNeighbor flag if it is a random or normal neighbor
   * @param edges            the changed edges, in the beginning all,
   *                         later on only one by one
   */
  public EdgePartitioningMessage(
    long sourceId, boolean isRandomNeighbor,
    Iterable<Edge<LongWritable, EdgePartitioningEdgeData>> edges) {

    super(sourceId, isRandomNeighbor, Type.ColorUpdate);

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge : edges) {
      connectedEdges.add(edge);
    }
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
  public EdgePartitioningMessage(
    long sourceId, Map<Integer, Integer> neighboringColorRatio,
    boolean isRandomNeighbor) {
    super(sourceId, isRandomNeighbor, Type.DegreeUpdate);

    this.neighboringColorRatio = neighboringColorRatio;
  }

  /**
   * Constructor for initializing the color exchange
   *
   * @param localEdgeId                    id of the local edge which will
   *                                       switch the color
   * @param partnerEdgeId                  id of the remote partner edge
   *                                       which shall switch the color
   * @param improvedNeighboringColorsValue the value by which the local
   *                                       ratios would be improved
   * @param isRandomNeighbor               flag if it is a random or normal
   *                                       neighbor
   */
  public EdgePartitioningMessage(
    long localEdgeId, long partnerEdgeId,
    double improvedNeighboringColorsValue, boolean isRandomNeighbor) {
    super(localEdgeId, isRandomNeighbor, Type.ColorExchangeInitialization);

    this.partnerEdgeId = partnerEdgeId;
    this.improvedNeighboringColorsValue = improvedNeighboringColorsValue;
  }

  /**
   * Constructor for confirming a color exchange
   *
   * @param localEdgeId      id of the local edge which will switch the color
   * @param partnerEdgeId    id of the remote partner edge which shall switch
   *                         the color
   * @param isRandomNeighbor flag if it is a random or normal neighbor
   */
  public EdgePartitioningMessage(
    long localEdgeId, long partnerEdgeId, boolean isRandomNeighbor) {
    super(localEdgeId, isRandomNeighbor, Type.ConfirmColorExchange);

    this.partnerEdgeId = partnerEdgeId;
  }

  public List<Edge<LongWritable, EdgePartitioningEdgeData>>
  getConnectedEdges() {
    return connectedEdges;
  }

  public void setConnectedEdges(
    List<Edge<LongWritable, EdgePartitioningEdgeData>> connectedEdges) {
    this.connectedEdges = connectedEdges;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);

    switch (super.getMessageType()) {
    case ColorUpdate:
      writeEdges(dataOutput);
      break;
    case DegreeUpdate:
      writeNeighboringColorRatio(dataOutput);
      break;
    case ColorExchangeInitialization:
      dataOutput.writeLong(this.partnerEdgeId);
      dataOutput.writeDouble(this.improvedNeighboringColorsValue);
      break;
    default:
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);

    switch (super.getMessageType()) {
    case ColorUpdate:
      readEdges(dataInput);
      break;
    case DegreeUpdate:
      readNeighboringColorRatio(dataInput);
      break;
    case ColorExchangeInitialization:
      this.partnerEdgeId = dataInput.readLong();
      this.improvedNeighboringColorsValue = dataInput.readDouble();
      break;
    default:
    }
  }

  /**
   * serializes and writes edges from the connectedEdges collection
   *
   * @param output DataOutput from {@code write}
   * @throws IOException forwarded exception from output.writeX
   */
  private void writeEdges(DataOutput output) throws IOException {
    super.writeCollection(output, connectedEdges,
      new ValueWriter<Edge<LongWritable, EdgePartitioningEdgeData>>() {

        @Override
        public void writeValue(
          DataOutput output, Edge<LongWritable, EdgePartitioningEdgeData> value)
          throws IOException {

          value.getTargetVertexId().write(output);
          value.getValue().write(output);
        }
      });
  }

  /**
   * reads and parses edges into the connectedEdges collection
   *
   * @param input DataInput from {@code readFields}
   * @throws IOException forwarded exception from input.readX
   */
  private void readEdges(DataInput input) throws IOException {
    super.readCollection(input, connectedEdges,
      new ValueReader<Edge<LongWritable, EdgePartitioningEdgeData>>() {

        @Override
        public Edge<LongWritable, EdgePartitioningEdgeData> readValue(
          DataInput input) throws IOException {

          EdgePartitioningComputation.SimpleEdge<LongWritable,
            EdgePartitioningEdgeData> edge = new EdgePartitioningComputation
            .SimpleEdge<LongWritable, EdgePartitioningEdgeData>();
          LongWritable id = new LongWritable();
          EdgePartitioningEdgeData data = new EdgePartitioningEdgeData();

          id.readFields(input);
          data.readFields(input);

          edge.setTargetVertexId(id);
          edge.setValue(data);

          return edge;
        }
      });
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

  public Map<Integer, Integer> getNeighboringColorRatio() {
    return neighboringColorRatio;
  }

  public double getImprovedNeighboringColorsValue() {
    return improvedNeighboringColorsValue;
  }

  public long getPartnerEdgeId() {
    return partnerEdgeId;
  }
}
