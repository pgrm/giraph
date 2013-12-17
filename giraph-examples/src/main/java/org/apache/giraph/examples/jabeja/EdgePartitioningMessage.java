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
import java.util.List;

/**
 *
 */
public class EdgePartitioningMessage extends BaseMessage {
  private List<Edge<LongWritable, EdgePartitioningEdgeData>> connectedEdges =
    new ArrayList<Edge<LongWritable, EdgePartitioningEdgeData>>();

  public EdgePartitioningMessage(
    long sourceId, boolean isRandomNeighbor,
    Iterable<Edge<LongWritable, EdgePartitioningEdgeData>> edges) {

    super(sourceId, isRandomNeighbor, Type.ColorUpdate);

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge : edges) {
      connectedEdges.add(edge);
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);

    switch (super.getMessageType()) {
    case ColorUpdate:
      writeEdges(dataOutput);
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
    default:
    }
  }

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
}
