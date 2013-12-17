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
package org.apache.giraph.examples.jabeja.io;

import org.apache.giraph.examples.jabeja.NodePartitioningVertexData;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A simple output format for vertices for the Node-Partitioning Problem
 */
public class NodePartitioningVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, NodePartitioningVertexData,
    IntWritable> {
  @Override
  public TextVertexWriter createVertexWriter(
    TaskAttemptContext context) throws IOException, InterruptedException {
    return new NodePartitioningVertexWriter();
  }

  /**
   * A debug output writer which provides you with all the information each
   * node has.
   */
  private class NodePartitioningVertexWriter
    extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
      Vertex<LongWritable, NodePartitioningVertexData,
        IntWritable> vertex) throws IOException {
      StringBuilder sb = new StringBuilder();
      NodePartitioningVertexData value = vertex.getValue();

      sb.append(vertex.getId());
      sb.append(":\t");
      sb.append(value.getNodeColor());
      sb.append("; ");
      sb.append(value.getNodeEnergy());
      sb.append("\t[");

      for (Long l : value.getNeighbors()) {
        sb.append(l);
        sb.append(", ");
      }
      sb.append("]");

      return new Text(sb.toString());
    }
  }
}
