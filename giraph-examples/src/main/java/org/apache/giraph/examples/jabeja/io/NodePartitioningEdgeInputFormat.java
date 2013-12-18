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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * A simple InputFormat VertexA \t VertexB to parse real data from social
 * networks
 */
public class NodePartitioningEdgeInputFormat extends
  TextEdgeInputFormat<LongWritable, IntWritable> {
  @Override
  public EdgeReader<LongWritable, IntWritable> createEdgeReader(
    InputSplit split, TaskAttemptContext context) throws IOException {
    return new NodePartitioningEdgeReader();
  }

  /**
   * A simple InputFormat VertexA \t VertexB to parse real data from social
   * networks
   */
  protected class NodePartitioningEdgeReader extends
    TextEdgeReaderFromEachLine {

    @Override
    protected LongWritable getSourceVertexId(Text line) throws IOException {
      long sourceId = Long.parseLong(line.toString().split("\t")[0]);
      return new LongWritable(sourceId);
    }

    @Override
    protected LongWritable getTargetVertexId(Text line) throws IOException {
      long sourceId = Long.parseLong(line.toString().split("\t")[1]);
      return new LongWritable(sourceId);
    }

    @Override
    protected IntWritable getValue(Text line) throws IOException {
      return new IntWritable(0);
    }
  }
}
