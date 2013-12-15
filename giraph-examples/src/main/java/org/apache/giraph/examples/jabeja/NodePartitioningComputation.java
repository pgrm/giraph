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

import java.io.IOException;
import java.util.Random;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation
    extends
    BasicComputation<LongWritable, NodePartitioningVertexData, IntWritable, Message> {
  /**
   * The default number of different colors, if no JaBeJa.NumberOfColors is
   * provided.
   */
  private static final int DEFAULT_NUMBER_OF_COLORS = 2;

  /**
   * The default number for many supersteps this algorithm should run, if no
   * JaBeJa.MaxNumberOfSupersteps is provided
   */
  private static final int DEFAULT_MAX_NUMBER_OF_SUPERSTEPS = 100;

  /**
   * The actually defined maximum number of supersteps for how many this
   * algorithm should run
   */
  private static Integer MAX_NUMBER_OF_SUPERSTEPS = null;
  /**
   * Since the JaBeJa algorithm is a Monte Carlo algorithm.
   */
  private Random randomGenerator = new Random();
  /**
   * The currently processed vertex
   */
  private Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex;

  @Override
  public void compute(
      Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex,
      Iterable<Message> messages) throws IOException {
    this.vertex = vertex;

    if (super.getSuperstep() < 2) {
      initializeGraph(messages);
    } else {
      // the 2nd step (mode == 1) is the same as the first step after the
      // initialization (Superstep == 2) since at this time we get the final
      // colors and can announce the degree in case it has changed.
      int mode = (int) ((super.getSuperstep() - 1) % 5);

      runJaBeJaAlgorithm(mode, messages);
    }

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
    }
  }

  /**
   * This function is used in the first stage, when the graph is being
   * initialized. It contains features for taking a random color as well as
   * announcing the color and finding all neighbors.
   * 
   * @param messages
   *          The messages sent to this Vertex
   */
  private void initializeGraph(Iterable<Message> messages) {
    if (getSuperstep() == 0) {
      initializeColor();
      announceColor();
    } else {
      storeColorsOfNodes(messages);
      announceColorToNewNeighbors(messages);
    }
  }

  /**
   * After the graph has been initialized in the first 2 steps, this function is
   * executed and will run the actual JaBeJa algorithm
   * 
   * @param mode
   *          The JaBeJa algorithm has multiple rounds, and in each round
   *          several sub-steps are performed, mode indicates which sub-step
   *          should be performed.
   * @param messages
   *          The messages sent to this Vertex
   */
  private void runJaBeJaAlgorithm(int mode, Iterable<Message> messages) {
    switch (mode) {
    case 0: // new round started, announce your new color
      announceColorIfChanged();
      break;
    case 1:
      // update colors of your neighboring nodes,
      // and announce your new degree for each color
      storeColorsOfNodes(messages);
      announceColoredDegreesIfChanged();
      break;
    case 2:
      // updated colored degrees of all nodes and find a partner to
      // initiate the exchange with
      storeColoredDegreesOfNodes(messages);
      Long partnerId = findPartner();

      if (partnerId != null) {
        initiateColoExchangeHandshake(partnerId);
      } else {
        // if no partner could be found, this node probably has already the
        // best possible color
        this.vertex.voteToHalt();
      }
      break;
    default:
      mode = mode - 3;
      continueColorExchange(mode, messages);
    }
  }

  /**
   * Checks if it is time to stop (if enough steps have been done)
   * 
   * @return true if it is time to stop, if the number of supersteps exceeds the
   *         maximum allowed number
   */
  private boolean isTimeToStop() {
    return getSuperstep() > getMaxNumberOfSuperSteps();
  }

  /**
   * Set the color of the own node.
   */
  private void initializeColor() {
    int numberOfColors = getNumberOfColors();
    int myColor = (int) getRandomNumber(numberOfColors);

    this.vertex.getValue().setNodeColor(myColor);
  }

  /**
   * Announces the color, only if it has changed after it has been announced the
   * last time
   */
  private void announceColorIfChanged() {
    if (this.vertex.getValue().getHasColorChanged()) {
      announceColor();
      this.vertex.getValue().resetHasColorChanged();
    }
  }

  /**
   * Announce the current color to all connected vertices.
   */
  private void announceColor() {
    // This should be vertex.vertex.getEdges();. Because I didn't see any code
// populating the vertex's neighbours.
    for (Long neighborId : this.vertex.getValue().getNeighbors()) {
      sendCurrentVertexColor(new LongWritable(neighborId));
    }
  }

  /**
   * Store the color of all neighboring nodes. Neighboring through outgoing as
   * well as incoming edges.
   * 
   * @param messages
   *          received messages from nodes with their colors.
   */
  private void storeColorsOfNodes(Iterable<Message> messages) {
    for (Message msg : messages) {
      this.vertex.getValue().setNeighborWithColor(msg.getVertexId(),
          msg.getColor());
    }
  }

  /**
   * Reply to all received messages with the current color.
   * 
   * @param messages
   *          all received messages.
   */
  private void announceColorToNewNeighbors(Iterable<Message> messages) {
    for (Message msg : messages) {
      sendCurrentVertexColor(new LongWritable(msg.getVertexId()));
    }
  }

  /**
   * Announces the different colored degrees, only if they have changed after
   * they have been announced the last time.
   */
  private void announceColoredDegreesIfChanged() {
    if (this.vertex.getValue().getHaveColoredDegreesChanged()) {
      announceColoredDegrees();
      this.vertex.getValue().resetHaveColoredDegreesChanged();
    }
  }

  /**
   * Announces the different colored degrees to all its neighbors.
   */
  private void announceColoredDegrees() {
    for (Long neighborId : this.vertex.getValue().getNeighbors()) {
      super.sendMessage(new LongWritable(neighborId), new Message(this.vertex
          .getId().get(), this.vertex.getValue().getNeighboringColorRatio()));
    }
  }

  /**
   * Updates the information about different colored degrees of its neighbors
   * 
   * @param messages
   *          The messages sent to this Vertex containing the colored degrees
   */
  private void storeColoredDegreesOfNodes(Iterable<Message> messages) {
    for (Message message : messages) {
      this.vertex.getValue().setNeighborWithColorRatio(message.getVertexId(),
          message.getNeighboringColorRatio());
    }
  }

  /**
   * TODO Find a partner to exchange the colors with
   * 
   * @return the vertex ID of the partner with whom the colors will be exchanged
   */
  private Long findPartner() {
    return 0L;
  }

  /**
   * TODO initialized the color-exchange handshake
   * 
   * @param partnerId
   *          the id of the vertex with whom I want to exchange colors
   */
  private void initiateColoExchangeHandshake(long partnerId) {
  }

  /**
   * TODO This is the part of the JaBeJa algorithm which implements the full
   * color exchange
   * 
   * @param mode
   *          the sub-step of the color exchange
   * @param messages
   *          The messages sent to this Vertex
   */
  private void continueColorExchange(int mode, Iterable<Message> messages) {
  }

  /**
   * Checks if the Maximum Number of Supersteps has been configured, otherwise
   * uses the default value.
   * 
   * @return the maximum number of supersteps for which the algorithm should run
   */
  private long getMaxNumberOfSuperSteps() {
    if (MAX_NUMBER_OF_SUPERSTEPS == null) {
      MAX_NUMBER_OF_SUPERSTEPS = super.getConf().getInt(
          "JaBeJa.MaxNumberOfSupersteps", DEFAULT_MAX_NUMBER_OF_SUPERSTEPS);
    }
    return MAX_NUMBER_OF_SUPERSTEPS;
  }

  /**
   * Send a message to a vertex with the current color and id, so that this
   * vertex would be able to reply.
   * 
   * @param targetId
   *          id of the vertex to which the message should be sent
   */
  private void sendCurrentVertexColor(LongWritable targetId) {
    super.sendMessage(targetId, new Message(this.vertex.getId().get(),
        this.vertex.getValue().getNodeColor()));
  }

  /**
   * @return the configured or default number of different colors in the current
   *         graph.
   */
  private int getNumberOfColors() {
    return super.getConf().getInt("JaBeJa.NumberOfColors",
        DEFAULT_NUMBER_OF_COLORS);
  }

  /**
   * generates a 64bit random number within an upper boundary.
   * 
   * @param exclusiveMaxValue
   *          the exclusive maximum value of the to be generated random value
   * @return a random number between 0 (inclusive) and exclusiveMaxValue
   *         (exclusive)
   */
  private long getRandomNumber(long exclusiveMaxValue) {
    if (this.randomGenerator == null) {
      initializeRandomGenerator();
    }
    return (long) (this.randomGenerator.nextDouble() * exclusiveMaxValue);
  }

  /**
   * Initializes the random generator, either with the default constructor or
   * with a seed which will guarantee, that the same vertex in the same round
   * will always generate the same random values.
   */
  private void initializeRandomGenerator() {
    int configuredSeed = super.getConf().getInt("JaBeJa.RandomSeed", 0);

    // if you want to have a totally random number just set the config to -1
    if (configuredSeed == -1) {
      this.randomGenerator = new Random();
    } else {
      long seed = calculateHashCode(String.format("%d#%d#%d", configuredSeed,
          super.getSuperstep(), this.vertex.getId().get()));
      this.randomGenerator = new Random(seed);
    }
  }

  /**
   * Based on the hashCode function defined in {@link java.lang.String}, with
   * the difference that it returns a long instead of only an integer.
   * 
   * @param value
   *          the String for which the hash-code should be calculated
   * @return a 64bit hash-code
   */
  private static long calculateHashCode(String value) {
    int hashCode = 0;

    for (int i = 0; i < value.length(); i++) {
      hashCode = 31 * hashCode + value.charAt(i);
    }

    return hashCode;
  }
}
