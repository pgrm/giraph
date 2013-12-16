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
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Random;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation extends
  BasicComputation<LongWritable, NodePartitioningVertexData,
    IntWritable, Message> {
  /**
   * How many additional steps are performed during initialization.
   * There is only one additional step during the initialization,
   * since it is necessary to collect neighbors who direct their edges to you.
   */
  private static final int NUMBER_OF_STEPS_DURING_INITIALIZATION = 2;

  /**
   * TODO fix this value
   */
  private static final int NUMBER_OF_STEPS_PER_JABEJA_ROUND = 3;

  /**
   * Since the JaBeJa algorithm is a Monte Carlo algorithm.
   */
  private Random randomGenerator = new Random();

  /**
   * The current temperature at the current superstep and JaBeJa round
   */
  private Double temperature = null;

  /**
   * An instance of the helper class to handle configurations.
   */
  private JaBeJaConfigurations conf = null;

  /**
   * The currently processed vertex
   */
  private Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex;

  @Override
  public void compute(
    Vertex<LongWritable, NodePartitioningVertexData, IntWritable> vertex,
    Iterable<Message> messages) throws IOException {

    if (this.conf == null) {
      this.conf = new JaBeJaConfigurations(super.getConf());
    }
    this.vertex = vertex;

    if (super.getSuperstep() < 2) {
      initializeGraph(messages);
    } else {
      // the 1st step (mode == 0) is the same as the first step after the
      // initialization (Superstep == 2) since at this time we get the final
      // colors and can announce the degree in case it has changed.
      // also note that announcing colors again, is the last step for JaBeJa
      // and kind of a restart step
      int mode = (int)
        ((super.getSuperstep() - NUMBER_OF_STEPS_DURING_INITIALIZATION) %
         NUMBER_OF_STEPS_PER_JABEJA_ROUND);

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
   * @param messages The messages sent to this Vertex
   */
  private void initializeGraph(Iterable<Message> messages) {
    if (super.getSuperstep() == 0) {
      initializeColor();
      announceInitialColor();
    } else {
      storeColorsOfNodes(messages);
      announceColorToNewNeighbors(messages);
    }
  }

  /**
   * After the graph has been initialized in the first 2 steps,
   * this function is executed and will run the actual JaBeJa algorithm
   *
   * @param mode     The JaBeJa algorithm has multiple rounds,
   *                 and in each round several sub-steps are performed,
   *                 mode indicates which sub-step should be performed.
   * @param messages The messages sent to this Vertex
   */
  private void runJaBeJaAlgorithm(int mode, Iterable<Message> messages) {
    switch (mode) {
    case 0:
      // update colors of your neighboring nodes,
      // and announce your new degree for each color
      storeColorsOfNodes(messages);
      announceColoredDegreesIfChanged();
      break;
    case 1:
      // updated colored degrees of all nodes and find a partner to
      // initiate the exchange with
      storeColoredDegreesOfNodes(messages);
      Map.Entry<Long, Double> partner = findPartner();

      if (partner.getKey() != null) {
        this.vertex.getValue().setChosenPartnerIdForExchange(partner.getKey());
        initiateColoExchangeHandshake(partner);
      } else {
        this.vertex.getValue().setChosenPartnerIdForExchange(-1);
        // if no partner could be found, this node probably has already the
        // best possible color
        this.vertex.voteToHalt();
      }
      break;
    default:
      mode = mode - 2;
      continueColorExchange(mode, messages);
    }
  }

  /**
   * Checks if it is time to stop (if enough steps have been done)
   *
   * @return true if it is time to stop, if the number of supersteps exceeds
   * the maximum allowed number
   */
  private boolean isTimeToStop() {
    return getSuperstep() > this.conf.getMaxNumberOfSuperSteps();
  }

  /**
   * Set the color of the own node.
   */
  private void initializeColor() {
    int numberOfColors = this.conf.getNumberOfColors();
    int myColor = (int) getRandomNumber(numberOfColors);

    this.vertex.getValue().setNodeColor(myColor);
  }

  /**
   * Announces the color, only if it has changed after it has been announced
   * the last time
   */
  private void announceColorIfChanged() {
    if (this.vertex.getValue().getHasColorChanged()) {
      for (Long neighborId : this.vertex.getValue().getNeighbors()) {
        sendCurrentVertexColor(new LongWritable(neighborId));
      }
      this.vertex.getValue().resetHasColorChanged();
    }
  }

  /**
   * Announce the current color to all connected vertices for the first time.
   */
  private void announceInitialColor() {
    for (Edge<LongWritable, IntWritable> edge : this.vertex.getEdges()) {
      sendCurrentVertexColor(edge.getTargetVertexId());
    }
  }

  /**
   * Store the color of all neighboring nodes. Neighboring through outgoing
   * as well as incoming edges.
   *
   * @param messages received messages from nodes with their colors.
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
   * @param messages all received messages.
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
      super.sendMessage(new LongWritable(neighborId),
        new Message(this.vertex.getId().get(),
          this.vertex.getValue().getNeighboringColorRatio()));
    }
  }

  /**
   * Updates the information about different colored degrees of its neighbors
   *
   * @param messages The messages sent to this Vertex containing the colored
   *                 degrees
   */
  private void storeColoredDegreesOfNodes(Iterable<Message> messages) {
    for (Message message : messages) {
      this.vertex.getValue().setNeighborWithColorRatio(
        message.getVertexId(), message.getNeighboringColorRatio());
    }
  }

  /**
   * Finds the best partner to exchange their colors with each other.
   *
   * @return the vertex ID of the partner with whom the colors will be
   * exchanged
   */
  private Map.Entry<Long, Double> findPartner() {
    double highest = 0;
    Long bestPartner = null;
    NodePartitioningVertexData data = this.vertex.getValue();

    for (Map.Entry<Long, VertexData.NeighborInformation> neighbor :
      data.getNeighborInformation().entrySet()) {

      int myDegree = data.getNumberOfNeighborsWithCurrentColor();
      int neighborsDegree =
        neighbor.getValue().getNumberOfNeighbors(
          neighbor.getValue().getColor());

      int myNewDegree =
        data.getNumberOfNeighbors(neighbor.getValue().getColor());
      int neighborsNewDegree =
        neighbor.getValue().getNumberOfNeighbors(data.getNodeColor());

      double sum = getJaBeJaSum(myDegree, neighborsDegree);
      double newSum = getJaBeJaSum(myNewDegree, neighborsNewDegree);

      if (isNewColorBetterThanOld(newSum, sum) && newSum > highest) {
        bestPartner = neighbor.getKey();
        highest = newSum;
      }
    }

    return new AbstractMap.SimpleEntry<Long, Double>(bestPartner, highest);
  }

  /**
   * initialize the color-exchange handshake by sending a simple message to
   * the partner
   *
   * @param partner the vertex with whom I want to exchange colors
   *                (key = vertex id,
   *                value = JaBeJa-sum for the finished exchange)
   */
  private void initiateColoExchangeHandshake(Map.Entry<Long, Double> partner) {
    super.sendMessage(new LongWritable(partner.getKey()),
      new Message(partner.getKey(), partner.getValue()));
  }

  /**
   * TODO This is the part of the JaBeJa algorithm which implements the full
   * color exchange
   *
   * @param mode     the sub-step of the color exchange
   * @param messages The messages sent to this Vertex
   */
  private void continueColorExchange(int mode, Iterable<Message> messages) {
    switch (mode) {
    case 0:
      Long partnerId = getBestOfferedPartnerIdForExchange(messages);

      if (partnerId != null) {
        long desiredPartnerId =
          this.vertex.getValue().getChosenPartnerIdForExchange();

        if (partnerId == desiredPartnerId) {
          exchangeColors(partnerId);
        } else if (this.conf.useComplexColorExchange()) {
          throw new UnsupportedOperationException();
          //confirmColorExchangeWithPartner(partnerId);
        }
      }
      break;
    default:
    }

    // restart the algorithm from the beginning again
    if (mode == 3 || (mode == 0 && this.conf.useComplexColorExchange())) {
      announceColorIfChanged();
    }
  }

  /**
   * Selects the partner id from incoming offers (messages) so that it is
   * either the desired one (best choice) or otherwise the one with the
   * highest value. If the desired partner is found, it means the other node
   * found it as well, and there is no need for additional confirmations
   * anymore.
   *
   * @param messages incoming exchange offers
   * @return the best offer from the incoming messages
   */
  private Long getBestOfferedPartnerIdForExchange(Iterable<Message> messages) {
    long desiredPartnerId =
      this.vertex.getValue().getChosenPartnerIdForExchange();
    Long partnerId = null;
    double bestValue = 0;

    for (Message message : messages) {
      if (message.getVertexId() == desiredPartnerId) {
        partnerId = message.getVertexId();
        break;
      } else if (message.getImprovedNeighboringColorsValue() > bestValue) {
        partnerId = message.getVertexId();
        bestValue = message.getImprovedNeighboringColorsValue();
      }
    }

    return partnerId;
  }

  /**
   * Once an agreement was met, it is safe to assume that both node know
   * about this agreement, both nodes also know about each others color
   * wherefore we don't need to send anything but simply only set the color
   * of the current node to that of the partner
   *
   * @param partnerId the id of the color exchanging partner
   */
  private void exchangeColors(long partnerId) {
    int newColor =
      this.vertex.getValue().getNeighborInformation().
        get(partnerId).getColor();

    this.vertex.getValue().setNodeColor(newColor);
  }

  /**
   * JaBeJa uses for summing up node degrees the exponent {@code alpha}
   *
   * @param node1Degree degree of the first node
   * @param node2Degree degree of the second node
   * @return node1Degree^alpha + node2Degree^alpha
   */
  private double getJaBeJaSum(int node1Degree, int node2Degree) {
    return Math.pow(node1Degree, this.conf.getAlpha()) +
           Math.pow(node2Degree, this.conf.getAlpha());
  }

  /**
   * uses simulated annealing to check if the new option is better than the
   * old one
   *
   * @param newJaBeJaSum sum of the new degrees calculated with
   *                     {@code getJaBeJaSum}
   * @param oldJaBeJaSum sum of the old degrees calculated with
   *                     {@code getJaBeJaSum}
   * @return decision if the new solution is better than the old one
   */
  private boolean isNewColorBetterThanOld(
    double newJaBeJaSum,
    double oldJaBeJaSum) {

    return (newJaBeJaSum * getTemperature()) > oldJaBeJaSum;
  }

  /**
   * Calculates the current temperature based on the initial temperature the
   * rounds passed and the cooling factor
   *
   * @return the current temperature
   */
  private double getTemperature() {
    if (temperature == null) {
      long rounds = getNumberOfJaBeJaRounds();
      double initialTemperature = this.conf.getInitialTemperature();
      double temperaturePreserving = 1 - this.conf.getCoolingFactor();

      temperature =
        Math.pow(temperaturePreserving, rounds) * initialTemperature;
    }
    return temperature;
  }

  /**
   * Calculates based on the supersteps how often the JaBeJa-algorithm ran
   * through with all it's substeps
   *
   * @return how often the JaBeJa-algorithm with all it's substeps ran through
   */
  private long getNumberOfJaBeJaRounds() {
    long jaBeJaSupersteps = super.getSuperstep() -
                            NUMBER_OF_STEPS_DURING_INITIALIZATION;
    return jaBeJaSupersteps / NUMBER_OF_STEPS_PER_JABEJA_ROUND;
  }

  /**
   * Send a message to a vertex with the current color and id,
   * so that this vertex would be able to reply.
   *
   * @param targetId id of the vertex to which the message should be sent
   */
  private void sendCurrentVertexColor(LongWritable targetId) {
    super.sendMessage(targetId, new Message(this.vertex.getId().get(),
      this.vertex.getValue().getNodeColor()));
  }

  /**
   * generates a 64bit random number within an upper boundary.
   *
   * @param exclusiveMaxValue the exclusive maximum value of the to be
   *                          generated random value
   * @return a random number between 0 (inclusive) and exclusiveMaxValue
   * (exclusive)
   */
  private long getRandomNumber(long exclusiveMaxValue) {
    if (this.randomGenerator == null) {
      this.randomGenerator = JaBeJaUtils.initializeRandomGenerator(
        this.conf, super.getSuperstep(), this.vertex.getId().get());
    }
    return (long) (this.randomGenerator.nextDouble() * exclusiveMaxValue);
  }
}
