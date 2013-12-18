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
import org.apache.log4j.Logger;

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

  /** logger */
  private static final Logger LOG =
    Logger.getLogger(NodePartitioningComputation.class);
  /**
   * How many stupersteps does it take to run through one JaBeJa round
   */
  private static final int NUMBER_OF_STEPS_PER_JABEJA_ROUND = 5;

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
    // reset the random generator in the beginning so that it will be
    // recreated for every step and vertex
    this.randomGenerator = null;

    if (vertex.getValue() == null) {
      vertex.setValue(new NodePartitioningVertexData());
    }

    if (this.conf == null) {
      this.conf = new JaBeJaConfigurations(super.getConf());
    }
    this.vertex = vertex;

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
      return;
    }

    LOG.trace("Running compute in the " + getSuperstep() + " round");

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

      LOG.trace("running JaBeJaAlgorithm");
      runJaBeJaAlgorithm(mode, messages);
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
      initializeRandomNeighbors();
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
        LOG.trace(this.vertex.getId().get() +
                  ": Initialize Color-Exchange with " + partner.getKey());
        initiateColoExchangeHandshake(partner,
          this.vertex.getValue().isRandomNeighbor(partner.getKey()));
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

    LOG.trace("Chose color " + myColor + " out of " + numberOfColors +
              " colors");

    this.vertex.getValue().setNodeColor(myColor);
  }

  /**
   * Selects random nodes for later color exchanges and sends them the current
   * color
   */
  private void initializeRandomNeighbors() {
    int numberOfRandomNeighbors = this.conf.getNumberOfRandomNeighbors();
    NodePartitioningVertexData vertexData = this.vertex.getValue();
    Map<Long, ?> neighbors = vertexData.getNeighborInformation();

    while (numberOfRandomNeighbors > 0) {
      long randomNeighborId = getRandomNumber(super.getTotalNumVertices());

      if (randomNeighborId != this.vertex.getId().get() &&
          !neighbors.containsKey(randomNeighborId)) {
        // one good random neighbor found
        numberOfRandomNeighbors--;
        sendCurrentVertexColor(new LongWritable(randomNeighborId), true);
      }
    }
  }

  /**
   * Announces the color, only if it has changed after it has been announced
   * the last time
   */
  private void announceColorIfChanged() {
    if (this.vertex.getValue().getHasColorChanged()) {
      for (Long neighborId : this.vertex.getValue().getNeighbors()) {
        sendCurrentVertexColor(new LongWritable(neighborId), false);
      }
      for (Long neighborId : this.vertex.getValue().getRandomNeighbors()) {
        sendCurrentVertexColor(new LongWritable(neighborId), true);
      }
      this.vertex.getValue().resetHasColorChanged();
    }
  }

  /**
   * Announce the current color to all connected vertices for the first time.
   */
  private void announceInitialColor() {
    for (Edge<LongWritable, IntWritable> edge : this.vertex.getEdges()) {
      sendCurrentVertexColor(edge.getTargetVertexId(), false);
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
        msg.getColor(), msg.isRandomNeighbor());
    }
  }

  /**
   * Reply to all received messages with the current color.
   *
   * @param messages all received messages.
   */
  private void announceColorToNewNeighbors(Iterable<Message> messages) {
    for (Message msg : messages) {
      sendCurrentVertexColor(new LongWritable(msg.getVertexId()),
        msg.isRandomNeighbor());
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
          this.vertex.getValue().getNeighboringColorRatio(), false));
    }
    for (Long neighborId : this.vertex.getValue().getRandomNeighbors()) {
      super.sendMessage(new LongWritable(neighborId),
        new Message(this.vertex.getId().get(),
          this.vertex.getValue().getNeighboringColorRatio(), true));
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
        message.getVertexId(), message.getNeighboringColorRatio(),
        message.isRandomNeighbor());
    }
  }

  /**
   * Finds the best partner to exchange their colors with each other. Looking
   * for normal as well as random neighbors
   *
   * @return the vertex ID of the partner with whom the colors will be
   * exchanged
   */
  private Map.Entry<Long, Double> findPartner() {
    NodePartitioningVertexData data = this.vertex.getValue();

    Map.Entry<Long, Double> firstPartner =
      findPartner(data.getNeighborInformation());
    Map.Entry<Long, Double> randomPartner =
      findPartner(data.getRandomNeighborInformation());

    if (firstPartner.getKey() == null) {
      return randomPartner;
    } else if (randomPartner.getKey() == null) {
      return firstPartner;
    } else if (firstPartner.getValue() >= randomPartner.getKey()) {
      return firstPartner;
    } else {
      return randomPartner;
    }
  }

  /**
   * Internal implementation of findPartner which can be called with the
   * collection of normal neighbors or random neighbors
   *
   * @param neighbors collection of neighbors from which the partner is to be
   *                  found
   * @return the vertex ID of the partner with whom the colors will be
   * exchanged
   */
  private Map.Entry<Long, Double> findPartner(
    Map<Long, VertexData.NeighborInformation> neighbors) {

    double highest = 0;
    Long bestPartner = null;
    NodePartitioningVertexData data = this.vertex.getValue();

    for (Map.Entry<Long, VertexData.NeighborInformation> neighbor :
      neighbors.entrySet()) {

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
   * @param partner          the vertex with whom I want to exchange colors
   *                         (key = vertex id,
   *                         value = JaBeJa-sum for the finished exchange)
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   */

  private void initiateColoExchangeHandshake(
    Map.Entry<Long, Double> partner, boolean isRandomNeighbor) {

    super.sendMessage(new LongWritable(partner.getKey()),
      new Message(this.vertex.getId().get(), partner.getValue(),
        isRandomNeighbor));
  }

  /**
   * This part is tricky - it performs the handshake and the actual color
   * exchange, following possibilities exist:
   * 1: no messages have been received - nobody wants to exchange colors - DONE
   * 2: a message from the preferred partner has been received,
   * since the partner got the message also from this node,
   * both know they can exchange the color - DONE
   * <p/>
   * 3: received message(s) for color exchange from different nodes,
   * pick the best performing one and register it as the new preferred
   * partner and reply. In the next step there are 2 possibilities:
   * <p/>
   * 3.1: no further message was received, reply one more time to the new
   * preferred partner to confirm it, during the next round either we receive
   * a second confirmation, exchange colors, or not - drop color exchange
   * <p/>
   * 3.2: one further message was received, this comes from the previous
   * preferred partner, no take a chance and choose one of the 2 and send a
   * second confirmation. If during the next round a confirmation from this
   * node is received, exchange the colors otherwise drop
   *
   * @param mode     the sub-step of the color exchange
   * @param messages The messages sent to this Vertex
   */
  private void continueColorExchange(int mode, Iterable<Message> messages) {
    NodePartitioningVertexData vertexData = this.vertex.getValue();

    switch (mode) {
    case 0:
      Long partnerId = getBestOfferedPartnerIdForExchange(messages);

      LOG.trace(this.vertex.getId().get() + ": best offer from " + partnerId);
      if (partnerId != null) {
        long desiredPartnerId = vertexData.getChosenPartnerIdForExchange();

        if (partnerId == desiredPartnerId) {
          LOG.trace("Direct match - let's exchange");
          exchangeColors(partnerId);
          vertexData.setChosenPartnerIdForExchange(-1); // reset partner
        } else {
          LOG.trace("Send confirmation");
          confirmColorExchangeWithPartner(partnerId);
        }
      }
      break;
    case 1:
      long preferredPartner = vertexData.getChosenPartnerIdForExchange();

      if (preferredPartner > -1) {
        // has previously preferred partner answered?
        if (messages.iterator().hasNext()) {
          // switch the vertex id with a 50:50 chance
          if (getRandomNumber(2) == 1) {
            preferredPartner = messages.iterator().next().getVertexId();
          }
        }
        LOG.trace(this.vertex.getId().get() +
                  ": second confirmation to " + preferredPartner);
        confirmColorExchangeWithPartner(preferredPartner);
      }
      break;
    case 2:
      Long finalPartnerId = getBestOfferedPartnerIdForExchange(messages);

      LOG.trace(this.vertex.getId().get() +
                ": got a final reply from " + finalPartnerId);
      if (finalPartnerId != null &&
          finalPartnerId == vertexData.getChosenPartnerIdForExchange()) {
        LOG.trace("It fits - let's exchange");
        exchangeColors(finalPartnerId);
      }
      announceColorIfChanged();
      break;
    default:
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
   * Send a confirmation to the partner and set it as the new preferred partner
   *
   * @param partnerId the id of the vertex with whom the exchange is planned
   */
  private void confirmColorExchangeWithPartner(long partnerId) {
    super.sendMessage(new LongWritable(partnerId),
      new Message(this.vertex.getId().get(),
        this.vertex.getValue().isRandomNeighbor(partnerId)));
    this.vertex.getValue().setChosenPartnerIdForExchange(partnerId);
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
    NodePartitioningVertexData vertexData = this.vertex.getValue();
    int newColor;

    if (vertexData.isRandomNeighbor(partnerId)) {
      newColor =
        vertexData.getRandomNeighborInformation().get(partnerId).getColor();
    } else {
      newColor = vertexData.getNeighborInformation().get(partnerId).getColor();
    }

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
   * @param targetId         id of the vertex to which the message should be
   *                         sent
   * @param isRandomNeighbor flag if it is a regular neighbor or one from the
   *                         random overlay
   */
  private void sendCurrentVertexColor(
    LongWritable targetId, boolean isRandomNeighbor) {

    super.sendMessage(targetId, new Message(this.vertex.getId().get(),
      this.vertex.getValue().getNodeColor(), isRandomNeighbor));
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
