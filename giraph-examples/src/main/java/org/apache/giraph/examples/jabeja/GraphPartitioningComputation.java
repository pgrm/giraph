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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * Base class for graph partitioning by the JaBeJa-Algorithm
 */
public abstract class
  GraphPartitioningComputation<V extends VertexData, E extends Writable,
  M extends Writable> extends BasicComputation<LongWritable, V, E, M> {
  /** logger */
  protected static final Logger LOG =
    Logger.getLogger(NodePartitioningComputation.class);
  /**
   * How many additional steps are performed during initialization.
   * There is only one additional step during the initialization,
   * since it is necessary to collect neighbors who direct their edges to you.
   */
  private static final int NUMBER_OF_STEPS_DURING_INITIALIZATION = 2;
  /**
   * How many stupersteps does it take to run through one JaBeJa round
   */
  private static final int NUMBER_OF_STEPS_PER_JABEJA_ROUND = 5;
  /**
   * The currently processed vertex
   */
  protected Vertex<LongWritable, V, E> vertex;
  /**
   * An instance of the helper class to handle configurations.
   */
  protected JaBeJaConfigurations conf = null;
  /**
   * Since the JaBeJa algorithm is a Monte Carlo algorithm.
   */
  private Random randomGenerator = new Random();
  /**
   * The current temperature at the current superstep and JaBeJa round
   */
  private Double temperature = null;

  @Override
  public void compute(Vertex<LongWritable, V, E> vertex, Iterable<M> messages)
    throws IOException {
    // reset the random generator in the beginning so that it will be
    // recreated for every step and vertex
    this.randomGenerator = null;

    if (this.conf == null) {
      this.conf = new JaBeJaConfigurations(getConf());
    }
    this.vertex = vertex;

    if (isTimeToStop()) {
      this.vertex.voteToHalt();
      return;
    }

    LOG.trace("Running compute in the " + getSuperstep() + " round");

    if (getSuperstep() < 2) {
      initializeGraph(messages);
    } else {
      // the 1st step (mode == 0) is the same as the first step after the
      // initialization (Superstep == 2) since at this time we get the final
      // colors and can announce the degree in case it has changed.
      // also note that announcing colors again, is the last step for JaBeJa
      // and kind of a restart step
      int mode = (int)
        ((getSuperstep() - NUMBER_OF_STEPS_DURING_INITIALIZATION) %
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
  private void initializeGraph(Iterable<M> messages) {
    if (getSuperstep() == 0) {
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
  private void runJaBeJaAlgorithm(int mode, Iterable<M> messages) {
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
  protected abstract void initializeColor();

  /**
   * Selects random nodes for later color exchanges and sends them the current
   * color
   */
  protected abstract void initializeRandomNeighbors();

  /**
   * Announce the current color to all connected vertices for the first time.
   */
  protected abstract void announceInitialColor();

  /**
   * Store the color of all neighboring nodes. Neighboring through outgoing
   * as well as incoming edges.
   *
   * @param messages received messages from nodes with their colors.
   */
  protected abstract void storeColorsOfNodes(Iterable<M> messages);

  /**
   * Reply to all received messages with the current color.
   *
   * @param messages all received messages.
   */
  protected abstract void announceColorToNewNeighbors(Iterable<M> messages);

  protected void announceColoredDegreesIfChanged() {
    if (this.vertex.getValue().getHaveColoredDegreesChanged()) {
      announceColoredDegrees();
      this.vertex.getValue().resetHaveColoredDegreesChanged();
    }
  }

  protected abstract void announceColoredDegrees();

  /**
   * Updates the information about different colored degrees of its neighbors
   *
   * @param messages The messages sent to this Vertex containing the colored
   *                 degrees
   */
  protected abstract void storeColoredDegreesOfNodes(Iterable<M> messages);

  /**
   * Finds the best partner to exchange their colors with each other. Looking
   * for normal as well as random neighbors
   *
   * @return the vertex ID of the partner with whom the colors will be
   * exchanged
   */
  protected abstract Map.Entry<Long, Double> findPartner();

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

  protected abstract void initiateColoExchangeHandshake(
    Map.Entry<Long, Double> partner, boolean isRandomNeighbor);

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
  protected abstract void continueColorExchange(int mode, Iterable<M> messages);

  /**
   * JaBeJa uses for summing up node degrees the exponent {@code alpha}
   *
   * @param node1Degree degree of the first node
   * @param node2Degree degree of the second node
   * @return node1Degree^alpha + node2Degree^alpha
   */
  protected double getJaBeJaSum(int node1Degree, int node2Degree) {
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
  protected boolean isNewColorBetterThanOld(
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
  protected double getTemperature() {
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
  protected long getNumberOfJaBeJaRounds() {
    long jaBeJaSupersteps = getSuperstep() -
                            NUMBER_OF_STEPS_DURING_INITIALIZATION;
    return jaBeJaSupersteps / NUMBER_OF_STEPS_PER_JABEJA_ROUND;
  }

  /**
   * generates a 64bit random number within an upper boundary.
   *
   * @param exclusiveMaxValue the exclusive maximum value of the to be
   *                          generated random value
   * @return a random number between 0 (inclusive) and exclusiveMaxValue
   * (exclusive)
   */
  protected long getRandomNumber(long exclusiveMaxValue) {
    if (this.randomGenerator == null) {
      this.randomGenerator = JaBeJaUtils.initializeRandomGenerator(
        this.conf, getSuperstep(), this.vertex.getId().get());
    }
    return (long) (this.randomGenerator.nextDouble() * exclusiveMaxValue);
  }
}
