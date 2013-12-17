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
import org.apache.hadoop.io.NullWritable;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Implement the original JaBeJa-Algorithm
 * (https://www.sics.se/~amir/files/download/papers/jabeja.pdf)
 */
public class NodePartitioningComputation extends
  GraphPartitioningComputation<NodePartitioningVertexData, NullWritable,
    NodePartitioningMessage> {

  @Override
  protected void initializeColor() {
    int numberOfColors = super.conf.getNumberOfColors();
    int myColor = (int) getRandomNumber(numberOfColors);

    LOG.trace("Chose color " + myColor + " out of " + numberOfColors +
              " colors");

    this.vertex.getValue().setNodeColor(myColor);
  }

  @Override
  protected void initializeRandomNeighbors() {
    int numberOfRandomNeighbors = super.conf.getNumberOfRandomNeighbors();
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

  @Override
  protected void announceInitialColor() {
    for (Edge<LongWritable, NullWritable> edge : this.vertex.getEdges()) {
      sendCurrentVertexColor(edge.getTargetVertexId(), false);
    }
  }

  @Override
  protected void storeColorsOfNodes(
    Iterable<NodePartitioningMessage> messages) {
    for (NodePartitioningMessage msg : messages) {
      this.vertex.getValue().setNeighborWithColor(msg.getSourceId(),
        msg.getColor(), msg.isRandomNeighbor());
    }
  }

  @Override
  protected void announceColorToNewNeighbors(
    Iterable<NodePartitioningMessage> messages) {
    for (NodePartitioningMessage msg : messages) {
      sendCurrentVertexColor(new LongWritable(msg.getSourceId()),
        msg.isRandomNeighbor());
    }
  }

  @Override
  protected void announceColoredDegreesIfChanged() {
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
        new NodePartitioningMessage(this.vertex.getId().get(),
          this.vertex.getValue().getNeighboringColorRatio(), false));
    }
    for (Long neighborId : this.vertex.getValue().getRandomNeighbors()) {
      super.sendMessage(new LongWritable(neighborId),
        new NodePartitioningMessage(this.vertex.getId().get(),
          this.vertex.getValue().getNeighboringColorRatio(), true));
    }
  }

  @Override
  protected void storeColoredDegreesOfNodes(
    Iterable<NodePartitioningMessage> messages) {
    for (NodePartitioningMessage message : messages) {
      this.vertex.getValue().setNeighborWithColorRatio(
        message.getSourceId(), message.getNeighboringColorRatio(),
        message.isRandomNeighbor());
    }
  }

  @Override
  protected Map.Entry<Long, Double> findPartner() {
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

  @Override
  protected void initiateColoExchangeHandshake(
    Map.Entry<Long, Double> partner, boolean isRandomNeighbor) {

    super.sendMessage(new LongWritable(partner.getKey()),
      new NodePartitioningMessage(this.vertex.getId().get(), partner.getValue(),
        isRandomNeighbor));
  }

  @Override
  protected void continueColorExchange(
    int mode, Iterable<NodePartitioningMessage> messages) {
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
            preferredPartner = messages.iterator().next().getSourceId();
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
  private Long getBestOfferedPartnerIdForExchange(
    Iterable<NodePartitioningMessage> messages) {
    long desiredPartnerId =
      this.vertex.getValue().getChosenPartnerIdForExchange();
    Long partnerId = null;
    double bestValue = 0;

    for (NodePartitioningMessage message : messages) {
      if (message.getSourceId() == desiredPartnerId) {
        partnerId = message.getSourceId();
        break;
      } else if (message.getImprovedNeighboringColorsValue() > bestValue) {
        partnerId = message.getSourceId();
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
      new NodePartitioningMessage(this.vertex.getId().get(),
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

    super.sendMessage(targetId,
      new NodePartitioningMessage(this.vertex.getId().get(),
        this.vertex.getValue().getNodeColor(), isRandomNeighbor));
  }

}
