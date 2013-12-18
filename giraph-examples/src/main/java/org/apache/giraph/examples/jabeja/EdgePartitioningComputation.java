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
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class EdgePartitioningComputation extends
  GraphPartitioningComputation<EdgePartitioningVertexData,
    EdgePartitioningEdgeData, EdgePartitioningMessage> {

  /**
   * The cached color-ratio for the current vertex so only the target vertex
   * has to be calculated.
   */
  private Map<Integer, Integer> colorRatioOfCurrentVertex;

  @Override
  public void compute(
    Vertex<LongWritable, EdgePartitioningVertexData,
      EdgePartitioningEdgeData> vertex,
    Iterable<EdgePartitioningMessage> messages) throws IOException {

    this.colorRatioOfCurrentVertex = null;
    super.compute(vertex, messages);
  }

  @Override
  protected void initializeColor() {
    EdgePartitioningVertexData vertexData = super.vertex.getValue();
    int numberOfColors = super.conf.getNumberOfColors();
    long index = 1;
    long edgeId;
    int edgeColor;

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      super.vertex.getEdges()) {

      edgeId = index * super.getTotalNumVertices() + super.vertex.getId().get();
      edgeColor = (int) getRandomNumber(numberOfColors);
      edge.getValue().setEdgeId(edgeId);
      edge.getValue().setEdgeColor(edgeColor);

      vertexData.setNeighborWithColor(edgeId, edgeColor, false);
      vertexData.updateVertexConnections(
        edge.getTargetVertexId().get(), edgeId);
      vertexData.updateVertexConnections(super.vertex.getId().get(), edgeId);

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

    EdgePartitioningVertexData vertexData = this.vertex.getValue();

    for (EdgePartitioningMessage msg : messages) {
      for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
        msg.getConnectedEdges()) {

        vertexData.setNeighborWithColor(edge.getValue().getEdgeId(),
          edge.getValue().getEdgeColor(), msg.isRandomNeighbor());
        vertexData.updateVertexConnections(
          extractVertexId(edge.getValue().getEdgeId()),
          edge.getValue().getEdgeId());
        vertexData.updateVertexConnections(edge.getTargetVertexId().get(),
          edge.getValue().getEdgeId());
      }
    }
  }

  @Override
  protected void announceColorToNewNeighbors(
    Iterable<EdgePartitioningMessage> messages) {

    for (EdgePartitioningMessage msg : messages) {
      super.sendMessage(new LongWritable(msg.getSourceId()),
        new EdgePartitioningMessage
          (this.vertex.getId().get(), false, transformNeighborsToEdges()));
    }
  }

  @Override
  protected void announceColoredDegrees() {
    Set<Long> sentNodes = new HashSet<Long>();

    for (Long neighborId : super.vertex.getValue().getNeighbors()) {
      long vertexId = extractVertexId(neighborId);

      if (vertexId == super.vertex.getId().get()) {
        vertexId = getEdge(neighborId).getTargetVertexId().get();
      }
      if (sentNodes.add(vertexId)) {
        for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
          super.vertex.getEdges()) {

          super.sendMessage(new LongWritable(vertexId),
            new EdgePartitioningMessage(edge.getValue().getEdgeId(),
              getNeighboringColorRatio(edge), false));
        }
      }
    }

    // TODO - implement color ratios also for random neighbors
//    for (Long neighborId : this.vertex.getValue().getRandomNeighbors()) {
//      super.sendMessage(new LongWritable(neighborId),
//        new NodePartitioningMessage(this.vertex.getId().get(),
//          this.vertex.getValue().getNeighboringColorRatio(
//            super.vertex.getId().get()), true));
//    }
  }

  @Override
  protected void storeColoredDegreesOfNodes(
    Iterable<EdgePartitioningMessage> messages) {
    for (EdgePartitioningMessage message : messages) {
      super.vertex.getValue().setNeighborWithColorRatio(
        message.getSourceId(), message.getNeighboringColorRatio(),
        message.isRandomNeighbor());
    }
  }

  @Override
  protected Map.Entry<Long, Double> findPartner() {
    EdgePartitioningVertexData data = super.vertex.getValue();

    BestPartner firstPartner =
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

  @Override
  protected void initiateColorExchangeHandshake(
    Map.Entry<Long, Double> partner, boolean isRandomNeighbor) {

    BestPartner myPartner = (BestPartner) partner;
    EdgePartitioningVertexData vertexData = super.vertex.getValue();
    long vertexId = extractVertexId(partner.getKey());

    // my edges want to exchange between each other
    if (vertexId == super.vertex.getId().get()) {
      vertexData.setChosenPartnerIdForExchange(-1);
      int localColor = vertexData.getNeighborInformation().get(
        myPartner.getLocalEdgeId()).getColor();
      int partnerColor = vertexData.getNeighborInformation().get(
        myPartner.getPartnerEdgeId()).getColor();

      vertexData.getNeighborInformation().get(myPartner.getLocalEdgeId()).
        setColor(partnerColor);
      vertexData.getNeighborInformation().get(myPartner.getPartnerEdgeId()).
        setColor(localColor);

      getEdge(myPartner.getLocalEdgeId()).getValue().setEdgeColor(partnerColor);
      getEdge(myPartner.getPartnerEdgeId()).getValue().setEdgeColor(localColor);
    } else {
      vertexData.setChosenEdgeId(myPartner.getLocalEdgeId());

      super.sendMessage(new LongWritable(vertexId),
        new EdgePartitioningMessage(myPartner.getLocalEdgeId(),
          myPartner.getPartnerEdgeId(), partner.getValue(), isRandomNeighbor));
    }
  }

  @Override
  protected void continueColorExchange(
    int mode, Iterable<EdgePartitioningMessage> messages) {
    EdgePartitioningVertexData vertexData = super.vertex.getValue();
    long preferredPartner = vertexData.getChosenPartnerIdForExchange();

    switch (mode) {
    case 0:
      if (preferredPartner > -1) {
        EdgePartitioningMessage partner =
          getBestOfferedPartnerForExchange(messages);

        LOG.trace(
          super.vertex.getId().get() + ": best offer from " + partner);
        if (partner != null) {
          long desiredPartnerId = vertexData.getChosenPartnerIdForExchange();

          if (partner.getSourceId() == desiredPartnerId) {
            LOG.trace("Direct match - let's exchange");
            exchangeColors(partner.getSourceId());
            vertexData.setChosenPartnerIdForExchange(-1); // reset partner
          } else {
            LOG.trace("Send confirmation");
            confirmColorExchangeWithPartner(partner);
          }
        }
      }
      break;
    case 1:
      if (preferredPartner > -1) {
        EdgePartitioningMessage newPartner = null;
        // has previously preferred partner answered?
        if (messages.iterator().hasNext()) {
          // switch the vertex id with a 50:50 chance
          if (getRandomNumber(2) == 1) {
            newPartner = messages.iterator().next();
            preferredPartner = newPartner.getSourceId();
          }
        }
        LOG.trace(super.vertex.getId().get() +
                  ": second confirmation to " + preferredPartner);

        if (newPartner == null) {
          confirmColorExchangeWithPartner();
        } else {
          confirmColorExchangeWithPartner(newPartner);
        }
      }
      break;
    case 2:
      if (preferredPartner > -1) {
        EdgePartitioningMessage finalPartner =
          getBestOfferedPartnerForExchange(messages);

        LOG.trace(super.vertex.getId().get() +
                  ": got a final reply from " + finalPartner);
        if (finalPartner != null &&
            finalPartner.getSourceId() ==
            vertexData.getChosenPartnerIdForExchange()) {
          LOG.trace("It fits - let's exchange");
          exchangeColors(finalPartner.getSourceId());
        }
      }
      announceColorIfChanged();
      break;
    default:
    }
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
    EdgePartitioningVertexData vertexData = super.vertex.getValue();
    int newColor;

    if (vertexData.isRandomNeighbor(partnerId)) {
      newColor =
        vertexData.getRandomNeighborInformation().get(partnerId).getColor();
    } else {
      newColor = vertexData.getNeighborInformation().get(partnerId).getColor();
    }

    vertexData.getNeighborInformation().get(vertexData.getChosenEdgeId()).
      setColor(newColor);
    getEdge(vertexData.getChosenEdgeId()).getValue().setEdgeColor(newColor);
  }

  /**
   * Send a confirmation to the partner and set it as the new preferred partner
   *
   * @param message the message to which the color exchange will be confirmed
   */
  private void confirmColorExchangeWithPartner(
    EdgePartitioningMessage message) {

    super.vertex.getValue()
      .setChosenPartnerIdForExchange(message.getSourceId());
    super.vertex.getValue().setChosenEdgeId(message.getPartnerEdgeId());
  }

  private void confirmColorExchangeWithPartner() {
    EdgePartitioningVertexData vertexData = super.vertex.getValue();

    long vertexId = extractVertexId(vertexData.getChosenPartnerIdForExchange());

    super.sendMessage(new LongWritable(vertexId),
      new EdgePartitioningMessage(
        vertexData.getChosenEdgeId(),  // local edge id
        vertexData.getChosenPartnerIdForExchange(), // partner edge id (remote)
        vertexData.isRandomNeighbor( // is partner random neighbor
          vertexData.getChosenPartnerIdForExchange())));
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
  private BestPartner findPartner(
    Map<Long, VertexData.NeighborInformation> neighbors) {

    double highest = 0;
    Long bestEdgeSource = null;
    Long bestPartner = null;

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      super.vertex.getEdges()) {

      for (Map.Entry<Long, VertexData.NeighborInformation> neighbor :
        neighbors.entrySet()) {

        // no need to calculate a possible color exchange with the current edge
        if (edge.getValue().getEdgeId() == neighbor.getKey()) {
          continue;
        }

        Map<Integer, Integer> myColorRatio = getNeighboringColorRatio(edge);
        EdgePartitioningEdgeData edgeData = edge.getValue();
        int myDegree = getNumberOfNeighborsWithSameColor(myColorRatio,
          edgeData.getEdgeColor());
        int neighborsDegree = neighbor.getValue().getNumberOfNeighbors(
          neighbor.getValue().getColor());

        int myNewDegree = getNumberOfNeighborsWithSameColor(myColorRatio,
          neighbor.getValue().getColor());
        int neighborsNewDegree =
          neighbor.getValue().getNumberOfNeighbors(edgeData.getEdgeColor());

        double sum = getJaBeJaSum(myDegree, neighborsDegree);
        double newSum = getJaBeJaSum(myNewDegree, neighborsNewDegree);

        if (isNewColorBetterThanOld(newSum, sum) && newSum > highest) {
          bestPartner = neighbor.getKey();
          bestEdgeSource = edgeData.getEdgeId();
          highest = newSum;
        }
      }
    }

    return new BestPartner(bestEdgeSource, bestPartner, highest);
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
  private EdgePartitioningMessage getBestOfferedPartnerForExchange(
    Iterable<EdgePartitioningMessage> messages) {
    long desiredPartnerId =
      super.vertex.getValue().getChosenPartnerIdForExchange();
    EdgePartitioningMessage partner = null;
    double bestValue = 0;

    for (EdgePartitioningMessage message : messages) {
      if (message.getSourceId() == desiredPartnerId) {
        partner = message;
        break;
      } else if (message.getImprovedNeighboringColorsValue() > bestValue) {
        partner = message;
        bestValue = message.getImprovedNeighboringColorsValue();
      }
    }

    return partner;
  }

  private Map<Integer, Integer> getNeighboringColorRatio(
    Edge<LongWritable, EdgePartitioningEdgeData> edge) {

    if (this.colorRatioOfCurrentVertex == null) {
      initializeColorRatioOfCurrentVertex();
    }

    Map<Integer, Integer> targetVertexColorRatio =
      super.vertex.getValue().getNeighboringColorRatio(
        edge.getTargetVertexId().get());

    targetVertexColorRatio = mergeColorRatios(targetVertexColorRatio,
      this.colorRatioOfCurrentVertex);

    // the current edge has been counted twice (once in the
    // colorRatioOfCurrentVertex and a second time in targetVertexColorRatio,
    // so it's color has to be removed
    int currentEdgeColorOccurrences =
      targetVertexColorRatio.get(edge.getValue().getEdgeColor());
    currentEdgeColorOccurrences -= 2;
    targetVertexColorRatio.put(
      edge.getValue().getEdgeColor(), currentEdgeColorOccurrences);

    return targetVertexColorRatio;
  }

  private Map<Integer, Integer> mergeColorRatios(
    Map<Integer, Integer> baseColorRatio,
    Map<Integer, Integer> additionalColorRatio) {

    for (Map.Entry<Integer, Integer> entry : additionalColorRatio.entrySet()) {
      Integer newValue = baseColorRatio.get(entry.getKey());

      if (newValue == null) {
        newValue = entry.getValue();
      } else {
        newValue += entry.getValue();
      }

      baseColorRatio.put(entry.getKey(), newValue);
    }
    return baseColorRatio;
  }

  private void initializeColorRatioOfCurrentVertex() {
    this.colorRatioOfCurrentVertex =
      super.vertex.getValue().getNeighboringColorRatio(
        super.vertex.getId().get());
  }

  private int getNumberOfNeighborsWithSameColor(
    Map<Integer, Integer> colorRatio, int color) {

    Integer degree = colorRatio.get(color);
    if (degree == null) {
      return 0;
    } else {
      return degree;
    }
  }

  /**
   * Transforms all the neighbors into a collection of edges. At the time
   * when this function is called there are only 2 types of neighbors stored:
   * 1. Neighbors outgoing from this vertex, their sourceVertexId will
   * therefor be the currentVertexId, hence the edge can be found to
   * determine the targetVertexId
   * 2. Neighbors pointing directly at this vertex, their sourceVertexId is
   * not the currentVertexId but their targetVertexId is the currentVertexId
   * <p/>
   * only after all edges, created by this function, have been sent to all
   * the neighbors the assumptions above won't hold anymore,
   * wherefore the function shouldn't be called anymore.
   *
   * @return a list of edges, to be sent in the 2nd initialization step
   * (superstep 1) in {@code announceColorToNewNeighbors}
   */
  private List<Edge<LongWritable, EdgePartitioningEdgeData>>
  transformNeighborsToEdges() {

    EdgePartitioningVertexData vertexData = super.vertex.getValue();
    long currentVertexId = super.vertex.getId().get();
    List<Edge<LongWritable, EdgePartitioningEdgeData>> edges = new
      ArrayList<Edge<LongWritable, EdgePartitioningEdgeData>>();

    for (Map.Entry<Long, VertexData.NeighborInformation> neighbor :
      vertexData.getNeighborInformation().entrySet()) {

      Long sourceVertexId = extractVertexId(neighbor.getKey());
      Long targetVertexId;

      if (sourceVertexId == currentVertexId) {
        targetVertexId = getEdge(neighbor.getKey()).getTargetVertexId().get();
      } else {
        targetVertexId = currentVertexId;
      }

      edges.add(new EdgePartitioningComputation.SimpleEdge<LongWritable,
        EdgePartitioningEdgeData>(new LongWritable(targetVertexId),
        new EdgePartitioningEdgeData(neighbor.getKey(),
          neighbor.getValue().getColor())));
    }

    return edges;
  }

  /**
   * Announces the color, only if it has changed after it has been announced
   * the last time
   */
  private void announceColorIfChanged() {
    Set<Long> sentNodes;
    EdgePartitioningVertexData vertexData = super.vertex.getValue();

    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      super.vertex.getEdges()) {

      sentNodes = new HashSet<Long>();
      if (edge.getValue().hasColorChanged()) {
        for (long neighborId :
          vertexData.getVertexConnections(super.vertex.getId().get())) {

          long vertexId = extractVertexId(neighborId);

          if (sentNodes.add(vertexId)) {
            super.sendMessage(new LongWritable(vertexId),
              new EdgePartitioningMessage(this.vertex.getId().get(), false,
                getIterableEdge(edge)));
          }
        }

        for (long neighborId :
          vertexData.getVertexConnections(edge.getTargetVertexId().get())) {

          long vertexId = extractVertexId(neighborId);

          if (sentNodes.add(vertexId)) {
            super.sendMessage(new LongWritable(vertexId),
              new EdgePartitioningMessage(this.vertex.getId().get(), false,
                getIterableEdge(edge)));
          }
        }
      }
    }
  }

  private Iterable<Edge<LongWritable, EdgePartitioningEdgeData>>
  getIterableEdge(Edge<LongWritable, EdgePartitioningEdgeData> edge) {

    List<Edge<LongWritable, EdgePartitioningEdgeData>> lst =
      new LinkedList<Edge<LongWritable, EdgePartitioningEdgeData>>();
    lst.add(edge);
    return lst;
  }

  private Long extractVertexId(Long edgeId) {
    return edgeId % super.getTotalNumVertices();
  }

  private Edge<LongWritable, EdgePartitioningEdgeData> getEdge(long edgeId) {
    for (Edge<LongWritable, EdgePartitioningEdgeData> edge :
      super.vertex.getEdges()) {

      if (edge.getValue().getEdgeId() == edgeId) {
        return edge;
      }
    }

    return null;
  }

  /**
   * Simple implementation of the Edge interface to be used in the
   * edge-partitioning version of the JaBeJa algorithm,
   * for nodes send a lot of edges to get an overview
   *
   * @param <I> Vertex index
   * @param <E> Edge value
   */
  public static class SimpleEdge<I extends WritableComparable,
    E extends Writable> implements Edge<I, E> {

    /** the id of the target vertex from this edge */
    private I targetVertexId;
    /** the value of the edge */
    private E value;

    /**
     * Default constructor
     */
    public SimpleEdge() {
    }

    /**
     * A constructor which allows to directly initialize both properties of
     * the edge
     *
     * @param targetVertexId the id of the target vertex from this edge
     * @param value          the value of the edge
     */
    public SimpleEdge(I targetVertexId, E value) {
      this.targetVertexId = targetVertexId;
      this.value = value;
    }

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

  /**
   * Map.Entry isn't enough anymore for Edge-Partitioning,
   * since it also needs to store the information, which edge should be
   * exchanged with the partner edge.
   */
  public static class BestPartner implements Map.Entry<Long, Double> {
    private Long localEdgeId;
    private Long partnerEdgeId;
    private Double benefit;

    public BestPartner(Long localEdgeId, Long partnerEdgeId, Double benefit) {
      this.localEdgeId = localEdgeId;
      this.partnerEdgeId = partnerEdgeId;
      this.benefit = benefit;
    }

    public Long getLocalEdgeId() {
      return localEdgeId;
    }

    public Long getPartnerEdgeId() {
      return partnerEdgeId;
    }

    public Double getBenefit() {
      return benefit;
    }

    @Override
    public Long getKey() {
      return this.partnerEdgeId;
    }

    @Override
    public Double getValue() {
      return this.benefit;
    }

    @Override
    public Double setValue(Double value) {
      return this.benefit = value;
    }
  }
}
