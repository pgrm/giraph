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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class EdgePartitioningVertexData extends VertexData {
  /**
   * An adjacency-matrix with a list of connected edgeIDs for each vertex
   */
  private final Map<Long, Set<Long>> vertexConnections =
    new HashMap<Long, Set<Long>>();

  /**
   * The id of the edge chosen to do the color exchange
   */
  private Long chosenEdgeId;

  /**
   * used for updating or inserting new values into {@code vertexConnection}
   *
   * @param vertexId the id of the vertex
   * @param edgeId   the id of the edge connected to the vertex
   */
  public void updateVertexConnections(long vertexId, long edgeId) {
    Set<Long> edgeSet = this.vertexConnections.get(vertexId);

    if (edgeSet == null) {
      edgeSet = new HashSet<Long>();
      this.vertexConnections.put(vertexId, edgeSet);
    }
    edgeSet.add(edgeId);
  }

  /**
   * Gets a list of edges connected to one vertex
   *
   * @param vertexId the id of the vertex to which the edges should be
   *                 connected
   * @return a list of edges connected to the specified vertex
   */
  public Set<Long> getVertexConnections(long vertexId) {
    return this.vertexConnections.get(vertexId);
  }

  /**
   * @param vertexId the id of the vertex for which the color ratio should be
   *                 calculated
   * @return data for a histogram for the colors of all neighbors.
   * How often each of the colors is represented between the neighbors.
   * If a color isn't represented, it's not in the final Map.
   */
  public Map<Integer, Integer> getNeighboringColorRatio(long vertexId) {
    Map<Integer, Integer> neighboringColorRatio =
      new HashMap<Integer, Integer>();

    for (Long edgeId : this.vertexConnections.get(vertexId)) {
      int color = super.getNeighborInformation().get(edgeId).getColor();

      addColorToNeighboringColoRatio(color, neighboringColorRatio);
    }

    return neighboringColorRatio;
  }

  /**
   * Check if the color already exists in the neighboringColorRatio-map. If
   * not, create a new entry with the count 1, if yes than update the count +1
   *
   * @param color                 the color of one neighboring item
   * @param neighboringColorRatio the map to which the color should be added
   */
  private void addColorToNeighboringColoRatio(
    int color, Map<Integer, Integer> neighboringColorRatio) {
    Integer numberOfColorAppearances = neighboringColorRatio.get(color);

    if (numberOfColorAppearances == null) {
      numberOfColorAppearances = 1;
    } else {
      numberOfColorAppearances++;
    }

    neighboringColorRatio.put(color, numberOfColorAppearances);
  }

  public void setChosenEdgeId(Long chosenEdgeId) {
    this.chosenEdgeId = chosenEdgeId;
  }

  public Long getChosenEdgeId() {
    return chosenEdgeId;
  }
}

