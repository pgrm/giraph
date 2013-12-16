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

import org.apache.log4j.Logger;

import java.util.Random;

/**
 *
 */
public class JaBeJaUtils {
  /** logger */
  private static final Logger LOG = Logger.getLogger(JaBeJaUtils.class);

  /**
   * Hiding default constructor
   */
  private JaBeJaUtils() {
  }

  /**
   * Initializes the random generator, either with the default constructor or
   * with a seed which will guarantee, that the same vertex in the same round
   * will always generate the same random values.
   *
   * @param conf      The JaBeJa configuration to get the seed value
   * @param superstep The superstep so the random values are different for
   *                  each step
   * @param vertexId  The vertex id so the random values are different for
   *                  each vertex
   * @return a Random generator with specific values
   */
  public static Random initializeRandomGenerator(
    JaBeJaConfigurations conf, long superstep, long vertexId) {

    int configuredSeed = conf.getRandomSeed();
    Random randomGenerator = null;

    // if you want to have a totally random number just set the config to -1
    if (configuredSeed == -1) {
      randomGenerator = new Random();
    } else {
      long seed = getMixedUpSeed(configuredSeed, superstep, vertexId);
      LOG.trace("Initializing random generator with the seed " + seed);
      randomGenerator = new Random(seed);
    }

    return randomGenerator;
  }

  /**
   * creates a seed which should be as diverse as possible to have a
   * reproducible random function per node and per superstep
   *
   * @param configuredSeed the seed configured by the user
   * @param superstep      the current superstep of giraph
   * @param vertexId       the id of the current vertex
   * @return seed which can be used to create a new Random-generator instance
   */
  private static long getMixedUpSeed(
    int configuredSeed, long superstep, long vertexId) {
    long firstValue = 0;
    long secondValue = 0;
    long thirdValue = 0;
    int mode = (int) (vertexId % 6);

    vertexId *= 107;
    superstep *= 107;

    if (mode < 2) {
      firstValue = configuredSeed;
      if (mode == 0) {
        secondValue = superstep;
        thirdValue = vertexId;
      } else {
        secondValue = vertexId;
        thirdValue = superstep;
      }
    } else if (mode < 4) {
      firstValue = superstep;
      if (mode == 2) {
        secondValue = configuredSeed;
        thirdValue = vertexId;
      } else {
        secondValue = vertexId;
        thirdValue = configuredSeed;
      }
    } else if (mode < 6) {
      firstValue = vertexId;
      if (mode == 4) {
        secondValue = configuredSeed;
        thirdValue = superstep;
      } else {
        secondValue = superstep;
        thirdValue = configuredSeed;
      }
    }

    long seed = JaBeJaUtils.calculateLongHashCode(String.format("%d#%d#%d",
      firstValue, secondValue, thirdValue));

    return seed;
  }

  /**
   * Based on the hashCode function defined in {@link java.lang.String},
   * with the difference that it returns a long instead of only an integer.
   *
   * @param value the String for which the hash-code should be calculated
   * @return a 64bit hash-code
   */
  public static long calculateLongHashCode(String value) {
    int hashCode = 0;

    for (int i = 0; i < value.length(); i++) {
      hashCode = 31 * hashCode + value.charAt(i);
    }

    return hashCode;
  }
}
