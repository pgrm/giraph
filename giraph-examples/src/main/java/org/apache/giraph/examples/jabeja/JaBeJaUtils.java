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

import java.util.Random;

/**
 *
 */
public class JaBeJaUtils {
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
      long seed = JaBeJaUtils.calculateLongHashCode(String.format("%d#%d#%d",
        configuredSeed, superstep, vertexId));
      randomGenerator = new Random(seed);
    }

    return randomGenerator;
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
