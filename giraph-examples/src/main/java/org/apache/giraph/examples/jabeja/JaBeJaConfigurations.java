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

import org.apache.hadoop.conf.Configuration;

/**
 * Class which is only responsible for reading and holding configuration
 * values, as well as defining the default values
 */
public class JaBeJaConfigurations {
  /**
   * The default number of different colors, if no
   * JaBeJa.NumberOfColors is provided.
   */
  private static final int DEFAULT_NUMBER_OF_COLORS = 2;

  /**
   * The default number for many supersteps this algorithm should run,
   * if no JaBeJa.MaxNumberOfSupersteps is provided
   */
  private static final int DEFAULT_MAX_NUMBER_OF_SUPERSTEPS = 100;

  /**
   * Default value for the alpha for JaBeJa
   */
  private static final float DEFAULT_ALPHA = 2;

  /**
   * Default initial temperature for simulated annealing
   */
  private static final float DEFAULT_INITIAL_TEMPERATURE = 2;

  /**
   * Default temperature cooling factor for simulated annealing
   */
  private static final float DEFAULT_COOLING_FACTOR = 0.003f;

  /**
   * Default seed for the custom creation of the random generator
   */
  private static final int DEFAULT_RANDOM_SEED = 0;

  /**
   * Default value if the complex or simple color exchange should be used
   */
  private static final boolean USE_COMPLEX_COLOR_EXCHANGE_DEFAULT = false;

  /**
   * Configuration to read values from, defined as member,
   * so it's not necessary to pass it on as an argument to all the functions.
   */
  private Configuration conf = null;

  /**
   * Constructor, while it wouldn't be necessary it's easier since all the
   * functions need the conf parameter.
   *
   * @param conf will read the configuration values from here (super.getConf())
   */
  public JaBeJaConfigurations(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Checks if the Maximum Number of Supersteps has been configured,
   * otherwise uses the default value.
   *
   * @return the maximum number of supersteps for which the algorithm should
   * run
   */
  public long getMaxNumberOfSuperSteps() {
    return this.conf.getInt("JaBeJa.MaxNumberOfSupersteps",
      JaBeJaConfigurations.DEFAULT_MAX_NUMBER_OF_SUPERSTEPS);
  }

  /**
   * @return the configured or default number of different colors in
   * the current graph.
   */
  public int getNumberOfColors() {
    return this.conf.getInt("JaBeJa.NumberOfColors",
      JaBeJaConfigurations.DEFAULT_NUMBER_OF_COLORS);
  }

  /**
   * alpha is the value used to adjust the sum of node degrees
   * ({@code getJaBeJaSum}). Default value is 2
   *
   * @return the alpha value for JaBeJa`s comparison function
   */
  public double getAlpha() {
    return (double) this.conf.getFloat("JaBeJa.Alpha",
      JaBeJaConfigurations.DEFAULT_ALPHA);
  }

  /**
   * Used for the simulated annealing algorithm, the initial temperature
   *
   * @return the initial temperature for simulated annealing
   */
  public double getInitialTemperature() {
    return (double) this.conf.getFloat("JaBeJa.InitialTemperature",
      JaBeJaConfigurations.DEFAULT_INITIAL_TEMPERATURE);
  }

  /**
   * Used for the simulated annealing algorithm, the factor by which the
   * initial temperature cools down every JaBeJa-round
   *
   * @return the factor by which the temperature cools down each round
   */
  public double getCoolingFactor() {
    return (double) this.conf.getFloat(
      "JaBeJa.CoolingFactor", DEFAULT_COOLING_FACTOR);
  }

  /**
   * Define if only direct matches should exchange colors or,
   * in case of no direct match, a negotiation about the exchange should happen
   *
   * @return flag if the complex color exchange should be used
   */
  public boolean useComplexColorExchange() {
    return this.conf.getBoolean("JaBeJa.UseComplexColorExchange",
      USE_COMPLEX_COLOR_EXCHANGE_DEFAULT);
  }

  /**
   * The random seed is used to create custom random generators for each node
   * to have with the same seed always reproducible outcomes.
   * <p/>
   * if you want to have a totally random number just set the config to -1
   *
   * @return the configured or default seed for the random generator
   */
  public int getRandomSeed() {
    return this.conf.getInt("JaBeJa.RandomSeed", DEFAULT_RANDOM_SEED);
  }
}
