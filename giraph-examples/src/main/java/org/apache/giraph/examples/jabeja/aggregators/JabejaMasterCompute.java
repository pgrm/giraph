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
package org.apache.giraph.examples.jabeja.aggregators;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Compute class for using aggrgators
 * */
public class JabejaMasterCompute extends DefaultMasterCompute {
  /** Name of the Energy funcation valus aggregator */
  public static final String ENEREGY = "Energy";

  @Override
  public void compute() {
    /**
     * This gives the enery after each super step.
     */
    // DoubleWritable val = getAggregatedValue(ENEREGY);
  }

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    registerAggregator(ENEREGY, DoubleSumAggregator.class);
  }
}
