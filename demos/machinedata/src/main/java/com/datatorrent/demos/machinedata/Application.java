/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.demos.machinedata;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.machinedata.data.MachineKey;
import com.datatorrent.demos.machinedata.operator.MachineInfoAveragingOperator;
import com.datatorrent.demos.machinedata.operator.MachineInfoAveragingPrerequisitesOperator;
import com.datatorrent.contrib.redis.RedisKeyValPairOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Resource monitor application.
 * </p>
 *
 * @since 0.3.5
 */
@ApplicationAnnotation(name = "MachineDataDemo")
@SuppressWarnings("unused")
public class Application implements StreamingApplication
{

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  /**
   * This function sets up the DAG for calculating the average
   *
   * @param dag  the DAG instance
   * @param conf the configuration instance
   * @return MachineInfoAveragingPrerequisitesOperator
   */
  private MachineInfoAveragingPrerequisitesOperator addAverageCalculation(DAG dag, Configuration conf)
  {
    MachineInfoAveragingPrerequisitesOperator prereqAverageOper = dag.addOperator("Aggregator", MachineInfoAveragingPrerequisitesOperator.class);
    MachineInfoAveragingOperator averageOperator = dag.addOperator("AverageCalculator", MachineInfoAveragingOperator.class);
    RedisKeyValPairOutputOperator<MachineKey, Map<String, String>> redisAvgOperator = dag.addOperator("Persister", new RedisKeyValPairOutputOperator<MachineKey, Map<String, String>>());
    dag.addStream("Average", averageOperator.outputPort, redisAvgOperator.input);
    SmtpOutputOperator smtpOutputOperator = dag.addOperator("Alerter", new SmtpOutputOperator());
    dag.addStream("Aggregates", prereqAverageOper.outputPort, averageOperator.inputPort);
    dag.addStream("Alerts", averageOperator.smtpAlert, smtpOutputOperator.input);
    return prereqAverageOper;
  }

  /**
   * Create the DAG
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputReceiver randomGen = dag.addOperator("Receiver", InputReceiver.class);
    DimensionGenerator dimensionGenerator = dag.addOperator("DimensionsGenerator", DimensionGenerator.class);
    dag.addStream("Events", randomGen.outputInline, dimensionGenerator.inputPort);
    MachineInfoAveragingPrerequisitesOperator prereqAverageOper = addAverageCalculation(dag, conf);
    dag.addStream("DimensionalData", dimensionGenerator.outputInline, prereqAverageOper.inputPort);
  }

}
