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
package com.datatorrent.benchmark.kafka;

import com.google.common.collect.Sets;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.HighlevelKafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaConsumer;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;


/**
 * The stream app to test the benckmark of kafka
 * You can set the property file to make it using either {@link SimpleKafkaConsumer} or {@link HighlevelKafkaConsumer}
 * The performance are pretty close
 *
 * @since 0.9.3
 */
@ApplicationAnnotation(name="KafkaInputBenchmark")
public class KafkaInputBenchmark implements StreamingApplication
{

  public static class CollectorModule extends BaseOperator
  {
    public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>() {

      @Override
      public void process(String arg0)
      {
      }
    };
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    dag.setAttribute(DAG.APPLICATION_NAME, "KafkaInputOperatorPartitionDemo");
    BenchmarkKafkaInputOperator bpkio = new BenchmarkKafkaInputOperator();


    String type = conf.get("kafka.consumertype", "simple");

    KafkaConsumer consumer = null;


    if (type.equals("highlevel")) {
      // Create template high-level consumer

      Properties props = new Properties();
      props.put("group.id", "main_group");
      props.put("auto.offset.reset", "smallest");
      consumer = new HighlevelKafkaConsumer(props);
    } else {
      // topic is set via property file
      consumer = new SimpleKafkaConsumer(null, 10000, 100000, "test_kafka_autop_client", null);
    }

    bpkio.setZookeeper(conf.get("dt.kafka.zookeeper"));
    bpkio.setInitialPartitionCount(1);
    //bpkio.setTuplesBlast(1024 * 1024);
    bpkio.setConsumer(consumer);

    bpkio = dag.addOperator("KafkaBenchmarkConsumer", bpkio);

    CollectorModule cm = dag.addOperator("DataBlackhole", CollectorModule.class);
    dag.addStream("end", bpkio.oport, cm.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(cm.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setAttribute(bpkio, OperatorContext.COUNTERS_AGGREGATOR, new KafkaConsumer.KafkaMeterStatsAggregator());
//    dag.setAttribute(bpkio, OperatorContext.STATS_LISTENER, KafkaMeterStatsListener.class);


  }

}
