/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.contrib.goldengate.DBQueryProcessor;
import com.datatorrent.contrib.goldengate.FileQueryProcessor;
import com.datatorrent.contrib.goldengate.KafkaJsonEncoder;
import com.datatorrent.contrib.goldengate.lib.*;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="GoldenGateDemo")
public class GoldenGateApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInput kafkaInput = dag.addOperator("GoldenGateInput", KafkaInput.class);
    CSVFileOutput csvFileOutput = dag.addOperator("CSVReplicator", CSVFileOutput.class);
    CSVFileInput csvFileReader = dag.addOperator("CSVReader", CSVFileInput.class);
    ConsoleOutputOperator console = dag.addOperator("debug", ConsoleOutputOperator.class);

    GoldenGateJMSOutputOperator jms = dag.addOperator("GoldenGateWriter", GoldenGateJMSOutputOperator.class);

    jms.setUser("");
    jms.setPassword("");
    jms.setUrl("tcp://node0.morado.com:61616");
    jms.setAckMode("CLIENT_ACKNOWLEDGE");
    jms.setClientId("ggdemo_jms_client");
    jms.setSubject("ggdemo");
    jms.setMaximumSendMessages(Integer.MAX_VALUE);
    jms.setMessageSize(Integer.MAX_VALUE);
    jms.setBatch(1);
    jms.setTopic(false);
    jms.setDurable(false);
    jms.setTransacted(false);
    jms.setVerbose(true);

    dag.addStream("CSVReplicatorStream", kafkaInput.outputPort, console.input);
    dag.addStream("GoldenGateWriterStream", kafkaInput.transactionPort, jms.inputPort);

    dag.addStream("csvOutputLines", csvFileReader.outputPort, console.input);

    ////

    KafkaSinglePortStringInputOperator dbQueryInput = dag.addOperator("DBQuery", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor dbQueryProcessor = dag.addOperator("DBQueryProcessor", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> dbQueryOutput = dag.addOperator("DBQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    dbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("dbQueries", dbQueryInput.outputPort, dbQueryProcessor.queryInput);
    dag.addStream("dbRows", dbQueryProcessor.queryOutput, dbQueryOutput.inputPort);

    ////

    KafkaSinglePortStringInputOperator fileQueryInput = dag.addOperator("FileQuery", KafkaSinglePortStringInputOperator.class);
    FileQueryProcessor fileQueryProcessor = dag.addOperator("FileQueryProcessor", FileQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> fileQueryOutput = dag.addOperator("FileQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());

    fileQueryOutput.setConfigProperties(configProperties);

    dag.addStream("fileQueries", fileQueryInput.outputPort, fileQueryProcessor.queryInput);
    dag.addStream("fileData", fileQueryProcessor.queryOutput, fileQueryOutput.inputPort);
  }
}
