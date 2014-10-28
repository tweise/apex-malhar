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
    //CSVFileInput csvFileReader = dag.addOperator("CSVReader", CSVFileInput.class);
    //ConsoleOutputOperator console = dag.addOperator("debug", ConsoleOutputOperator.class);
    GoldenGateJMSOutputOperator jms = dag.addOperator("GoldenGateWriter", GoldenGateJMSOutputOperator.class);

    dag.addStream("CSVReplicatorStream", kafkaInput.outputPort, csvFileOutput.input);
    dag.addStream("GoldenGateWriterStream", kafkaInput.transactionPort, jms.inputPort);
    //dag.addStream("csvOutputLines", csvFileReader.outputPort, console.input);

    ////

    KafkaSinglePortStringInputOperator dbQueryInput = dag.addOperator("DBQuery", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor dbQueryProcessor = dag.addOperator("DBQueryProcessor", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> dbQueryOutput = dag.addOperator("DBQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());

    KafkaSinglePortStringInputOperator odbQueryInput = dag.addOperator("ODBQuery", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor odbQueryProcessor = dag.addOperator("ODBQueryProcessor", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> odbQueryOutput = dag.addOperator("ODBQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());


    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    dbQueryOutput.setConfigProperties(configProperties);
    odbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("dbQueries", dbQueryInput.outputPort, dbQueryProcessor.queryInput);
    dag.addStream("dbRows", dbQueryProcessor.queryOutput, dbQueryOutput.inputPort);

    dag.addStream("odbQueries", odbQueryInput.outputPort, odbQueryProcessor.queryInput);
    dag.addStream("odbRows", odbQueryProcessor.queryOutput, odbQueryOutput.inputPort);

    ////

    KafkaSinglePortStringInputOperator fileQueryInput = dag.addOperator("FileQuery", KafkaSinglePortStringInputOperator.class);
    FileQueryProcessor fileQueryProcessor = dag.addOperator("FileQueryProcessor", FileQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> fileQueryOutput = dag.addOperator("FileQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());
    fileQueryOutput.setConfigProperties(configProperties);

    //fileQueryOutput.setConfigProperties(configProperties);

    dag.addStream("fileQueries", fileQueryInput.outputPort, fileQueryProcessor.queryInput);
    dag.addStream("fileData", fileQueryProcessor.queryOutput, fileQueryOutput.inputPort);
  }
}
