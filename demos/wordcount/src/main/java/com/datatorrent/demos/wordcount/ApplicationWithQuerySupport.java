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
package com.datatorrent.demos.wordcount;

import java.net.URI;

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="TopNWordsWithQueries")
public class ApplicationWithQuerySupport implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationWithQuerySupport.class);

  public static final String SNAPSHOT_SCHEMA = "WordDataSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    LineReader lineReader            = dag.addOperator("lineReader", new LineReader());
    WordReader wordReader            = dag.addOperator("wordReader", new WordReader());
    WindowWordCount windowWordCount  = dag.addOperator("windowWordCount", new WindowWordCount());
    FileWordCount fileWordCount      = dag.addOperator("fileWordCount", new FileWordCount());
    WordCountWriter wcWriter         = dag.addOperator("wcWriter", new WordCountWriter());
    ConsoleOutputOperator console    = dag.addOperator("console", new ConsoleOutputOperator());
    console.setStringFormat("wordCount: %s");

    // create streams

    dag.addStream("lines",   lineReader.output,  wordReader.input);
    dag.addStream("control", lineReader.control, fileWordCount.control);
    dag.addStream("words",   wordReader.output,  windowWordCount.input);
    dag.addStream("windowWordCounts", windowWordCount.output, fileWordCount.input);
    dag.addStream("fileWordCounts", fileWordCount.fileOutput, wcWriter.input);

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);

    if ( ! StringUtils.isEmpty(gatewayAddress)) {        // add query support
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

      AppDataSnapshotServerMap snapshotServerFile
        = dag.addOperator("snapshotServerFile", new AppDataSnapshotServerMap());
      AppDataSnapshotServerMap snapshotServerGlobal
        = dag.addOperator("snapshotServerGlobal", new AppDataSnapshotServerMap());

      String snapshotServerJSON = SchemaUtils.jarResourceFileToString(SNAPSHOT_SCHEMA);
      snapshotServerFile.setSnapshotSchemaJSON(snapshotServerJSON);
      snapshotServerGlobal.setSnapshotSchemaJSON(snapshotServerJSON);

      PubSubWebSocketAppDataQuery wsQueryFile = new PubSubWebSocketAppDataQuery();
      PubSubWebSocketAppDataQuery wsQueryGlobal = new PubSubWebSocketAppDataQuery();
      wsQueryFile.setUri(uri);
      wsQueryGlobal.setUri(uri);

      snapshotServerFile.setEmbeddableQueryInfoProvider(wsQueryFile);
      snapshotServerGlobal.setEmbeddableQueryInfoProvider(wsQueryGlobal);

      PubSubWebSocketAppDataResult wsResultFile
        = dag.addOperator("wsResultFile", new PubSubWebSocketAppDataResult());
      PubSubWebSocketAppDataResult wsResultGlobal
        = dag.addOperator("wsResultGlobal", new PubSubWebSocketAppDataResult());
      wsResultFile.setUri(uri);
      wsResultGlobal.setUri(uri);

      Operator.InputPort<String> queryResultFilePort = wsResultFile.input;
      Operator.InputPort<String> queryResultGlobalPort = wsResultGlobal.input;

      dag.addStream("WordCountsFile", fileWordCount.outputPerFile,
                    snapshotServerFile.input, console.input);
      dag.addStream("WordCountsGlobal", fileWordCount.outputGlobal,
                    snapshotServerGlobal.input);

      dag.addStream("ResultFile", snapshotServerFile.queryResult, queryResultFilePort);
      dag.addStream("ResultGlobal", snapshotServerGlobal.queryResult, queryResultGlobalPort);
    } else {
      //throw new RuntimeException("Error: No GATEWAY_CONNECT_ADDRESS");
      dag.addStream("WordCounts", fileWordCount.outputPerFile, console.input);
    }

    System.out.println("done with populateDAG, isDebugEnabled = " + LOG.isDebugEnabled());
    LOG.info("Returning from populateDAG");
  }

}
