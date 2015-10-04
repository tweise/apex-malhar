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
package com.datatorrent.benchmark;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.PortContext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationFixedTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    LocalMode lma = LocalMode.newInstance();
    new ApplicationFixed().populateDAG(lma.getDAG(), new Configuration(false));

    DAG dag = lma.cloneDAG();
    FixedTuplesInputOperator wordGenerator = (FixedTuplesInputOperator)dag.getOperatorMeta("WordGenerator").getOperator();
    Assert.assertEquals("Queue Capacity", ApplicationFixed.QUEUE_CAPACITY, (int)dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getValue(PortContext.QUEUE_CAPACITY));

    LocalMode.Controller lc = lma.getController();
    lc.run(60000);
  }
}
