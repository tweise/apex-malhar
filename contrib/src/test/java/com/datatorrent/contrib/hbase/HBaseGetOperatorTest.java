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
package com.datatorrent.contrib.hbase;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 *
 */
public class HBaseGetOperatorTest
{
  //private static final Logger logger = LoggerFactory.getLogger(HBaseGetOperatorTest.class);

  public HBaseGetOperatorTest()
  {
  }

  @Test
  public void testGet()
  {
    try {
      //HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.populateHBase();
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      dag.setAttribute(DAG.APPLICATION_NAME, "HBaseGetOperatorTest");
      TestHBaseGetOperator thop = dag.addOperator("testhbaseget", TestHBaseGetOperator.class);
      HBaseTupleCollector tc = dag.addOperator("tuplecollector", HBaseTupleCollector.class);
      dag.addStream("ss", thop.outputPort, tc.inputPort);

      thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2181);

      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      lc.run(30000);

      /*
      tuples = new ArrayList<HBaseTuple>();
      TestHBaseGetOperator thop = new TestHBaseGetOperator();
           thop.setTableName("table1");
      thop.setZookeeperQuorum("127.0.0.1");
      thop.setZookeeperClientPort(2822);
      thop.setupConfiguration();

      thop.emitTuples();
      */

      // TODO review the generated test code and remove the default call to fail.
      //fail("The test case is a prototype.");
      // Check total number
      List<HBaseTuple> tuples = HBaseTupleCollector.tuples;
      Assert.assertTrue(tuples.size() > 0);
      HBaseTuple tuple = HBaseTestHelper.findTuple(tuples, "row0", "colfam0", "col-0");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-0");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-0");
      Assert.assertTrue(tuples.size() >= 499);
      tuple = HBaseTestHelper.findTuple(tuples, "row0", "colfam0", "col-499");
      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(), "col-499");
      Assert.assertEquals("Tuple column value", tuple.getColValue(), "val-0-499");
    }
    catch (Exception ex) {
      ex.printStackTrace();
      //logger.error(ex.getMessage());
      assert false;
    }
  }

  public static class TestHBaseGetOperator extends HBaseGetOperator<HBaseTuple>
  {

    @Override
    public Get operationGet()
    {
      Get get = new Get(Bytes.toBytes("row0"));
      get.addFamily(HBaseTestHelper.colfam0_bytes);
      return get;
    }

    @Override
    //protected HBaseTuple getTuple(KeyValue[] kvs)
    protected HBaseTuple getTuple(KeyValue kv)
    {
      return HBaseTestHelper.getHBaseTuple(kv);
    }

  }

}
