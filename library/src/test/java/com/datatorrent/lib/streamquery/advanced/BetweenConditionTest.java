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
package com.datatorrent.lib.streamquery.advanced;

import java.util.HashMap;

import org.junit.Test;

import com.datatorrent.lib.streamquery.SelectOperator;
import com.datatorrent.lib.streamquery.condition.BetweenCondition;
import com.datatorrent.lib.streamquery.index.ColumnIndex;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link com.datatorrent.lib.streamquery.advanced.BetweenConditionTest}.
 */
public class BetweenConditionTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
  	// create operator   
  	SelectOperator oper = new SelectOperator();
  	oper.addIndex(new ColumnIndex("b", null));
  	oper.addIndex(new ColumnIndex("c", null));
  	
  	BetweenCondition cond = new BetweenCondition("a", 0, 2); 
  	oper.setCondition(cond);
  	
  	
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outport.setSink(sink);
  	
  	oper.setup(null);
  	oper.beginWindow(1);
  	
  	HashMap<String, Object> tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 1);
  	tuple.put("c", 2);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 3);
  	tuple.put("c", 4);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 2);
  	tuple.put("b", 5);
  	tuple.put("c", 6);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
    tuple.put("a", 3);
    tuple.put("b", 7);
    tuple.put("c", 8);
    oper.inport.process(tuple);
    
  	oper.endWindow();
  	oper.teardown();
  	
  	System.out.println(sink.collectedTuples.toString());
  }
}
