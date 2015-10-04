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

import com.datatorrent.lib.streamquery.SelectFunctionOperator;
import com.datatorrent.lib.streamquery.function.FirstLastFunction;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link com.datatorrent.lib.streamquery.SelectOperatorTest}.
 */
public class SelectFirstLastTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
  	// create operator   
  	SelectFunctionOperator oper = new SelectFunctionOperator();
  	oper.addSqlFunction(new FirstLastFunction("b", null, false));
  	
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outport.setSink(sink);
  	
  	oper.setup(null);
  	oper.beginWindow(1);
  	
  	HashMap<String, Object> tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", null);
  	tuple.put("c", 2);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", null);
  	tuple.put("c", 4);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 1);
  	tuple.put("b", 5);
  	tuple.put("c", 6);
  	oper.inport.process(tuple);
  	
  	oper.endWindow();
  	oper.teardown();
  	
  	System.out.println(sink.collectedTuples.toString());
  }
}
