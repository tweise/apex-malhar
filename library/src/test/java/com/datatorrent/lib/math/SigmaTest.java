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
package com.datatorrent.lib.math;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.testbench.SumTestSink;

/**
 * 
 * Functional tests for {@link com.datatorrent.lib.math.Sigma}
 * <p>
 * 
 */
public class SigmaTest
{
	/**
	 * Test oper logic emits correct results
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testNodeSchemaProcessing()
	{
		Sigma oper = new Sigma();
		SumTestSink lmultSink = new SumTestSink();
		SumTestSink imultSink = new SumTestSink();
		SumTestSink dmultSink = new SumTestSink();
		SumTestSink fmultSink = new SumTestSink();
		oper.longResult.setSink(lmultSink);
		oper.integerResult.setSink(imultSink);
		oper.doubleResult.setSink(dmultSink);
		oper.floatResult.setSink(fmultSink);

		int sum = 0;
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < 100; i++) {
			list.add(i);
			sum += i;
		}

		oper.beginWindow(0); //
		oper.input.process(list);
		oper.endWindow(); //

		oper.beginWindow(1); //
		oper.input.process(list);
		oper.endWindow(); //
		sum = sum * 2;

		Assert.assertEquals("sum was", sum, lmultSink.val.intValue());
		Assert.assertEquals("sum was", sum, imultSink.val.intValue());
		Assert.assertEquals("sum was", sum, dmultSink.val.intValue());
		Assert.assertEquals("sum", sum, fmultSink.val.intValue());
	}
}
