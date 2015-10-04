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
package com.datatorrent.lib.multiwindow;


import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Provides information needed to calculate simple moving average.
 *
 * @displayName Simple Moving Average Object
 * @category Stats and Aggregations
 * @tags average, sum, count
 * @since 0.3.2
 */
public class SimpleMovingAverageObject
{
  private MutableDouble sum;
  private MutableInt count;

  public double getSum()
  {
    return sum.doubleValue();
  }

  public int getCount()
  {
    return count.intValue();
  }

  public SimpleMovingAverageObject()
  {
    sum = new MutableDouble(0);
    count = new MutableInt(0);
  }

  public void add(double d)
  {
    sum.add(d);
    count.add(1);
  }

  public void clear()
  {
    sum.setValue(0);
    count.setValue(0);
  }
}
