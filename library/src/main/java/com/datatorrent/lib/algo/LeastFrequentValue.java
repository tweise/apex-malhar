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
package com.datatorrent.lib.algo;

import java.util.ArrayList;
import java.util.HashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.util.AbstractBaseFrequentKey;

/**
 * This operator filters the incoming stream of values by emitting the value or values (if there is a tie)
 * that occurred the fewest number of times within each window to the output port "list".&nbsp;
 * One of the values is emitted to the output port "least" at the end of each window.
 * <p>
 * Occurrences of each tuple is counted and at the end of window any of the least frequent tuple is emitted on output port 'least'
 * All keys with same least frequency value least are emitted on output port 'list'.<br>
 * This module is an end of window module<br>
 * In case of a tie any of the least key would be emitted. The list port would however have all the tied keys
 * <br>
 * <b>StateFull : Yes, </b> tuple are compared across application window(s). <br>
 * <b>Partitions : Yes, </b> least keys are unified on output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>least</b>: emits HashMap&lt;K,Integer&gt;(1), Where K is the least occurring key in the window.
 *               In case of tie any of the least key would be emitted<br>
 * <b>list</b>: emits ArrayList&lt;HashMap&lt;K,Integer&gt;(1)&gt, Where the list includes all the keys that are least frequent<br>
 * <br>
 * </p>
 *
 * @displayName Emit Least Frequent Value
 * @category Rules and Alerts
 * @tags filter, count
 *
 * @since 0.3.3
 */

@OperatorAnnotation(partitionable = true)
public class LeastFrequentValue<K> extends AbstractBaseFrequentKey<K>
{
  /**
   * The input port on which tuples are received.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Calls super.processTuple(tuple)
     */
    @Override
    public void process(K tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * The output port on which one of the tuples,
   * which occurred the least number of times,
   * is emitted.
   */
  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<HashMap<K, Integer>> least = new DefaultOutputPort<HashMap<K, Integer>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<HashMap<K, Integer>> getUnifier()
    {
      return new LeastFrequentKeyUnifier<K>();
    }
  };

  /**
   * The output port on which all the tuples,
   * which occurred the least number of times,
   * is emitted.
   */
  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<ArrayList<HashMap<K, Integer>>> list = new DefaultOutputPort<ArrayList<HashMap<K, Integer>>>()
  {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Unifier<ArrayList<HashMap<K, Integer>>> getUnifier()
    {
      return new LeastFrequentKeyArrayUnifier<K>();
    }
  };

  /**
   * Emits tuple on port "least"
   * @param tuple
   */
  @Override
  public void emitTuple(HashMap<K, Integer> tuple)
  {
    least.emit(tuple);
  }

  /**
   * returns val1 < val2
   * @param val1
   * @param val2
   * @return val1 < val2
   */
  @Override
    public boolean compareCount(int val1, int val2)
  {
    return val1 < val2;
  }

  /**
   * Emits tuple on port "list"
   * @param tlist
   */
  @Override
  public void emitList(ArrayList<HashMap<K, Integer>> tlist)
  {
    list.emit(tlist);
  }
}
