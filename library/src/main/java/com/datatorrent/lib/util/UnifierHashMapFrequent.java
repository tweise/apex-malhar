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
package com.datatorrent.lib.util;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.Unifier;

import java.util.HashMap;
import java.util.Map;

/**
 * This unifier consumes key value pairs in the form of a hash map, where the key is an object and the value is an integer.&nbsp;
 * The operator emits either the largest or smallest value associated with each key at the end of each application window.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Hash Map Frequent
 * @category Algorithmic
 * @tags numeric
 * @since 0.3.2
 */
public class UnifierHashMapFrequent<K> implements Unifier<HashMap<K, Integer>>
{
  HashMap<K, Integer> mergedTuple = new HashMap<K, Integer>();
  /**
   * This is the output port which emits either the largest or smallest value for each input key in the form of hash map.
   */
  public final transient DefaultOutputPort<HashMap<K, Integer>> mergedport = new DefaultOutputPort<HashMap<K, Integer>>();

  Integer lval;
  boolean least = true;
  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, Integer> tuple)
  {
    // Tuple is HashMap<K,Integer>(1)
    if (mergedTuple.isEmpty()) {
      mergedTuple.putAll(tuple);
      for (Map.Entry<K, Integer> e: tuple.entrySet()) {
        lval = e.getValue();
        break;
      }
    }
    else {
      for (Map.Entry<K, Integer> e: tuple.entrySet()) {
        if ((least && (e.getValue() < lval))
                || (!least && (e.getValue() > lval))) {
          mergedTuple.clear();
          mergedTuple.put(e.getKey(), e.getValue());
          break;
        }
      }
    }
  }

  /**
   * getter function for combiner doing least (true) or most (false) compare
   * @return least flag
   */
  public boolean getLeast()
  {
    return least;
  }

  /**
   * setter funtion for combiner doing least (true) or most (false) compare
   * @param b
   */
  public void setLeast(boolean b)
  {
    least = b;
  }

  /**
   * a no op
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      mergedport.emit(mergedTuple);
      mergedTuple = new HashMap<K, Integer>();
    }
  }

  /**
   * a no-op
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
  }

  /**
   * a noop
   */
  @Override
  public void teardown()
  {
  }
}
