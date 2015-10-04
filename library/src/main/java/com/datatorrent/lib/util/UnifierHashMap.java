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

/**
 * This unifier combines all the hash maps it receives within an application window,
 * and emits the combined hash map at the end of the application window.
 * <p>
 * The processing is done with sticky key partitioning, i.e. each one key belongs only to one partition.
 * </p>
 * @displayName Unifier Hash Map
 * @category Algorithmic
 * @tags key value
 * @since 0.3.2
 */
public class UnifierHashMap<K, V> implements Unifier<HashMap<K, V>>
{
  public HashMap<K, V> mergedTuple = new HashMap<K, V>();
  /**
   * This is the output port which emits a merged hashmap.
   */
  public final transient DefaultOutputPort<HashMap<K, V>> mergedport = new DefaultOutputPort<HashMap<K, V>>();

  /**
   * combines the tuple into a single final tuple which is emitted in endWindow
   * This is a merge metric for operators that use sticky key partition
   * @param tuple incoming tuple from a partition
   */
  @Override
  public void process(HashMap<K, V> tuple)
  {
    mergedTuple.putAll(tuple);
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
    if (!mergedTuple.isEmpty())  {
      mergedport.emit(mergedTuple);
      mergedTuple = new HashMap<K, V>();
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
