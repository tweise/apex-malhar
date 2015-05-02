/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.twitter;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.lib.util.BaseNumberKeyValueOperator;

import java.util.HashMap;
import java.util.Map;

public class UnifierHashMapSumKeys extends BaseNumberKeyValueOperator<String,Integer> implements Unifier<HashMap<String, Integer>>
{
  public HashMap<String, Integer> mergedTuple = new HashMap<String, Integer>();
  /**
   * This is the output port which emits key value pairs which map keys to sums.
   */
  public final transient DefaultOutputPort<Map<String, Integer>> mergedport = new DefaultOutputPort<Map<String, Integer>>();

  @Override
  public void process(HashMap<String, Integer> tuple)
  {
    for (Map.Entry<String, Integer> e: tuple.entrySet()) {
      Integer val = mergedTuple.get(e.getKey());
      if (val == null) {
        mergedTuple.put(e.getKey(), e.getValue());
      }
      else {
        val += e.getValue();
        mergedTuple.put(e.getKey(), val);
      }
    }
  }

  /**
   * emits mergedTuple on mergedport if it is not empty
   */
  @Override
  public void endWindow()
  {
    if (!mergedTuple.isEmpty()) {
      HashMap<String, Integer> stuples = new HashMap<String, Integer>();
      for (Map.Entry<String, Integer> e: mergedTuple.entrySet()) {
        stuples.put(e.getKey(), getValue(e.getValue()));
      }
      mergedport.emit(stuples);
      mergedTuple = new HashMap<String, Integer>();
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
