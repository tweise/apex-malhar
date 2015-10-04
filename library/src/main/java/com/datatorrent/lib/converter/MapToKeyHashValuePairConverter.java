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
package com.datatorrent.lib.converter;

import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyHashValPair;

/**
 *
 * This operator outputs key value pair for each entry in input Map
 *
 * @displayName Map to key-value pair converter
 * @category Tuple Converters
 * @tags key value
 *
 * @since 3.0.0
 */
public class MapToKeyHashValuePairConverter<K, V> extends BaseOperator {

  /**
   * Input port which accepts Map<K, V>.
   */
  public final transient DefaultInputPort<Map<K, V>> input = new DefaultInputPort<Map<K, V>>()
  {
    @Override
    public void process(Map<K, V> tuple)
    {
      for(Entry<K, V> entry:tuple.entrySet())
      {
        output.emit(new KeyHashValPair<K, V>(entry.getKey(), entry.getValue()));
      }
    }
  };

  /*
   * Output port which outputs KeyValue pair for each entry in Map
   */
  public final transient DefaultOutputPort<KeyHashValPair<K, V>> output = new DefaultOutputPort<KeyHashValPair<K, V>>();
}
