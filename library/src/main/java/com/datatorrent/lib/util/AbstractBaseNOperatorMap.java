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

import java.util.Map;

import javax.validation.constraints.Min;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;

/**
 * This is the base implementation for the Top N sorting operators.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p>
 * Users need to provide processTuple, beginWindow, and endWindow to implement TopN operator
 * This is an end of window module. At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * </p>
 * <p>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects HashMap&lt;K,V&gt;<br>
 * <b>top</b>: emits HashMap&lt;K,ArrayList&lt;V&gt;&gt;<br>
 * <br>
 * <b>Properties</b>:
 * <b>N</b>: The number of top values to be emitted per key<br>
 * <br>
 * <b>Specific compile time checks are</b>:<br>
 * N: Has to be an integer<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmark</b>: Not done as this is an abstract operator<br>
 * </p>
 * @displayName Abstract Base N Map
 * @category Algorithmic
 * @tags rank, key value
 * @since 0.3.2
 */
abstract public class AbstractBaseNOperatorMap<K,V> extends BaseKeyValueOperator<K,V>
{
  /**
   * This is the input port that receives key value pairs.
   */
  public final transient DefaultInputPort<Map<K,V>> data = new DefaultInputPort<Map<K,V>>()
  {
    @Override
    public void process(Map<K,V> tuple)
    {
      processTuple(tuple);
    }

    @Override
    public StreamCodec<Map<K, V>> getStreamCodec()
    {
      StreamCodec<Map<K, V>> streamCodec = AbstractBaseNOperatorMap.this.getStreamCodec();
      if (streamCodec == null) {
        return super.getStreamCodec();
      } else {
        return streamCodec;
      }
    }


  };
  @Min(1)
  int n = 1;

  /**
   * Implementations should specify exactly how tuples should be processed
   *
   * @param tuple
   */
  abstract public void processTuple(Map<K,V> tuple);

  /**
   * Sets value of N (depth)
   * @param val
   */
  public void setN(int val)
  {
    n = val;
  }

  /**
   * getter function for N
   * @return n
   */
  @Min(1)
  public int getN()
  {
    return n;
  }

  /*
   * Provides ability for implemented operators to provide their own stream codec
   */
  protected StreamCodec<Map<K, V>> getStreamCodec()
  {
    return null;
  }
}
