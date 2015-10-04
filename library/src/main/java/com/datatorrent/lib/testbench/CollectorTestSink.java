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
package com.datatorrent.lib.testbench;

import com.datatorrent.api.Sink;

import java.util.ArrayList;
import java.util.List;

/**
 * A sink implementation to collect expected test results.
 * <p>
 * @displayName Collector Test Sink
 * @category Test Bench
 * @tags sink
 * @since 0.3.2
 */
public class CollectorTestSink<T> implements Sink<T>
{
  final public List<T> collectedTuples = new ArrayList<T>();

  /**
   * clears data
   */
  public void clear()
  {
    this.collectedTuples.clear();
  }

  @Override
  public void put(T payload)
  {
      synchronized (collectedTuples) {
        collectedTuples.add(payload);
        collectedTuples.notifyAll();
      }
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < count && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < count) {
          collectedTuples.wait(20);
        }
      }
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
