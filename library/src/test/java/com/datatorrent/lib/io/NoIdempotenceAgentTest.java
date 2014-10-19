/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.io;

import com.esotericsoftware.kryo.Kryo;
import org.junit.Test;

public class NoIdempotenceAgentTest
{
  public static final int OPERATOR_ID = 0;
  public static final String APPLICATION_ID = "1";

  @Test
  public void testSerialization()
  {
    Kryo kryo = new Kryo();

    NoIdempotenceAgent<Integer> agentA = new NoIdempotenceAgent<Integer>();
    NoIdempotenceAgent<Integer> agentB = kryo.copy(agentA);
  }
}
