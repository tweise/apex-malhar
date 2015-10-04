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
package com.datatorrent.lib.helper;

import java.util.Collection;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

public class TestPortContext implements Context.PortContext
{
  public final Attribute.AttributeMap attributeMap;

  public TestPortContext(@NotNull Attribute.AttributeMap attributeMap)
  {
    this.attributeMap = Preconditions.checkNotNull(attributeMap, "attributes");
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attributeMap;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return attributeMap.get(key);
  }

  @Override
  public void setCounters(Object counters)
  {

  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {

  }
}
