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
package com.datatorrent.lib.metric.max;

import java.io.Serializable;
import java.util.Collection;

import com.datatorrent.api.annotation.Name;

import com.datatorrent.common.metric.SingleMetricAggregator;

/**
 * @since 3.1.0
 */

@Name("max")
public class IntMaxAggregator implements SingleMetricAggregator, Serializable
{
  @Override
  public Object aggregate(Collection<Object> metricValues)
  {
    Integer max = null;
    for (Object value : metricValues) {
      int ival = ((Number) value).intValue();
      if (max == null || ival > max) {
        max = ival;
      }
    }
    return max;
  }

  private static final long serialVersionUID = 201504081326L;
}
