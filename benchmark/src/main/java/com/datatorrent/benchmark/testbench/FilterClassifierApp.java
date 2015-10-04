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
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.lib.testbench.FilterClassifier;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;

/**
 * Benchmark App for FilterClassifier Operator.
 * This operator is benchmarked to emit 700K tuples/sec on cluster node.
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name = "FilterClassifierApp")
public class FilterClassifierApp implements StreamingApplication
{
  private final Locality locality = null;
  public static final int QUEUE_CAPACITY = 16 * 1024;
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FilterClassifier filter = dag.addOperator("filter", new FilterClassifier());

    HashMap<String, Double> kmap = new HashMap<String, Double>(3);
    kmap.put("a", 1.0);
    kmap.put("b", 4.0);
    kmap.put("c", 5.0);

    ArrayList<Integer> list = new ArrayList<Integer>(3);
    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>(4);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);

    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);

    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);

    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);

    filter.setKeyMap(kmap);
    filter.setKeyWeights(wmap);
    filter.setPassFilter(10);
    filter.setTotalFilter(100);

    HashMapOperator hmapOper = dag.addOperator("hmapOper", new HashMapOperator());
    DevNull<HashMap<String,Double>> dev = dag.addOperator("dev",  new DevNull());
    dag.addStream("filter1",hmapOper.hmap_data,filter.data).setLocality(locality);
    dag.addStream("filer2",filter.filter,dev.data).setLocality(locality);

  }
}

