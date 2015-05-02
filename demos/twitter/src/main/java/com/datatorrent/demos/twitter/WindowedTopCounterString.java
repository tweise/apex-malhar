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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class WindowedTopCounterString extends BaseOperator
{
  public static final String FIELD_TYPE = "type";
  public static final String FIELD_COUNT = "count";

  private static final Logger logger = LoggerFactory.getLogger(WindowedTopCounter.class);

  private PriorityQueue<SlidingContainer<String>> topCounter;
  private int windows;
  private int slidingWindowWidth;
  private int dagWindowWidth;
  private int topCount = 10;
  private HashMap<String, SlidingContainer<String>> objects = new HashMap<String, SlidingContainer<String>>();

  /**
   * Input port on which map objects containing keys with their respective frequency as values will be accepted.
   */
  public final transient DefaultInputPort<Map<String, Integer>> input = new DefaultInputPort<Map<String, Integer>>()
  {
    @Override
    public void process(Map<String, Integer> map)
    {
      for (Map.Entry<String, Integer> e : map.entrySet()) {
        SlidingContainer<String> holder = objects.get(e.getKey());
        if (holder == null) {
          holder = new SlidingContainer<String>(e.getKey(), getWindows());
          objects.put(e.getKey(), holder);
        }
        holder.adjustCount(e.getValue());
      }
    }
  };

  public final transient DefaultOutputPort<List<Map<String, Object>>> output = new DefaultOutputPort<List<Map<String, Object>>>();

  @Override
  public void setup(OperatorContext context)
  {
    windows = (int)(slidingWindowWidth / dagWindowWidth) + 1;
    if (slidingWindowWidth % dagWindowWidth != 0) {
      logger.warn("slidingWindowWidth(" + slidingWindowWidth + ") is not exact multiple of dagWindowWidth(" + dagWindowWidth + ")");
    }
    
    topCounter = new PriorityQueue<SlidingContainer<String>>(this.topCount, new TopSpotComparator());
  }

  @Override
  public void beginWindow(long windowId)
  {
    topCounter.clear();
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<String, SlidingContainer<String>>> iterator = objects.entrySet().iterator();
    int i = topCount;

    /*
     * Try to fill the priority queue with the first topCount URLs.
     */
    SlidingContainer<String> holder;
    while (iterator.hasNext()) {
      holder = iterator.next().getValue();
      holder.slide();

      if (holder.totalCount == 0) {
        iterator.remove();
      }
      else {
        topCounter.add(holder);
        if (--i == 0) {
          break;
        }
      }
    }
    logger.debug("objects.size(): {}", objects.size());

    /*
     * Make room for the new element in the priority queue by deleting the
     * smallest one, if we KNOW that the new element is useful to us.
     */
    if (i == 0) {
      int smallest = topCounter.peek().totalCount;
      while (iterator.hasNext()) {
        holder = iterator.next().getValue();
        holder.slide();

        if (holder.totalCount > smallest) {
          topCounter.poll();
          topCounter.add(holder);
          smallest = topCounter.peek().totalCount;
        }
        else if (holder.totalCount == 0) {
          iterator.remove();
        }
      }
    }

    List<Map<String, Object>> data = Lists.newArrayList();

    Iterator<SlidingContainer<String>> topIter = topCounter.iterator();

    while(topIter.hasNext()) {
      final SlidingContainer<String> wh = topIter.next();
      Map<String, Object> tableRow = Maps.newHashMap();

      tableRow.put(FIELD_TYPE, wh.identifier.toString());
      tableRow.put(FIELD_COUNT, wh.totalCount);

      data.add(tableRow);
    }

    output.emit(data);
    topCounter.clear();
  }

  @Override
  public void teardown()
  {
    topCounter = null;
    objects = null;
  }

  /**
   * Set the count of most frequently occurring keys to emit per map object.
   *
   * @param count count of the objects in the map emitted at the output port.
   */
  public void setTopCount(int count)
  {
    topCount = count;
  }

  /**
   * @return the windows
   */
  public int getWindows()
  {
    return windows;
  }

  /**
   * @param windows the windows to set
   */
  public void setWindows(int windows)
  {
    this.windows = windows;
  }

  /**
   * @return the slidingWindowWidth
   */
  public int getSlidingWindowWidth()
  {
    return slidingWindowWidth;
  }

  /**
   * Set the width of the sliding window.
   *
   * Sliding window is typically much larger than the dag window. e.g. One may want to measure the most frequently
   * occurring keys over the period of 5 minutes. So if dagWindowWidth (which is by default 500ms) is set to 500ms,
   * the slidingWindowWidth would be (60 * 5 * 1000 =) 300000.
   *
   * @param slidingWindowWidth - Sliding window width to be set for this operator, recommended to be multiple of DAG window.
   */
  public void setSlidingWindowWidth(int slidingWindowWidth)
  {
    this.slidingWindowWidth = slidingWindowWidth;
  }

  /**
   * @return the dagWindowWidth
   */
  public int getDagWindowWidth()
  {
    return dagWindowWidth;
  }

  /**
   * Set the width of the sliding window.
   *
   * Sliding window is typically much larger than the dag window. e.g. One may want to measure the most frequently
   * occurring keys over the period of 5 minutes. So if dagWindowWidth (which is by default 500ms) is set to 500ms,
   * the slidingWindowWidth would be (60 * 5 * 1000 =) 300000.
   *
   * @param dagWindowWidth - DAG's native window width. It has to be the value of the native window set at the application level.
   */
  public void setDagWindowWidth(int dagWindowWidth)
  {
    this.dagWindowWidth = dagWindowWidth;
  }

  static class TopSpotComparator implements Comparator<SlidingContainer<?>>
  {
    @Override
    public int compare(SlidingContainer<?> o1, SlidingContainer<?> o2)
    {
      if (o1.totalCount > o2.totalCount) {
        return 1;
      }
      else if (o1.totalCount < o2.totalCount) {
        return -1;
      }

      return 0;
    }
  }
}

