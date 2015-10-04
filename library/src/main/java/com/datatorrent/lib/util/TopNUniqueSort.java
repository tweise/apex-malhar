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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.Min;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 * Gives top N objects in ascending or descending order and counts only unique objects<p>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N. There is no API to look at top of the list at any given time as per design. The aim is for users to only take the topN
 * once all the inserts are done<br>
 *
 * @since 0.3.2
 */
public class TopNUniqueSort<E>
{
  @Min(1)
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;
  HashMap<E, MutableInt> hmap = null;
  PriorityQueue<E> q = null;

  /**
   * getter function for qbound
   * @return qbound
   */
  @Min(1)
  public int getQbound()
  {
    return qbound;
  }

  /**
   * setter function for qbound
   * @param i
   */
  public void setQbound(int i)
  {
    qbound = i;
  }


  /**
   * Added default constructor for deserializer
   */
  public TopNUniqueSort()
  {
  }

  /**
   * Constructs and sets values accordingly
   * @param initialCapacity
   * @param bound
   * @param flag
   */
  public TopNUniqueSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(flag));
    qbound = bound;
    hmap = new HashMap<E, MutableInt>();
  }

  /**
   * adds an object
   * @param e
   * @return true if add() succeeds
   */
  public boolean add(E e)
  {
    return offer(e);
  }

  /**
   * Size of the queue
   * @return size of the queue
   */
  public int size()
  {
    return q.size();
  }

  /**
   * Clears the queue
   */
  public void clear()
  {
    q.clear();
  }

  /**
   *
   * @return true if queue is empty
   */
  public boolean isEmpty()
  {
    return q.isEmpty();
  }

  /**
   * Returns topN objects
   * @param n
   * @return Top N in an ArrayList
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public ArrayList getTopN(int n)
  {
    ArrayList list = new ArrayList();
    E v;
    int j = 0;
    while (((v = q.poll()) != null) && j < n) {
      list.add(v);
      j++;
    }
    ArrayList<HashMap<E,Integer>> ret = new ArrayList<HashMap<E,Integer>>(list.size());
    int size = list.size();
    int depth = size;
    if (depth > n) {
      depth = n;
    }
    for (int i = 0; i < depth; i++) {
      E o = (E) list.get(size - i - 1);
      HashMap<E, Integer> val = new HashMap<E, Integer>(1);
      MutableInt ival = hmap.get(o);
      val.put(o, ival.toInteger());
      ret.add(val);
    }
    return ret;
  }

  //
  // If ascending, put the order in reverse
  //
  /**
   * Adds object
   * @param e object to be added
   * @return true if offer() succeeds
   */
  @SuppressWarnings("unchecked")
  public boolean offer(E e)
  {
    MutableInt ival = hmap.get(e);
    if (ival != null) { // already exists, so no insertion
      ival.increment();
      return true;
    }
    if (q.size() < qbound) {
      if (ival == null) {
        hmap.put(e, new MutableInt(1));
      }
      return q.offer(e);
    }

    boolean ret = false;
    boolean insert;
    Comparable<? super E> head = (Comparable<? super E>) q.peek();

    if (ascending) { // means head is the lowest value due to inversion
      insert = head.compareTo(e) < 0; // e > head
    }
    else { // means head is the highest value due to inversion
      insert = head.compareTo(e) > 0; // head is < e
    }

    // object e makes it, someone else gets dropped
    if (insert && q.offer(e)) {
      hmap.put(e, new MutableInt(1));
      ret = true;

      // the dropped object will never make it to back in anymore
      E drop = q.poll();
      hmap.remove(drop);
    }
    return ret;
  }
}
