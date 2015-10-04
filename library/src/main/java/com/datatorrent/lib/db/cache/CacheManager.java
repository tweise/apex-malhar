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
package com.datatorrent.lib.db.cache;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.datatorrent.lib.db.KeyValueStore;

/**
 * Manages primary and secondary stores.<br/>
 * <p>
 * It firsts checks the primary store for a key. If the primary store doesn't have the key, it queries the backup store and retrieves the value.<br/>
 * If the key was present in the backup store, its value is returned and also saved in the primary store.
 * </p>
 * <p>
 * Typically primary store is faster but has limited size like memory and backup store is slower but unlimited like databases.<br/>
 * Store Manager can also refresh the values of keys at a specified time every day. This time is in format HH:mm:ss Z.<br/>
 * This is not thread-safe.
 * </p>
 *
 * @since 0.9.2
 */
public class CacheManager implements Closeable
{
  @NotNull
  protected Primary primary;
  @NotNull
  protected Backup backup;
  protected String refreshTime;
  private transient Timer refresher;

  public CacheManager()
  {
    this.primary = new CacheStore();
  }

  public void initialize() throws IOException
  {
    primary.connect();
    backup.connect();
    Map<Object, Object> initialEntries = backup.loadInitialData();
    if (initialEntries != null) {
      primary.putAll(initialEntries);
    }

    if (!Strings.isNullOrEmpty(refreshTime)) {

      String[] parts = refreshTime.split("[:\\s]");

      Calendar timeToRefresh = Calendar.getInstance();
      timeToRefresh.set(Calendar.HOUR_OF_DAY, Integer.parseInt(parts[0]));
      if (parts.length >= 2) {
        timeToRefresh.set(Calendar.MINUTE, Integer.parseInt(parts[1]));
      }
      if (parts.length >= 3) {
        timeToRefresh.set(Calendar.SECOND, Integer.parseInt(parts[2]));
      }
      long initialDelay = timeToRefresh.getTimeInMillis() - Calendar.getInstance().getTimeInMillis();

      TimerTask task = new TimerTask()
      {
        @Override
        public void run()
        {
          List<Object> keysToRefresh = Lists.newArrayList(primary.getKeys());
          if (keysToRefresh.size() > 0) {
            List<Object> refreshedValues = backup.getAll(keysToRefresh);
            if (refreshedValues != null) {
              for (int i = 0; i < keysToRefresh.size(); i++) {
                primary.put(keysToRefresh.get(i), refreshedValues.get(i));
              }
            }
          }
        }
      };

      refresher = new Timer();
      if (initialDelay < 0) {
        refresher.schedule(task, 0);
        timeToRefresh.add(Calendar.DAY_OF_MONTH, 1);
        initialDelay = timeToRefresh.getTimeInMillis();
      }
      refresher.scheduleAtFixedRate(task, initialDelay, 86400000);
    }
  }

  @Nullable
  public Object get(@Nonnull Object key)
  {
    Object primaryVal = primary.get(key);
    if (primaryVal != null) {
      return primaryVal;
    }

    Object backupVal = backup.get(key);
    if (backupVal != null) {
      primary.put(key, backupVal);
    }
    return backupVal;
  }

  public void put(@Nonnull Object key, @Nonnull Object value)
  {
    primary.put(key, value);
    backup.put(key, value);
  }

  @Override
  public void close() throws IOException
  {
    refresher.cancel();
    primary.disconnect();
    backup.disconnect();
  }

  public void setPrimary(Primary primary)
  {
    this.primary = primary;
  }

  public Primary getPrimary()
  {
    return primary;
  }

  public void setBackup(Backup backup)
  {
    this.backup = backup;
  }

  public Backup getBackup()
  {
    return backup;
  }

  /**
   * The cache store can be refreshed every day at a specific time. This sets
   * the time. If the time is not set, cache is not refreshed.
   *
   * @param time time at which cache is refreshed everyday. Format is HH:mm:ss Z.
   */
  public void setRefreshTime(String time)
  {
    refreshTime = time;
  }

  public String getRefreshTime()
  {
    return refreshTime;
  }

  /**
   * A primary store should also provide setting the value for a key.
   */
  public static interface Primary extends KeyValueStore
  {

    /**
     * Get all the keys in the store.
     *
     * @return all present keys.
     */
    Set<Object> getKeys();
  }

  /**
   * Backup store is queried when {@link Primary} doesn't contain a key.<br/>
   * It also provides data needed at startup.<br/>
   */
  public static interface Backup extends KeyValueStore
  {
    /**
     * <br>Backup stores are also used to initialize primary stores. This fetches initialization data.</br>
     *
     * @return map of key/value to initialize {@link CacheManager}
     */
    Map<Object, Object> loadInitialData();
  }

  @SuppressWarnings("unused")
  private final static Logger LOG = LoggerFactory.getLogger(CacheManager.class);

}
