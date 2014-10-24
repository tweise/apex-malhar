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

import java.io.IOException;
import java.util.*;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator;
import com.datatorrent.lib.util.FSStorageAgent;
import com.datatorrent.stram.util.FSUtil;

/**
 * An idempotent storage manager allows an operator to emit the same tuples in every replayed application window. An idempotent agent
 * cannot make any guarantees about the tuples emitted in the application window which fails.
 *
 * The order of tuples is guaranteed for ordered input sources.
 *
 * <b>Important:</b> In order for an idempotent storage manager to function correctly it cannot allow
 * checkpoints to occur within an application window and checkpoints must be aligned with
 * application window boundaries.
 */

public interface IdempotentStorageManager extends StorageAgent, Component<Context.OperatorContext>
{
  /**
   * Gets the largest window for which there is recovery data.
   */
  boolean isWindowOld(long windowId);

  /**
   * When an operator can partition itself dynamically then there is no guarantee that an input state which was being handled
   * by one instance previously will be handled by the same instance after partitioning. <br/>
   * For eg. An {@link AbstractFSDirectoryInputOperator} instance which reads a File X till offset l (not check-pointed) may no longer be the
   * instance that handles file X after repartitioning as no. of instances may have changed and file X is re-hashed to another instance. <br/>
   * The new instance wouldn't know from what point to read the File X unless it reads the idempotent storage of all the operators for the window
   * being replayed and fix it's state.
   */
  List<Object> load(long windowId) throws IOException;

  /**
   * This informs the idempotent storage manager that operator is partitioned so that it can set properties and distribute state.
   *
   * @param newManagers        all the new idempotent storage managers.
   * @param removedOperatorIds set of operator ids which were removed after partitioning.
   */
  void partitioned(Collection<IdempotentStorageManager> newManagers, Set<Integer> removedOperatorIds);

  public static class FSIdempotentStorageManager implements IdempotentStorageManager
  {

    protected FSStorageAgent storageAgent;

    @NotNull
    protected String recoveryPath;

    /**
     * largest window for which there is recovery data across all physical operator instances.
     */
    protected transient long largestRecoveryWindow;

    /**
     * This is not null only for one physical instance.<br/>
     * It consists of operator ids which have been deleted but have some state that can be replayed.
     * Only one of the instances would be handling (modifying) the files that belong to this state.
     */
    protected Set<Integer> deletedOperators;

    /**
     * Sorted mapping from window id to all the operators that have state to replay for that window.
     */
    protected final transient TreeMultimap<Long, Integer> replayState;

    protected transient FileSystem fs;
    protected transient Path appPath;

    public FSIdempotentStorageManager()
    {
      replayState = TreeMultimap.create();
      deletedOperators = Sets.newHashSet();
      largestRecoveryWindow = Stateless.WINDOW_ID;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      Configuration configuration = new Configuration();
      appPath = new Path(recoveryPath + '/' + context.getValue(DAG.APPLICATION_ID));

      if (storageAgent == null) {
        storageAgent = new FSStorageAgent(appPath.toString(), configuration);
      }
      try {
        fs = FileSystem.newInstance(appPath.toUri(), configuration);
        if (FSUtil.mkdirs(fs, appPath)) {
          fs.setWorkingDirectory(appPath);
        }

        FileStatus[] fileStatuses = fs.listStatus(appPath);

        for (FileStatus operatorDirStatus : fileStatuses) {
          int operatorId = Integer.parseInt(operatorDirStatus.getPath().getName());

          for (FileStatus status : fs.listStatus(operatorDirStatus.getPath())) {
            String fileName = status.getPath().getName();
            long windowId = Long.parseLong(fileName, 16);
            replayState.put(windowId, operatorId);
            if (windowId > largestRecoveryWindow) {
              largestRecoveryWindow = windowId;
            }
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
      storageAgent.save(object, operatorId, windowId);
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      return storageAgent.load(operatorId, windowId);
    }

    @Override
    public List<Object> load(long windowId) throws IOException
    {
      SortedSet<Integer> operators = replayState.get(windowId);
      List<Object> data = Lists.newArrayListWithCapacity(operators.size());
      for (int operatorId : operators) {
        data.add(load(operatorId, windowId));
      }

      return data;
    }

    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      return storageAgent.getWindowIds(operatorId);
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
      if (windowId <= largestRecoveryWindow && deletedOperators != null && !deletedOperators.isEmpty()) {
        Iterator<Long> windowsIterator = replayState.keySet().iterator();
        while (windowsIterator.hasNext()) {
          long lwindow = windowsIterator.next();
          if (lwindow > windowId) {
            break;
          }
          Iterator<Integer> operatorsIterator = replayState.get(lwindow).iterator();
          while (operatorsIterator.hasNext()) {
            int loperator = operatorsIterator.next();

            if (deletedOperators.contains(loperator)) {
              Path operatorDir = new Path(String.valueOf(loperator));
              fs.delete(new Path(operatorDir, Long.toHexString(lwindow)), true);

              if (fs.listStatus(operatorDir).length == 0) {
                //The operator was deleted and it has nothing to replay.
                deletedOperators.remove(loperator);
                fs.delete(operatorDir, true);
              }
              operatorsIterator.remove();
            }
          }
          windowsIterator.remove();
        }
      }
      storageAgent.delete(operatorId, windowId);
    }

    @Override
    public boolean isWindowOld(long windowId)
    {
      return windowId <= largestRecoveryWindow;
    }

    @Override
    public void partitioned(Collection<IdempotentStorageManager> newManagers, Set<Integer> removedOperatorIds)
    {
      Preconditions.checkArgument(newManagers != null && !newManagers.isEmpty(), "there has to be one idempotent storage manager");
      FSIdempotentStorageManager deletedOperatorsManager = null;

      for (IdempotentStorageManager storageManager : newManagers) {

        FSIdempotentStorageManager lmanager = (FSIdempotentStorageManager) storageManager;
        lmanager.recoveryPath = this.recoveryPath;
        if (lmanager.deletedOperators != null) {
          deletedOperatorsManager = lmanager;
        }
      }

      if (removedOperatorIds == null || removedOperatorIds.isEmpty()) {
        //Nothing to do
        return;
      }

      //If some operators were removed then there needs to be a manager which can clean there state when it is not needed.
      if (deletedOperatorsManager == null) {
        //None of the managers were handling deleted operators data.
        deletedOperatorsManager = (FSIdempotentStorageManager) newManagers.iterator().next();
        deletedOperatorsManager.deletedOperators = Sets.newHashSet();
      }

      deletedOperatorsManager.deletedOperators.addAll(removedOperatorIds);
    }

    @Override
    public void teardown()
    {
      try {
        fs.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public String getRecoveryPath()
    {
      return recoveryPath;
    }

    public void setRecoveryPath(String recoveryPath)
    {
      this.recoveryPath = recoveryPath;
    }
  }
}
