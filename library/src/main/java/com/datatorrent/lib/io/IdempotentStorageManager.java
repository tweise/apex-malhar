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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.lib.util.FSStorageAgent;

/**
 * This interface should be implemented by an idempotence agent. An idempotence agent allows
 * an operator to always emit the same tuples in every application window. An idempotent agent
 * cannot make any gaurantees about the tuples emitted in the application window which fails.
 *
 * The order of tuples is gauranteed for ordered input sources when input operators are not partitioned.
 *
 * <b>Important:</b> In order for an idempotent agent to function correctly it cannot allow
 * checkpoints to occur within an application window and checkpoints must be aligned with
 * application window boundaries.
 */

public interface IdempotentStorageManager extends StorageAgent
{
  /**
   * Gets the largest window for which there is recovery data.
   */
  boolean isWindowOld(long windowId);

  public static class FSIdempotentStorageManager extends FSStorageAgent implements IdempotentStorageManager
  {
    //largest window for which there is recovery data.
    private long largestRecoveryWindow = Stateless.WINDOW_ID;

    /**
     * This value is null if the recovery state is unknown. Recovery state is undefined (null) until
     * setup and begin window are called. This property is true if the idempotent agent is still
     * recovering tuples.
     */
    private transient Boolean recovering;

    private FSIdempotentStorageManager()
    {
      this(null, null);
    }

    public FSIdempotentStorageManager(String path, Configuration conf)
    {
      super(path, conf);

      try {
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));

        for (FileStatus operatorDirStatus : fileStatuses) {
          for (FileStatus status : fs.listStatus(operatorDirStatus.getPath())) {
            String fileName = status.getPath().getName();
            long windowId = Long.parseLong(fileName, 16);
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
    public boolean isWindowOld(long windowId)
    {
      return windowId <= largestRecoveryWindow;
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
      super.delete(operatorId, windowId);
    }

    /**
     * This is a helper method to handle common cases for different repartitioning methods.
     *
     * @param <ST>              The type of the tuples to serialize.
     * @param idempotenceAgents The idempotence agents to repartition.
     * @param newPartitionSize  The new partition size.
     * @param agentIdsUnion     The union of all the idempotence agents ids.
     * @return The repartitioned idempotence agents.
     */
    private static <ST> Collection<IdempotenceAgent<ST>> repartition(Collection<IdempotenceAgent<ST>> idempotenceAgents,
                                                                     int newPartitionSize,
                                                                     Set<Integer> agentIdsUnion)
    {
      //This is not valid
      if (newPartitionSize <= 0) {
        throw new IllegalArgumentException("The new partition size must be positive.");
      }

      for (IdempotenceAgent<ST> agent : idempotenceAgents) {
        agentIdsUnion.addAll(agent.getRecoverIdempotentAgentIds());
      }

      //Nothing needs to be done
      if (idempotenceAgents.size() == newPartitionSize) {
        for (IdempotenceAgent<ST> agent : idempotenceAgents) {
          agent.setAllAgentIds(agentIdsUnion);
        }

        return idempotenceAgents;
      }

      return null;
    }

    /**
     * This method repartitions the given idempotence agents to the given partition size. Existing idempotent agent
     * data is evenly distributed accross partitions. This should be used when creating a partitioner for an operator.
     *
     * @param <ST>              The type of the tuples recorded by the idempotence agents.
     * @param idempotenceAgents The idempotence agents to repartition.
     * @param newPartitionSize  The new number of idempotence agents.
     * @return A collection containing the repartitioned idempotent agents.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <ST> Collection<IdempotenceAgent<ST>> repartitionEven(Collection<IdempotenceAgent<ST>> idempotenceAgents,
                                                                        int newPartitionSize)
    {
      Set<Integer> agentIdsUnion = Sets.newHashSet();

      Collection<IdempotenceAgent<ST>> tempNewIdempotenceAgents = repartition(idempotenceAgents,
        newPartitionSize,
        agentIdsUnion);

      if (tempNewIdempotenceAgents != null) {
        return tempNewIdempotenceAgents;
      }

      //More partitions than existing idempotence agents
      if (idempotenceAgents.size() < newPartitionSize) {
        //Template idempotence agent
        IdempotenceAgent<ST> templateAgent = idempotenceAgents.iterator().next().clone();
        templateAgent.clearState();

        //Create new collection of idempotent agents
        List<IdempotenceAgent<ST>> newIdempotenceAgents = Lists.newArrayList();
        newIdempotenceAgents.addAll(idempotenceAgents);

        //Add new previously non existing idempotence agents.
        int numberOfNewAgents = newPartitionSize - idempotenceAgents.size();

        for (int newAgentCounter = 0;
             newAgentCounter < numberOfNewAgents;
             newAgentCounter++) {
          newIdempotenceAgents.add(templateAgent.clone());
        }

        for (IdempotenceAgent<ST> agent : newIdempotenceAgents) {
          agent.setAllAgentIds(agentIdsUnion);
        }

        return newIdempotenceAgents;
      }

      //This is the case where the number of new partitions is smaller than the number of
      //idempotence agents
      IdempotenceAgent<ST>[] repartitionedAgents = new IdempotenceAgent[newPartitionSize];
      Set<Integer>[] agentIdSets = new HashSet[newPartitionSize];

      //Existing idempotence agents as a list.
      List<IdempotenceAgent<ST>> agentsList = Lists.newArrayList();
      agentsList.addAll(idempotenceAgents);

      for (int agentCounter = 0;
           agentCounter < agentsList.size();
           agentCounter++) {
        int agentIndex = agentCounter % newPartitionSize;
        IdempotenceAgent<ST> agent = agentsList.get(agentCounter);

        if (repartitionedAgents[agentIndex] == null) {
          repartitionedAgents[agentIndex] = agent;

          Set<Integer> agentIds = Sets.newHashSet();
          agentIds.add(agent.getIdempotentAgentId());

          agentIdSets[agentIndex] = agentIds;
        }
        else {
          agentIdSets[agentIndex].add(agent.getIdempotentAgentId());
        }
      }

      for (int agentCounter = 0;
           agentCounter < newPartitionSize;
           agentCounter++) {
        repartitionedAgents[agentCounter].setRecoverIdempotentAgentIds(agentIdSets[agentCounter]);
        repartitionedAgents[agentCounter].setAllAgentIds(agentIdsUnion);
      }

      return Arrays.asList(repartitionedAgents);
    }

    /**
     * This method repartitions the given idempotence agents to the given partition size. Existing idempotent agent
     * data is shared accross all partitions. This should be used when creating a partitioner for an operator.
     *
     * @param <ST>              The type of the tuples recorded by the idempotence agents.
     * @param idempotenceAgents The idempotence agents to repartition.
     * @param newPartitionSize  The new number of idempotence agents.
     * @return A collection containing the repartitioned idempotent agents.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <ST> Collection<IdempotenceAgent<ST>> repartitionAll(Collection<IdempotenceAgent<ST>> idempotenceAgents,
                                                                       int newPartitionSize)
    {
      Set<Integer> agentIdsUnion = Sets.newHashSet();

      Collection<IdempotenceAgent<ST>> tempNewIdempotenceAgents = repartition(idempotenceAgents,
        newPartitionSize,
        agentIdsUnion);

      if (tempNewIdempotenceAgents != null) {
        return tempNewIdempotenceAgents;
      }

      //More partitions than existing idempotence agents
      if (idempotenceAgents.size() < newPartitionSize) {
        //Template idempotence agent
        IdempotenceAgent<ST> templateAgent = idempotenceAgents.iterator().next().clone();
        templateAgent.clearState();

        //Create new collection of idempotent agents
        List<IdempotenceAgent<ST>> newIdempotenceAgents = Lists.newArrayList();
        newIdempotenceAgents.addAll(idempotenceAgents);

        //Add new previously non existing idempotence agents.
        int numberOfNewAgents = newPartitionSize - idempotenceAgents.size();

        for (int newAgentCounter = 0;
             newAgentCounter < numberOfNewAgents;
             newAgentCounter++) {
          newIdempotenceAgents.add(templateAgent.clone());
        }

        for (IdempotenceAgent<ST> agent : newIdempotenceAgents) {
          agent.setRecoverIdempotentAgentIds(agentIdsUnion);
          agent.setAllAgentIds(agentIdsUnion);
        }

        return newIdempotenceAgents;
      }

      //This is the case where the number of new partitions is smaller than the number of
      //idempotence agents
      IdempotenceAgent<ST>[] repartitionedAgents = new IdempotenceAgent[newPartitionSize];

      //Existing idempotence agents as a list.
      List<IdempotenceAgent<ST>> agentsList = Lists.newArrayList();
      agentsList.addAll(idempotenceAgents);

      for (int agentCounter = 0;
           agentCounter < newPartitionSize;
           agentCounter++) {
        IdempotenceAgent<ST> agent = agentsList.get(agentCounter);
        agent.setRecoverIdempotentAgentIds(agentIdsUnion);
        agent.setAllAgentIds(agentIdsUnion);
        repartitionedAgents[agentCounter] = agent;
      }

      return Arrays.asList(repartitionedAgents);
    }
  }
}
