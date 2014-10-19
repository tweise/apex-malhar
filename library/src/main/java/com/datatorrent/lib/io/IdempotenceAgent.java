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

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.*;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class IdempotenceAgent<T> implements CheckpointListener,
                                                     Component<Context.OperatorContext>,
                                                     Cloneable
{
  private static final Logger logger = LoggerFactory.getLogger(IdempotenceAgent.class);

  /**
   * The codec used to serialize tuples to a byte array.
   */
  @NotNull
  private StreamCodec<T> streamCodec;
  /**
   * The parent operator's applicationId as determined by the setup context.
   */
  private String applicationId;
  /**
   * This idempotent agent's id as determined by the setup context.
   */
  private Integer idempotentAgentId;
  /**
   * The id of the current window.
   */
  private long currentWindowId = Stateless.WINDOW_ID;
  /**
   * The largest window for which there is recovery data.
   */
  private long largestRecoveryWindow = Stateless.WINDOW_ID;
  /**
   * The ids of the idempotent agents which manage tuples to be replayed. This
   * parameter should only be set during repartitioning.
   */
  protected Set<Integer> idempotentAgentIds = Sets.newHashSet();
  /**
   * The ids of all idempotent agents assigned to a logical operator. This
   * parameter should only be set during repartitioning. This should be used
   * to find the largetRecoveryWindow in the idempotent agent's setup method.
   */
  protected Set<Integer> allIdempotentAgentIds = Sets.newHashSet();
  /**
   * This value is null if the recovery state is unknown. Recovery state is undefined (null) until
   * setup and begin window are called. This property is true if the idempotent agent is still
   * recovering tuples.
   */
  private transient Boolean recovering;

  public IdempotenceAgent()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    if(this.applicationId == null) {
      this.applicationId = context.getValue(DAG.APPLICATION_ID);
    }

    if(this.idempotentAgentId == null) {
      this.idempotentAgentId = context.getId();
    }

    idempotentAgentIds.add(idempotentAgentId);
    allIdempotentAgentIds.add(idempotentAgentId);
    recovering = null;
  }

  /**
   * This method should be called in the begin window method of the operator.
   * @param windowId The window id.
   */
  public final void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    recovering = windowId <= this.largestRecoveryWindow;

    beginWindowPri(windowId);
  }

  /**
   * Helper method which implements the begin window functionality of the idempotent agent.
   * @param windowId The current window id.
   */
  protected abstract void beginWindowPri(long windowId);

  /**
   * This method should be called in the endWindow method of an operator.
   */
  public final void endWindow()
  {
    recovering = currentWindowId < this.largestRecoveryWindow;
    endWindowPri();
  }

  /**
   * This is a helper method which implements the endwindow functionality.
   */
  protected abstract void endWindowPri();

  /**
   * This method determines if there are any tuples left which must be emitted to maintain idempotency.
   * @return True if there are tuples left which must be emitted to maintain idempotency.
   */
  public abstract boolean hasNext();

  /**
   * This method retrieves the next tuple to emit for recovery.
   * @return The next tuple to emit to maintain idempotency.
   */
  @SuppressWarnings("unchecked")
  public T next()
  {
    byte[] tuple = nextTupleBytes();
    return (T) streamCodec.fromByteArray(new Slice(tuple, 0, tuple.length));
  }

  /**
   * This method retrieves the next tuple to emit for recovery.
   * @return The next tuple to emit for recovery as an array of bytes.
   */
  protected abstract byte[] nextTupleBytes();

  /**
   * This method persists tuples which are serialized as an array of bytes.
   * @param tuple A tuple serialized as an array of bytes.
   */
  public final void write(T tuple)
  {
    write(streamCodec.toByteArray(tuple).buffer);
  }

  /**
   * This method persists tuples which are serialized as an array of bytes.
   * @param tuple A tuple serialized as an array of bytes.
   */
  protected abstract void write(byte[] tuple);

  /**
   * This method sets a serializable stream codec used to serialize tuples for the idempotent agent.
   * @param streamCodec The stream codec used to serialize tuples for the idempotent agent.
   */
  public void setStreamCodec(StreamCodec<T> streamCodec)
  {
    this.streamCodec = streamCodec;
  }

  /**
   * This method gets the stream codec used to serialize tuples for the idempotent agent.
   * @return This retrieves the stream codec used to serialize tuples for the idempotent agent.
   */
  public StreamCodec<T> getStreamCodec()
  {
    return streamCodec;
  }

  /**
   * Gets the largest window for which there is recovery data.
   */
  public long getLargestRecoveryWindow()
  {
    return largestRecoveryWindow;
  }

  /**
   * Sets the largest window for which there is recovery data.
   */
  public void setLargestRecoveryWindow(long largestRecoveryWindow)
  {
    this.largestRecoveryWindow = largestRecoveryWindow;
  }

  /**
   * This method computes the largest recovery window.
   * @return The largest recovery window.
   */
  public abstract long computeLargestRecoveryWindow();

  /**
   * This sets the operatorIs to monitor for recovery.
   * @param idempotentAgentIds The set of operatorIds to monitor for recovery.
   */
  public void setRecoverIdempotentAgentIds(Set<Integer> idempotentAgentIds)
  {
    if(idempotentAgentIds == null) {
      throw new NullPointerException("The recoverOperatorIds cannot be null.");
    }

    this.idempotentAgentIds = Sets.newHashSet();
    this.idempotentAgentIds.addAll(idempotentAgentIds);
  }

  public void setAllAgentIds(Set<Integer> allAgentIds)
  {
    this.allIdempotentAgentIds = allAgentIds;
  }

  public Set<Integer> getAllAgentIds()
  {
    return this.allIdempotentAgentIds;
  }

  /**
   * This gets the operator ids to monitor for recovery.
   * @return The operator ids to monitor for recovery.
   */
  public Set<Integer> getRecoverIdempotentAgentIds()
  {
    if(this.idempotentAgentIds == null) {
      return null;
    }
    return Collections.unmodifiableSet(this.idempotentAgentIds);
  }

  /**
   * This method returns the idempotent agent's id.
   * @return The idempotent agent's id.
   */
  public Integer getIdempotentAgentId()
  {
    return idempotentAgentId;
  }

  /**
   * This method returns the application id.
   * @return The application id.
   */
  public String getApplicationId()
  {
    return applicationId;
  }

  /**
   * This method returns true if this idempotence agent actually stores and replays tuples
   * to maintain idempotency.
   * @return true if this idempotence agent actually stores and replays tuples
   * to maintain idempotency.
   */
  public boolean isActive()
  {
    return true;
  }

  /**
   * This method returns the recovery state of the Idempotent agent.
   * @return The recovery state of the Idempotent agent.
   */
  public Boolean isRecovering()
  {
    return recovering;
  }

  /**
   * This method returns true if this is the last recovery window.
   * @return True if this is the last recovery window.
   */
  public boolean isLastRecoveryWindow()
  {
    return currentWindowId == largestRecoveryWindow;
  }

  /**
   * This method is called to clear the state of the idempotent agent when new
   * idempotent agents are created in the case of dynamic partitioning.
   * <br />
   * <br />
   * <b>Note:</b> This method should not interfere with an idempotence agent's ability
   * to be cloned.
   */
  public void clearState()
  {
    idempotentAgentId = null;
    idempotentAgentIds = Sets.newHashSet();
  }

  /**
   * This method is used to create new idempotence agents when repartitioning.
   * @return A cloned idempotent agent.
   */
  @Override
  public abstract IdempotenceAgent<T> clone();

  /**
   * This is a helper method to handle common cases for different repartitioning methods.
   * @param <ST> The type of the tuples to serialize.
   * @param idempotenceAgents The idempotence agents to repartition.
   * @param newPartitionSize The new partition size.
   * @param agentIdsUnion The union of all the idempotence agents ids.
   * @return The repartitioned idempotence agents.
   */
  private static <ST> Collection<IdempotenceAgent<ST>> repartition(Collection<IdempotenceAgent<ST>> idempotenceAgents,
                                                                   int newPartitionSize,
                                                                   Set<Integer> agentIdsUnion)
  {
    //This is not valid
    if(newPartitionSize <= 0) {
      throw new IllegalArgumentException("The new partition size must be positive.");
    }

    for(IdempotenceAgent<ST> agent: idempotenceAgents) {
      agentIdsUnion.addAll(agent.getRecoverIdempotentAgentIds());
    }

    //Nothing needs to be done
    if(idempotenceAgents.size() == newPartitionSize) {
      for(IdempotenceAgent<ST> agent: idempotenceAgents) {
        agent.setAllAgentIds(agentIdsUnion);
      }

      return idempotenceAgents;
    }

    return null;
  }

  /**
   * This method repartitions the given idempotence agents to the given partition size. Existing idempotent agent
   * data is evenly distributed accross partitions. This should be used when creating a partitioner for an operator.
   * @param <ST> The type of the tuples recorded by the idempotence agents.
   * @param idempotenceAgents The idempotence agents to repartition.
   * @param newPartitionSize The new number of idempotence agents.
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

    if(tempNewIdempotenceAgents != null) {
      return tempNewIdempotenceAgents;
    }

    //More partitions than existing idempotence agents
    if(idempotenceAgents.size() < newPartitionSize) {
      //Template idempotence agent
      IdempotenceAgent<ST> templateAgent = idempotenceAgents.iterator().next().clone();
      templateAgent.clearState();

      //Create new collection of idempotent agents
      List<IdempotenceAgent<ST>> newIdempotenceAgents = Lists.newArrayList();
      newIdempotenceAgents.addAll(idempotenceAgents);

      //Add new previously non existing idempotence agents.
      int numberOfNewAgents = newPartitionSize - idempotenceAgents.size();

      for(int newAgentCounter = 0;
          newAgentCounter < numberOfNewAgents;
          newAgentCounter++) {
        newIdempotenceAgents.add(templateAgent.clone());
      }

      for(IdempotenceAgent<ST> agent: newIdempotenceAgents) {
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

    for(int agentCounter = 0;
        agentCounter < agentsList.size();
        agentCounter++) {
      int agentIndex = agentCounter % newPartitionSize;
      IdempotenceAgent<ST> agent = agentsList.get(agentCounter);

      if(repartitionedAgents[agentIndex] == null) {
        repartitionedAgents[agentIndex] = agent;

        Set<Integer> agentIds = Sets.newHashSet();
        agentIds.add(agent.getIdempotentAgentId());

        agentIdSets[agentIndex] = agentIds;
      }
      else {
        agentIdSets[agentIndex].add(agent.getIdempotentAgentId());
      }
    }

    for(int agentCounter = 0;
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
   * @param <ST> The type of the tuples recorded by the idempotence agents.
   * @param idempotenceAgents The idempotence agents to repartition.
   * @param newPartitionSize The new number of idempotence agents.
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

    if(tempNewIdempotenceAgents != null) {
      return tempNewIdempotenceAgents;
    }

    //More partitions than existing idempotence agents
    if(idempotenceAgents.size() < newPartitionSize) {
      //Template idempotence agent
      IdempotenceAgent<ST> templateAgent = idempotenceAgents.iterator().next().clone();
      templateAgent.clearState();

      //Create new collection of idempotent agents
      List<IdempotenceAgent<ST>> newIdempotenceAgents = Lists.newArrayList();
      newIdempotenceAgents.addAll(idempotenceAgents);

      //Add new previously non existing idempotence agents.
      int numberOfNewAgents = newPartitionSize - idempotenceAgents.size();

      for(int newAgentCounter = 0;
          newAgentCounter < numberOfNewAgents;
          newAgentCounter++) {
        newIdempotenceAgents.add(templateAgent.clone());
      }

      for(IdempotenceAgent<ST> agent: newIdempotenceAgents) {
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

    for(int agentCounter = 0;
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
