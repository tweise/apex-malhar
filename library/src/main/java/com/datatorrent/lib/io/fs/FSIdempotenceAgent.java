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
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.io.IdempotenceAgent;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import javax.validation.constraints.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an idempotence agent which stores tuples in a file system.
 * @param <T>
 */
public class FSIdempotenceAgent<T> extends IdempotenceAgent<T>
{
  private static transient final Logger LOG = LoggerFactory.getLogger(FSIdempotenceAgent.class);
  /**
   * This is the temporary window file extension.
   */
  public static final String TEMPORARY_FILE_EXTENSION = ".tmp";
  /**
   * This is the default recovery directory.
   */
  public static final String DEFAULT_RECOVERY_DIRECTORY = "recovery";
  /**
   * This is a String representing the path to the recovery directory, where
   * tuples emitted by operators are stored.
   */
  @NotNull
  protected String recoveryDirectory = DEFAULT_RECOVERY_DIRECTORY;
  /**
   * The configuration for the recovery file system.
   */
  private transient Configuration configuration;
  /**
   * The recovery file system which stores tuples for replay.
   */
  private transient FileSystem recoveryFS;
  /**
   * The current windowId.
   */
  private transient long currentWindowId;
  /**
   * The number of windows to recover as determined by in the setup method of the idempotent agent.
   */
  private transient int numberOfWindowsToRecover;
  /**
   * The output stream to write out recovery windows.
   */
  private transient FSDataOutputStream output;
  /**
   * This set holds the recovery state for all the idempotent agents which are still replaying tuples.
   */
  private transient Set<IdempotentAgentRecoveryState> replayRecoveryStates = Sets.newHashSet();
  /**
   * This set holds the recovery state for all the idempotent agents replaying tuples this window.
   */
  private transient List<IdempotentAgentRecoveryState> currentWindowRecoveryState = Lists.newLinkedList();
  /**
   * This set holds the recovery state for all the idempotent agents which manage tuples for uncommitted windows.
   */
  private transient Set<IdempotentAgentRecoveryState> uncommittedRecoveryStates = Sets.newHashSet();
  /**
   * The recovery state for this agent.
   */
  private transient IdempotentAgentRecoveryState thisAgentRecoveryState;
 /**
   * The path to this application's recovery directory.
   */
  protected transient Path applicationRecoveryDirectoryPath;
  /**
   * The path to the idempotentAgent's parent operator's recovery directory.
   */
  protected transient Path idempotentAgentRecoveryDirectoryPath;

  public FSIdempotenceAgent()
  {
  }

  /**
   * This sets the recovery directory.
   * @param recoveryDirectory The recover directory. This is the root directory used to store metadata
   * necessary to maintain idempotence.
   */
  public final void setRecoveryDirectory(@NotNull String recoveryDirectory)
  {
    this.recoveryDirectory = recoveryDirectory;
  }

  /**
   * This gets the recovery directory.
   * @return The recovery directory.
   */
  public final String getRecoveryDirectory()
  {
    return recoveryDirectory;
  }

  /**
   * This method returns the configuration to be used for the file system. This method can be
   * overriden if a custom configuration is desired.
   * @return The configuration to be used for the file system.
   */
  protected Configuration getRecoveryFSConfiguration()
  {
    return new Configuration();
  }

  /**
   * This method returns the recovery file system to be used for the idempotent agent. This method
   * can be overriden if the FileSystem creation logic needs to be customized.
   * @param recoveryPath The path pointing to the directory in which recovery file are stored.
   * @param tempConfiguration The configuration to use for the FileSystem.
   * @return The file system on which recovery files are stored.
   */
  protected FileSystem getRecoveryFS(Path recoveryPath,
                                     Configuration tempConfiguration)
  {
    FileSystem tempRecoveryFS = null;

    try {
      tempRecoveryFS = FileSystem.newInstance(recoveryPath.toUri(), tempConfiguration);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    if(tempRecoveryFS instanceof LocalFileSystem) {
      tempRecoveryFS = ((LocalFileSystem) tempRecoveryFS).getRaw();
    }

    return tempRecoveryFS;
  }

  @Override
  public void beginWindowPri(long windowId)
  {
    currentWindowId = windowId;
    LOG.debug("current {} largest {}", windowId, getLargestRecoveryWindow());

    try {
      //Recover windows
      if (windowId <= getLargestRecoveryWindow()) {
        for(IdempotentAgentRecoveryState agentRecovery:
            replayRecoveryStates) {
          Path windowPath = new Path(agentRecovery.idempotentAgentRecoveryPath,
                                     Long.toString(windowId));

          if(!recoveryFS.exists(windowPath)) {
            continue;
          }

          FileStatus status = recoveryFS.getFileStatus(windowPath);
          agentRecovery.currentFileSize = status.getLen();
          agentRecovery.inputStream = null;
          agentRecovery.windowPath = windowPath;

          //If the recovery file is empty, there is nothing to be done
          if (agentRecovery.currentFileSize <= 0) {
            continue;
          }

          currentWindowRecoveryState.add(agentRecovery);
        }

        Collections.sort(currentWindowRecoveryState);
        LOG.debug("currentWindowRecoveryState {}", currentWindowRecoveryState);
      }
      //When not recovering then persist data for recovery
      else {
        Path tmpWindowPath = new Path(idempotentAgentRecoveryDirectoryPath,
                                      Long.toString(currentWindowId) +
                                      TEMPORARY_FILE_EXTENSION);

        LOG.debug("Persisting data for recovery {}", tmpWindowPath);
        output = recoveryFS.create(tmpWindowPath);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext()
  {
    return !currentWindowRecoveryState.isEmpty();
  }

  @Override
  public byte[] nextTupleBytes()
  {
    IdempotentAgentRecoveryState agentRecoveryState = currentWindowRecoveryState.get(0);

    if(agentRecoveryState.inputStream == null) {
      try {
        agentRecoveryState.inputStream = recoveryFS.open(agentRecoveryState.windowPath);
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    try {
      //Get the number of bytes the tuple is comprised of
      int bytesToRead = agentRecoveryState.inputStream.readInt();
      //read in the tuple
      byte[] buffer = new byte[bytesToRead];
      agentRecoveryState.inputStream.read(buffer);

      if(agentRecoveryState.inputStream.getPos() >=
         agentRecoveryState.currentFileSize) {
        agentRecoveryState.inputStream.close();
        currentWindowRecoveryState.remove(0);
        LOG.debug("currentWindowRecoveryState {}", currentWindowRecoveryState);
      }

      return buffer;
    }
    catch (EOFException e) {
      LOG.error("Unexpected end of file, when replaying tuples for idempotency. " +
                "Tuples were lost in window {}", currentWindowId);
      currentWindowRecoveryState.remove(0);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public void write(byte[] bytes)
  {
    LOG.debug("writing");
    try {
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void endWindowPri()
  {
    if(thisAgentRecoveryState.completedWindowsToRecover.isEmpty() ||
       thisAgentRecoveryState.completedWindowsToRecover.get(
       thisAgentRecoveryState.completedWindowsToRecover.size() - 1) < currentWindowId) {
      thisAgentRecoveryState.completedWindowsToRecover.add(currentWindowId);
    }

    try {
      for(IdempotentAgentRecoveryState recoveryState: currentWindowRecoveryState) {
        if(recoveryState.inputStream != null) {
          recoveryState.inputStream.close();
        }
      }

      //Close the file we wrote the tuples out for replay
      if (output != null) {
        output.close();
        output = null;
      }

      if (currentWindowId > getLargestRecoveryWindow()) {
        recoveryFS.rename(new Path(idempotentAgentRecoveryDirectoryPath,
                                   Long.toString(currentWindowId) +
                                   TEMPORARY_FILE_EXTENSION),
                          new Path(idempotentAgentRecoveryDirectoryPath,
                                   Long.toString(currentWindowId)));
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    //root recovery directory
    Path recoveryPath = new Path(recoveryDirectory);
    //application recovery directory
    applicationRecoveryDirectoryPath = new Path(recoveryPath, this.getApplicationId());
    //parent operator's recovery directory
    idempotentAgentRecoveryDirectoryPath = new Path(applicationRecoveryDirectoryPath,
                                                    Integer.toString(this.getIdempotentAgentId()));

    //Create file system
    configuration = getRecoveryFSConfiguration();
    recoveryFS = getRecoveryFS(applicationRecoveryDirectoryPath, configuration);

    numberOfWindowsToRecover = Integer.MIN_VALUE;
    this.setLargestRecoveryWindow(computeLargestRecoveryWindow());

    replayRecoveryStates = Sets.newHashSet();
    Set<Integer> recoverIdempotentAgentIds = this.getRecoverIdempotentAgentIds();

    for(Integer recoverIdempotentAgentId: recoverIdempotentAgentIds) {
      IdempotentAgentRecoveryState agentState =
      setupIdempotentAgentRecoveryStates(recoverIdempotentAgentId);

      if(agentState.idempotentAgentId == getIdempotentAgentId()) {
        thisAgentRecoveryState = agentState;
      }

      replayRecoveryStates.add(agentState);

      if(!agentState.completedWindowsToRecover.isEmpty()) {
        if(numberOfWindowsToRecover <
           agentState.completedWindowsToRecover.size()) {
          numberOfWindowsToRecover = agentState.completedWindowsToRecover.size();
        }
      }
    }

    uncommittedRecoveryStates = Sets.newHashSet();
    uncommittedRecoveryStates.addAll(replayRecoveryStates);
  }

  private IdempotentAgentRecoveryState setupIdempotentAgentRecoveryStates(int idempotentAgentId)
  {
    Path idempotentAgentDirectoryPath = new Path(applicationRecoveryDirectoryPath,
                                             Integer.toString(idempotentAgentId));

    IdempotentAgentRecoveryState state = new IdempotentAgentRecoveryState(idempotentAgentId,
                                                                          idempotentAgentDirectoryPath);
    state.idempotentAgentRecoveryPath = idempotentAgentDirectoryPath;

    try {
      if(!recoveryFS.exists(idempotentAgentDirectoryPath)) {
        return state;
      }

      FileStatus[] fileStatuses = recoveryFS.listStatus(idempotentAgentDirectoryPath);

      for (FileStatus status: fileStatuses) {
          String fileName = status.getPath().getName();

          long windowId;

          try {
            windowId = Long.parseLong(fileName);
          }
          catch(NumberFormatException e) {
            continue;
          }

          LOG.debug("Adding window {} to recovery", windowId);
          state.completedWindowsToRecover.add(windowId);
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    Collections.sort(state.completedWindowsToRecover);
    return state;
  }

  @Override
  public void teardown()
  {
    try {

      //If the output stream is not null then we are not
      //recovering
      if(output != null) {
        output.close();
        output = null;
      }
      //If the output stream is null then we are recovering.
      else {
        //Close the input streams we are using to read tuples
        for(IdempotentAgentRecoveryState agentRecoveryState:
            currentWindowRecoveryState) {
          agentRecoveryState.inputStream.close();
          agentRecoveryState.inputStream = null;
        }
      }

      //Close file system
      recoveryFS.close();
      recoveryFS = null;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    thisAgentRecoveryState = null;
    replayRecoveryStates.clear();
    currentWindowRecoveryState.clear();
    uncommittedRecoveryStates.clear();
    configuration = null;
    applicationRecoveryDirectoryPath = null;
    idempotentAgentRecoveryDirectoryPath = null;
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  //This method is called after all the operators in the dag reach the next checkpoint
  //So we will never replay this window and the windows before it.
  @Override
  public void committed(long windowId)
  {
    LOG.debug("committed {}", windowId);

    //Go through all the recovery states
    Set<IdempotentAgentRecoveryState> completedRecovery = Sets.newHashSet();

    for(IdempotentAgentRecoveryState recoveryState: uncommittedRecoveryStates) {
        Iterator<Long> windowIterator = recoveryState.completedWindowsToRecover.iterator();

      //Go through all the recovered windows and remove the ones which have been comitted.
      while(windowIterator.hasNext()) {
        long recoveredWindow = windowIterator.next();

        //remove committed windows
        if(recoveredWindow <= windowId) {
          Path windowPath = new Path(recoveryState.idempotentAgentRecoveryPath,
                                     Long.toString(recoveredWindow));

          LOG.debug("deleting idempotent agent window {}", windowPath);

          try {
            recoveryFS.delete(windowPath, true);
          }
          catch (IOException ex) {
            throw new RuntimeException(ex);
          }

          windowIterator.remove();
        }
      }

      if(recoveryState.completedWindowsToRecover.isEmpty() &&
         (thisAgentRecoveryState.idempotentAgentId !=
         recoveryState.idempotentAgentId)) {

        completedRecovery.add(recoveryState);
      }
    }

    uncommittedRecoveryStates.removeAll(completedRecovery);
  }

  /**
   * This method returns the number of completed windows to recover as determined by the
   * setup method of the idempotent agent.
   * @return The number of completed windows to recover as determined by the setup method
   * of the idempotent agent.
   */
  protected int getNumberOfWindowsToRecover()
  {
    return numberOfWindowsToRecover;
  }

  /**
   * This is not a true clone. It just copies the necessary state information to create another
   * Idempotent agent with the same settings.
   * @return
   */
  @Override
  public IdempotenceAgent<T> clone()
  {
    FSIdempotenceAgent<T> fsAgent = new FSIdempotenceAgent<T>();
    fsAgent.setRecoveryDirectory(recoveryDirectory);
    fsAgent.setStreamCodec(getStreamCodec());
    return fsAgent;
  }

  @Override
  public long computeLargestRecoveryWindow()
  {
    long largestWindow = Stateless.WINDOW_ID;

    for(Integer recoverIdempotentAgentId: this.getAllAgentIds()) {
      IdempotentAgentRecoveryState agentState =
      setupIdempotentAgentRecoveryStates(recoverIdempotentAgentId);
      replayRecoveryStates.add(agentState);

      if(!agentState.completedWindowsToRecover.isEmpty()) {
        long tempWindowId = agentState.completedWindowsToRecover.get(
                            agentState.completedWindowsToRecover.size() - 1);

        if(largestWindow < tempWindowId) {
           largestWindow = tempWindowId;
        }
      }
    }

    return largestWindow;
  }

  /**
   * This class represents the state required to recover the tuples sent by an operator.
   */
  private static class IdempotentAgentRecoveryState implements Comparable<IdempotentAgentRecoveryState>
  {
    /**
     * The windows to recover
     */
    public List<Long> completedWindowsToRecover = Lists.newArrayList();
    public int idempotentAgentId;
    public transient Path idempotentAgentRecoveryPath;
    public transient Path windowPath;
    public transient FSDataInputStream inputStream;
    public transient long currentFileSize;
    public transient boolean uncleanEnd;

    public IdempotentAgentRecoveryState(int idempotentAgentId,
                                        Path idempotentAgentRecoveryPath)
    {
      this.idempotentAgentId = idempotentAgentId;
      this.idempotentAgentRecoveryPath = idempotentAgentRecoveryPath;
    }

    @Override
    public int compareTo(IdempotentAgentRecoveryState t)
    {
      return idempotentAgentId - t.idempotentAgentId;
    }

    @Override
    public String toString()
    {
      return Integer.toString(idempotentAgentId);
    }
  }
}
