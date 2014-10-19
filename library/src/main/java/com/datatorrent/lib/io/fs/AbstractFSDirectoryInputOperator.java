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

import com.datatorrent.api.*;
import com.datatorrent.api.Context.CountersAggregator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.IdempotenceAgent;
import com.datatorrent.lib.io.NoIdempotenceAgent;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
<<<<<<< HEAD
 * Input operator that reads files from a directory.
 * <br/>
 * <br/>
 * Derived class defines how to read entries from the input stream and emit to the port.
 * <br/>
 * <br/>
 * The directory scanning logic is pluggable to support custom directory layouts and naming schemes. The default
 * implementation scans a single directory.
 * <br/>
 * <br/>
 * Fault tolerant by tracking previously read files and current offset as part of checkpoint state. In case of failure
 * the operator will skip files that were already processed and fast forward to the offset of the current file.
 * <br/>
 * <br/>
 * Supports partitioning and dynamic changes to number of partitions through property {@link #partitionCount}. The
 * directory scanner is responsible to only accept the files that belong to a partition.
 * <br/>
 * <br/>
 * This class supports retrying of failed files by putting them into failed list, and retrying them after pending
 * files are processed. Retrying is disabled when maxRetryCount is set to zero.
 * <br/>
 * <br/>
 * This operator can provide the idempotent gaurantee if the idempotence property is set to true. <b>Note:</b> the
 * idempotence gaurantee will only be satisfied if the following conditions are met:<br /><br />
 *
 *    1. Files are not modified while they are in the directory being scanned. Idempotency
 * will only be gauranteed if files are only added to the directory.
 *    2. Checkpoints will never occur within an application window, and will only occur on a
 * window boundary.
=======
 * This is the base implementation of a directory input operator, which scans a directory for files.&nbsp;
 * Files are then read and split into tuples, which are emitted.&nbsp;
 * Subclasses should implement the methods required to read and emit tuples from files.
 * <p>
 * Derived class defines how to read entries from the input stream and emit to the port.
 * </p>
 * <p>
 * The directory scanning logic is pluggable to support custom directory layouts and naming schemes. The default
 * implementation scans a single directory.
 * </p>
 * <p>
 * Fault tolerant by tracking previously read files and current offset as part of checkpoint state. In case of failure
 * the operator will skip files that were already processed and fast forward to the offset of the current file.
 * </p>
 * <p>
 * Supports partitioning and dynamic changes to number of partitions through property {@link #partitionCount}. The
 * directory scanner is responsible to only accept the files that belong to a partition.
 * </p>
 * <p>
 * This class supports retrying of failed files by putting them into failed list, and retrying them after pending
 * files are processed. Retrying is disabled when maxRetryCount is set to zero.
 * </p>
 * @displayName FS Directory Scan Input
 * @category Input
 * @tags fs, file, input operator
>>>>>>> 9713ff9b0aff91e7fea4a99ac06e30317d7e5e46
 *
 * @param <T> The type of the object that this input operator reads.
 * @since 1.0.2
 */
public abstract class AbstractFSDirectoryInputOperator<T> implements InputOperator,
                                                                     Partitioner<AbstractFSDirectoryInputOperator<T>>,
                                                                     StatsListener,
                                                                     CheckpointListener
{
  private transient static final Logger LOG = LoggerFactory.getLogger(AbstractFSDirectoryInputOperator.class);

  //Operator State
  protected transient long lastScanMillis;
  protected int currentPartitions = 1 ;

  private transient OperatorContext context;
  private int maxRetryCount = 5;
  protected int partitionCount = 1;
  protected int emitBatchSize = 1000;
  protected int scanIntervalMillis = 5000;
  @NotNull
  protected String directory;
  @NotNull
  protected DirectoryScanner scanner = new DirectoryScanner();
  protected Set<String> processedFiles = new HashSet<String>();
  protected long lastRepartition = 0;
  protected List<String> pendingFiles = new LinkedList<String>();
  /* List of unfinished files */
  protected LinkedList<FailedFile> unfinishedFiles = new LinkedList<FailedFile>();
  /* List of failed file */
  protected LinkedList<FailedFile> failedFiles = new LinkedList<FailedFile>();
  protected transient FileSystem fs;
  protected transient Configuration configuration;

  //File State
  @NotNull
  protected long offset;
  protected String currentFile;
  private int retryCount = 0;
  protected transient long skipCount = 0;
  protected transient Path filePath;
  protected transient InputStream inputStream;

  //Idempotence
  private transient List<IdempotenceRecoveryData> windowReplay = Lists.newArrayList();
  private transient IdempotenceRecoveryData currentWindowRecovery = new IdempotenceRecoveryData();
  private IdempotenceAgent<IdempotenceRecoveryData> idempotenceAgent = new NoIdempotenceAgent<IdempotenceRecoveryData>();

  //Counters
  private BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);
  protected MutableLong globalNumberOfFailures = new MutableLong();
  protected MutableLong localNumberOfFailures = new MutableLong();
  protected MutableLong globalNumberOfRetries = new MutableLong();
  protected MutableLong localNumberOfRetries = new MutableLong();
  private transient MutableLong globalProcessedFileCount = new MutableLong();
  private transient MutableLong localProcessedFileCount = new MutableLong();
  private transient MutableLong pendingFileCount = new MutableLong();

  /**
   * Returns the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @return The maximum number of times the operator will attempt to process a
   * file on which it encounters an error.
   */
  public int getMaxRetryCount()
  {
    return maxRetryCount;
  }

  /**
   * Sets the maximum number of times the operator will attempt to process
   * a file on which it encounters an error.
   * @param maxRetryCount The maximum number of times the operator will attempt
   * to process a file on which it encounters an error.
   */
  public void setMaxRetryCount(int maxRetryCount)
  {
    this.maxRetryCount = maxRetryCount;
  }

  /**
   * Gets the directory that is being scanned.
   * @return A string representing the path of the directory being scanned.
   */
  public String getDirectory()
  {
    return directory;
  }

  /**
   * Sets the directory to be scanned.
   * @param directory The path of the directory to be scanned.
   */
  public void setDirectory(String directory)
  {
    this.directory = directory;
  }

  /**
   * Gets the scanner
   * @return
   */
  public DirectoryScanner getScanner()
  {
    return scanner;
  }

  public void setScanner(DirectoryScanner scanner)
  {
    this.scanner = scanner;
  }

  /**
   * Returns the frequency with which new files are scanned for in milliseconds.
   * @return The scan interval in milliseconds.
   */
  public int getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets the frequency with which new files are scanned for in milliseconds.
   * @param scanIntervalMillis The scan interval in milliseconds.
   */
  public void setScanIntervalMillis(int scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Returns the number of tuples emitted in a batch. If the operator is
   * idempotent then this is the number of tuples emitted in a window.
   * @return The number of tuples emitted in a batch.
   */
  public int getEmitBatchSize()
  {
    return emitBatchSize;
  }

  /**
   * Sets the number of tuples to emit in a batch. If the operator is
   * idempotent then this is the number of tuples emitted in a window.
   * @param emitBatchSize The number of tuples to emit in a batch.
   */
  public void setEmitBatchSize(int emitBatchSize)
  {
    this.emitBatchSize = emitBatchSize;
  }

  /**
   * Returns the desired number of partitions.
   * @return the desired number of partitions.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Sets the desired number of partitions.
   * @param requiredPartitions The desired number of partitions.
   */
  public void setPartitionCount(int requiredPartitions)
  {
    this.partitionCount = requiredPartitions;
  }

  /**
   * Returns the current number of partitions for the operator.
   * @return The current number of partitions for the operator.
   */
  public int getCurrentPartitions()
  {
    return currentPartitions;
  }

  public void setIdempotenceAgent(IdempotenceAgent<IdempotenceRecoveryData> idempotenceAgent)
  {
    if(idempotenceAgent == null) {
      throw new IllegalArgumentException("The idempotence agent cannot be null");
    }

    this.idempotenceAgent = idempotenceAgent;
  }

  public IdempotenceAgent<IdempotenceRecoveryData> getIdempotenceAgent()
  {
    return idempotenceAgent;
  }

  @Override
  public void setup(OperatorContext context)
  {
    //Setup the idempotence agent
    idempotenceAgent.setup(context);

    globalProcessedFileCount.setValue((long) processedFiles.size());
    LOG.debug("Setup processed file count: {}", globalProcessedFileCount);
    this.context = context;

    try {
      filePath = new Path(directory);
      configuration = new Configuration();
      fs = FileSystem.newInstance(filePath.toUri(), configuration);
    }
    catch (IOException ex) {
      failureHandling(ex);
    }

    //Make sure our list of directories is up to date
    if(idempotenceAgent.isActive()) {
      scanDirectory();
    }

    //Prepare counters
    fileCounters.setCounter(FileCounters.GLOBAL_PROCESSED_FILES,
                            globalProcessedFileCount);
    fileCounters.setCounter(FileCounters.LOCAL_PROCESSED_FILES,
                            localProcessedFileCount);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES,
                            globalNumberOfFailures);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES,
                            localNumberOfFailures);
    fileCounters.setCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES,
                            globalNumberOfRetries);
    fileCounters.setCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES,
                            localNumberOfRetries);
    fileCounters.setCounter(FileCounters.PENDING_FILES,
                            pendingFileCount);
  }


  @Override
  public void beginWindow(long windowId)
  {
    this.idempotenceAgent.beginWindow(windowId);

    if(inputStream != null) {
      newRecoveryData();
    }
  }

  @Override
  public void emitTuples()
  {
    if(idempotenceAgent.isActive() &&
       idempotenceAgent.isRecovering()) {
      idempotentEmitTuples();
    }
    else {
      nonIdempotentEmitTuples();
    }
  }

  /**
   * This is a helper method for emitTuples which emits tuples in the case that
   * idempotent recovery is taking place.
   */
  private void idempotentEmitTuples()
  {
    LOG.debug("idempotent emit tuples");
    while(idempotenceAgent.hasNext()) {
      IdempotenceRecoveryData idempotenceRecoveryData =
      idempotenceAgent.next();
      LOG.debug("replaying recovery data file {} start {} end {}",
                idempotenceRecoveryData.filePath,
                idempotenceRecoveryData.startOffset,
                idempotenceRecoveryData.endOffset);

      if(scanner.acceptFile(idempotenceRecoveryData.filePath)) {
        replayTuples(idempotenceRecoveryData);
        fixState(idempotenceRecoveryData);
      }
    }
  }

  /**
   * This is a helper method for idempotentEmitTuples which replays tuples for
   * the given idempotenceRecoveryData.
   * @param idempotenceRecoveryData Data which we want to replay.
   */
  private void replayTuples(IdempotenceRecoveryData idempotenceRecoveryData)
  {
    Path path = new Path(idempotenceRecoveryData.filePath);
    boolean justOpened = false;

    if(inputStream == null) {
      try {
        inputStream = openFile(path);
        justOpened = true;
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    else if(!path.toString().equals(this.currentFile)) {
      try {
        closeFile(inputStream);
        inputStream = openFile(path);
        justOpened = true;
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    try {
      if(justOpened) {
        LOG.debug("Reading until start offset file {}, start {}",
                  idempotenceRecoveryData.filePath,
                  idempotenceRecoveryData.startOffset);
        for(long tupleCounter = 0;
                 tupleCounter < idempotenceRecoveryData.startOffset;
                 tupleCounter++) {
          readEntity();
        }
      }

      for(long tupleCounter = idempotenceRecoveryData.startOffset;
          tupleCounter < idempotenceRecoveryData.endOffset;
          tupleCounter++) {
        T entity = readEntity();
        if(entity == null) {
          idempotenceRecoveryData.eof = true;
          break;
        }
        emit(entity);
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * This is a helper method which helps us fix the state of the operator after
   * tuples are replayed.
   * @param idempotenceRecoveryData File for which we want to fix state.
   */
  private void fixState(IdempotenceRecoveryData idempotenceRecoveryData)
  {
    FailedFile ff = new FailedFile(idempotenceRecoveryData.filePath,
                                   idempotenceRecoveryData.endOffset);

    LOG.debug("Fixing state file {}, endOffset {}, eof {}",
              idempotenceRecoveryData.filePath,
              idempotenceRecoveryData.endOffset,
              idempotenceRecoveryData.eof);

    if(pendingFiles.remove(idempotenceRecoveryData.filePath) &&
       !idempotenceRecoveryData.eof) {
      LOG.debug("Removed from pendingFiles adding to unfinishedFiles");
      unfinishedFiles.add(0, ff);
    }
    else if(FailedFile.ffsRemove(idempotenceRecoveryData.filePath,
                                 this.failedFiles) &&
            !idempotenceRecoveryData.eof) {
      LOG.debug("Removed from failed files adding to unfinishedFiles");
      unfinishedFiles.add(0, ff);
    }
    else if(FailedFile.ffsRemove(idempotenceRecoveryData.filePath,
                                 this.unfinishedFiles) &&
            !idempotenceRecoveryData.eof) {
      LOG.debug("Removed from unfinishedFiles adding to unfinishedFiles");
      failedFiles.add(0, ff);
    }
  }

  private void nonIdempotentEmitTuples()
  {
    if (inputStream == null) {
      try {
        if(currentFile != null) {
          LOG.debug("Reloading existing state {}", currentFile);
          this.inputStream = openFile(new Path(currentFile));
          this.skipCount = offset;
          newRecoveryData();
        }
        //Check unfinished files queue for next file
        else if (!unfinishedFiles.isEmpty()) {
          retryFailedFile(unfinishedFiles.poll());
          //Create a new piece of recovery data. for the data read from
          //this file
          newRecoveryData();
        }
        //Check pending files queue for next file
        else if (!pendingFiles.isEmpty()) {
          String newPathString = pendingFiles.get(0);
          pendingFiles.remove(0);
          this.inputStream = openFile(new Path(newPathString));
          //Create a new piece of recovery data. for the data read from
          //this file
          newRecoveryData();
        }
        //Check failed files queue for next file
        else if (!failedFiles.isEmpty()) {
          retryFailedFile(failedFiles.poll());
          //Create a new piece of recovery data. for the data read from
          //this file
          newRecoveryData();
        }
        else {
          scanDirectory();
        }
      }
      catch (IOException ex) {
        failureHandling(ex);
      }
    }
    else {
      LOG.debug("inputstream not null.");
    }

    if (inputStream != null) {
      try {
        int counterForTuple = 0;
        while (counterForTuple++ < emitBatchSize) {
          T line = readEntity();
          if (line == null) {
            currentWindowRecovery.eof = true;
            LOG.info("done reading file ({} entries).", offset);
            closeFile(inputStream);
            break;
          }

          // If skipCount is non zero, then failed file recovery is going on, skipCount is
          // used to prevent already emitted records from being emitted again during recovery.
          // When failed file is open, skipCount is set to the last read offset for that file.
          if (skipCount == 0) {
            offset++;
            emit(line);
            currentWindowRecovery.endOffset = offset;
          }
          else {
            skipCount--;
          }
        }
      }
      catch (IOException e) {
        failureHandling(e);
      }
    }
  }

  @Override
  public void endWindow()
  {
    if(context != null) {
      pendingFileCount.setValue(pendingFiles.size() +
                                     failedFiles.size() +
                                     unfinishedFiles.size());

      if(currentFile != null) {
        pendingFileCount.increment();
      }

      context.setCounters(fileCounters);
    }

    for(IdempotenceRecoveryData idempotencyRecoveryData:
        windowReplay) {

      if(idempotencyRecoveryData.isEmpty()) {
        continue;
      }
      LOG.debug("recover data file {} start {} end {}",
                idempotencyRecoveryData.filePath,
                idempotencyRecoveryData.startOffset,
                idempotencyRecoveryData.endOffset);
      idempotenceAgent.write(idempotencyRecoveryData);
    }

    idempotenceAgent.endWindow();

    if(idempotenceAgent.isActive() &&
       idempotenceAgent.isLastRecoveryWindow()) {
      LOG.debug("Cleaning up recovery state {}.", this.currentFile);
      try {
        if(inputStream != null) {
          LOG.debug("Closing recovery state.");
          closeFile(this.inputStream);
        }
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    windowReplay.clear();
  }

  @Override
  public void teardown()
  {
    IOException savedException = null;
    boolean fileFailed = false;

    try {
      if(inputStream != null) {
        closeFile(inputStream);
      }
    }
    catch (IOException ex) {
      savedException = ex;
      fileFailed = true;
    }

    boolean fsFailed = false;

    try {
      fs.close();
      fs = null;
    }
    catch (IOException ex) {
      savedException = ex;
      fsFailed = true;
    }

    if(savedException != null) {
      String errorMessage = "";

      if(fileFailed) {
        errorMessage += "Failed to close " + currentFile + ". ";
      }

      if(fsFailed) {
        errorMessage += "Failed to close filesystem.";
      }

      throw new RuntimeException(errorMessage, savedException);
    }

    scanner.regex = null;
    filePath = null;
    context = null;
    configuration = null;
    idempotenceAgent.teardown();
  }

  /**
   * This method create a new piece of recovery data and adds it to the replay list for this
   * window.
   */
  private void newRecoveryData()
  {
    if(!idempotenceAgent.isActive()) {
      return;
    }

    currentWindowRecovery = new IdempotenceRecoveryData();
    currentWindowRecovery.startOffset = offset;
    currentWindowRecovery.filePath = currentFile;

    windowReplay.add(currentWindowRecovery);
  }

  /**
   * Scans the directory for new files.
   */
  protected void scanDirectory()
  {
    if(System.currentTimeMillis() - scanIntervalMillis >= lastScanMillis) {
      List<Path> newPaths;

      //Use the non chronological scanner method
      if(!scanner.getChronological()) {
        newPaths = Lists.newArrayList();
        newPaths.addAll(scanner.scan(fs, filePath, processedFiles));
      }
      //Use the chronological scanner method
      else {
        newPaths = scanner.scanChronological(fs, filePath, processedFiles);
      }

      for(Path newPath : newPaths) {
        String newPathString = newPath.toString();
        pendingFiles.add(newPathString);
        processedFiles.add(newPathString);
        localProcessedFileCount.increment();
      }

      lastScanMillis = System.currentTimeMillis();
    }
  }

  /**
   * Helper method for handling IOExceptions.
   * @param e The caught IOException.
   */
  private void failureHandling(Exception e)
  {
    localNumberOfFailures.increment();
    if(maxRetryCount <= 0) {
      throw new RuntimeException(e);
    }
    LOG.error("FS reader error", e);
    addToFailedList();
  }

  protected void addToFailedList() {

    FailedFile ff = new FailedFile(currentFile, offset, retryCount);

    try {
      // try to close file
      if (this.inputStream != null)
        this.inputStream.close();
    } catch(IOException e) {
      localNumberOfFailures.increment();
      LOG.error("Could not close input stream on: " + currentFile);
    }

    ff.retryCount ++;
    ff.lastFailedTime = System.currentTimeMillis();
    ff.offset = this.offset;

    // Clear current file state.
    this.currentFile = null;
    this.inputStream = null;
    this.offset = 0;

    if (ff.retryCount > maxRetryCount)
      return;

    localNumberOfRetries.increment();
    LOG.info("adding to failed list path {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    failedFiles.add(ff);
  }

  protected InputStream retryFailedFile(FailedFile ff)  throws IOException
  {
    LOG.info("retrying failed file {} offset {} retry {}", ff.path, ff.offset, ff.retryCount);
    String path = ff.path;
    this.inputStream = openFile(new Path(path));
    this.offset = ff.offset;
    this.retryCount = ff.retryCount;
    this.skipCount = ff.offset;
    return this.inputStream;
  }

  protected InputStream openFile(Path path) throws IOException
  {
    LOG.info("opening file {}", path);
    FSDataInputStream input = fs.open(path);
    currentFile = path.toString();
    offset = 0;
    retryCount = 0;
    skipCount = 0;
    return input;
  }

  protected void closeFile(InputStream is) throws IOException
  {
    LOG.info("closing file {} offset {}", currentFile, offset);

    if (is != null)
      is.close();

    currentFile = null;
    inputStream = null;
  }

  protected int computedNewPartitionCount(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    boolean isInitialParitition = partitions.iterator().next().getStats() == null;

    if (isInitialParitition && partitionCount == 1) {
      partitionCount = currentPartitions = partitions.size() + incrementalCapacity;
    } else {
      incrementalCapacity = partitionCount - currentPartitions;
    }

    int totalCount = partitions.size() + incrementalCapacity;
    LOG.info("definePartitions trying to create {} partitions, current {}  required {}", totalCount, partitionCount, currentPartitions);
    return totalCount;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFSDirectoryInputOperator<T>>> partitions)
  {
    currentPartitions = partitions.size();
  }

  /**
   * Read the next item from the stream. Depending on the type of stream, this could be a byte array, line, or object.
   * This method should be stateless, meaning no information other than the input stream and it's current position should
   * be required to read an entity. Upon return of null, the stream will be considered fully consumed.
   * Multiple calls to a fully consumed stream should continue to return null.
   * @throws IOException
   * @return Depending on the type of stream an object is returned. When null is returned the stream is consumed.
   */
  abstract protected T readEntity() throws IOException;

  /**
   * Emit the tuple on your output port(s).
   * @param tuple
   */
  abstract protected void emit(T tuple);

  @Override
  public Collection<Partition<AbstractFSDirectoryInputOperator<T>>> definePartitions(Collection<Partition<AbstractFSDirectoryInputOperator<T>>> partitions, int incrementalCapacity)
  {
    lastRepartition = System.currentTimeMillis();

    int totalCount = computedNewPartitionCount(partitions, incrementalCapacity);

    LOG.debug("Computed new partitions: {}", totalCount);

    List<IdempotenceAgent<IdempotenceRecoveryData>> agents = Lists.newArrayList();

    for(Partition<AbstractFSDirectoryInputOperator<T>> partition: partitions) {
      agents.add(partition.getPartitionedInstance().getIdempotenceAgent());
    }

    List<IdempotenceAgent<IdempotenceRecoveryData>> newAgents = Lists.newArrayList();
    newAgents.addAll(IdempotenceAgent.repartitionAll(agents,
                                                     totalCount));

    if(totalCount == partitions.size()) {

      int agentCounter = 0;
      for(Partition<AbstractFSDirectoryInputOperator<T>> partition: partitions) {
        partition.getPartitionedInstance().setIdempotenceAgent(newAgents.get(agentCounter));
        agentCounter++;
      }

      return partitions;
    }

    AbstractFSDirectoryInputOperator<T> tempOperator = partitions.iterator().next().getPartitionedInstance();

    MutableLong tempGlobalNumberOfRetries = tempOperator.globalNumberOfRetries;
    MutableLong tempGlobalNumberOfFailures = tempOperator.globalNumberOfRetries;

    // Build collective state from all instances of the operator.
    Set<String> totalProcessedFiles = Sets.newHashSet();
    Set<FailedFile> currentFiles = Sets.newHashSet();
    List<DirectoryScanner> oldscanners = Lists.newLinkedList();
    List<FailedFile> totalFailedFiles = Lists.newLinkedList();
    List<String> totalPendingFiles = Lists.newLinkedList();
    for(Partition<AbstractFSDirectoryInputOperator<T>> partition : partitions) {
      AbstractFSDirectoryInputOperator<T> oper = partition.getPartitionedInstance();
      totalProcessedFiles.addAll(oper.processedFiles);
      totalFailedFiles.addAll(oper.failedFiles);
      totalPendingFiles.addAll(oper.pendingFiles);
      currentFiles.addAll(unfinishedFiles);
      tempGlobalNumberOfRetries.add(oper.localNumberOfRetries);
      tempGlobalNumberOfFailures.add(oper.localNumberOfFailures);
      if (oper.currentFile != null)
        currentFiles.add(new FailedFile(oper.currentFile, oper.offset));
      oldscanners.add(oper.getScanner());
    }

    LOG.debug("Scanner class {}", scanner.getClass());

    // Create partitions of scanners, scanner's partition method will do state
    // transfer for DirectoryScanner objects.
    List<DirectoryScanner> scanners = scanner.partition(totalCount, oldscanners);

    Kryo kryo = new Kryo();
    Collection<Partition<AbstractFSDirectoryInputOperator<T>>> newPartitions = Lists.newArrayListWithExpectedSize(totalCount);
    for (int i = 0; i < totalCount; i++) {
      AbstractFSDirectoryInputOperator<T> oper = kryo.copy(this);
      DirectoryScanner scn = scanners.get(i);
      oper.setScanner(scn);

      // Do state transfer for processed files.
      oper.processedFiles.addAll(totalProcessedFiles);
      oper.globalNumberOfFailures = tempGlobalNumberOfRetries;
      oper.localNumberOfFailures.setValue(0);
      oper.globalNumberOfRetries = tempGlobalNumberOfFailures;
      oper.localNumberOfRetries.setValue(0);

      // redistribute unfinished files properly
      oper.unfinishedFiles.clear();
      oper.currentFile = null;
      oper.offset = 0;
      Iterator<FailedFile> unfinishedIter = currentFiles.iterator();
      while(unfinishedIter.hasNext()) {
        FailedFile unfinishedFile = unfinishedIter.next();
        if (scn.acceptFile(unfinishedFile.path)) {
          oper.unfinishedFiles.add(unfinishedFile);
          unfinishedIter.remove();
        }
      }

      // transfer failed files
      oper.failedFiles.clear();
      Iterator<FailedFile> iter = totalFailedFiles.iterator();
      while (iter.hasNext()) {
        FailedFile ff = iter.next();
        if (scn.acceptFile(ff.path)) {
          oper.failedFiles.add(ff);
          iter.remove();
        }
      }

      // redistribute pending files properly
      oper.pendingFiles.clear();
      Iterator<String> pendingFilesIterator = totalPendingFiles.iterator();
      while(pendingFilesIterator.hasNext()) {
        String pathString = pendingFilesIterator.next();
        if(scn.acceptFile(pathString)) {
          oper.pendingFiles.add(pathString);
          pendingFilesIterator.remove();
        }
      }

      oper.setIdempotenceAgent(newAgents.get(i));
      newPartitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<T>>(oper));
    }

    LOG.info("definePartitions called returning {} partitions", newPartitions.size());
    return newPartitions;
  }

  /**
   * Repartition is required when number of partitions are not equal to required
   * partitions.
   * @param batchedOperatorStats the stats to use when repartitioning.
   * @return Returns the stats listener response.
   */
  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    Response res = new Response();
    res.repartitionRequired = false;
    if (currentPartitions != partitionCount) {
      LOG.info("processStats: trying repartition of input operator current {} required {}", currentPartitions, partitionCount);
      res.repartitionRequired = true;
    }
    return res;
  }

  @Override
  public void committed(long windowId)
  {
    LOG.debug("committed {}", windowId);
    idempotenceAgent.committed(windowId);
  }

  @Override
  public void checkpointed(long windowId)
  {
    //Do nothing
  }

  /**
   * Class representing failed file, When read fails on a file in middle, then the file is
   * added to failedList along with last read offset.
   * The files from failedList will be processed after all pendingFiles are processed, but
   * before checking for new files.
   * failed file is retried for maxRetryCount number of times, after that the file is
   * ignored.
   */
  protected static class FailedFile {
    String path;
    long   offset;
    int    retryCount;
    long   lastFailedTime;

    /* For kryo serialization */
    protected FailedFile() {}

    protected FailedFile(String path, long offset) {
      this.path = path;
      this.offset = offset;
      this.retryCount = 0;
    }

    protected FailedFile(String path, long offset, int retryCount) {
      this.path = path;
      this.offset = offset;
      this.retryCount = retryCount;
    }

    @Override
    public String toString()
    {
      return "FailedFile[" +
          "path='" + path + '\'' +
          ", offset=" + offset +
          ", retryCount=" + retryCount +
          ", lastFailedTime=" + lastFailedTime +
          ']';
    }

    /**
      * This is a helper method to check if given list of failed files contains a reference to the
      * given file.
      * @param filePath The path of the file whose existence we want to check in the list.
      * @param ffs The list of failed files to check.
      * @return True if this FailedFile list contains the given file.
      */
    public static boolean ffsContains(String filePath,
                                      List<FailedFile> ffs)
    {
      for(FailedFile ff: ffs) {
        if(ff.path.equals(filePath)) {
          return true;
        }
      }

      return false;
    }

    /**
     * This is a helper method to remove the given file path from the collection of failed files.
     * @param filePath The path of the file whose existence we want to check in the list.
     * @param ffs The list of failed files to check.
     * @return True if the filePath was successfully removed.
     */
    public static boolean ffsRemove(String filePath,
                                    List<FailedFile> ffs)
    {
      for(int counter = 0;
          counter < ffs.size();
          counter++) {
        FailedFile ff = ffs.get(counter);

        if(ff.path.equals(filePath)) {
          ffs.remove(counter);
          return true;
        }
      }

      return false;
    }
  }

  /**
   * Enums for aggregated counters about file processing.
   * <p/>
   * Contains the enums representing number of files processed, number of
   * pending files, number of file errors, and number of retries.
   * <p/>
   * @since 1.0.4
   */
  public static enum AggregatedFileCounters
  {
    /**
     * The number of files processed by the logical operator up until this.
     * point in time
     */
    PROCESSED_FILES,
    /**
     * The number of files waiting to be processed by the logical operator.
     */
    PENDING_FILES,
    /**
     * The number of IO errors encountered by the logical operator.
     */
    NUMBER_OF_ERRORS,
    /**
     * The number of times the logical operator tried to resume reading a file
     * on which it encountered an error.
     */
    NUMBER_OF_RETRIES
  }

  /**
   * The enums used to track statistics about the
   * AbstractFSDirectoryInputOperator.
   */
  protected static enum FileCounters
  {
    /**
     * The number of files that were in the processed list up to the last
     * repartition of the operator.
     */
    GLOBAL_PROCESSED_FILES,
    /**
     * The number of files added to the processed list by the physical operator
     * since the last repartition.
     */
    LOCAL_PROCESSED_FILES,
    /**
     * The number of io errors encountered up to the last repartition of the
     * operator.
     */
    GLOBAL_NUMBER_OF_FAILURES,
    /**
     * The number of failures encountered by the physical operator since the
     * last repartition.
     */
    LOCAL_NUMBER_OF_FAILURES,
    /**
     * The number of retries encountered by the physical operator up to the last
     * repartition.
     */
    GLOBAL_NUMBER_OF_RETRIES,
    /**
     * The number of retries encountered by the physical operator since the last
     * repartition.
     */
    LOCAL_NUMBER_OF_RETRIES,
    /**
     * The number of files pending on the physical operator.
     */
    PENDING_FILES
  }

  /**
   * A counter aggregator for AbstractFSDirectoryInputOperator.
   * <p/>
   * In order for this CountersAggregator to be used on your operator, you must
   * set it within your application like this.
   * <p/>
   * <code>
   * dag.getOperatorMeta("fsinputoperator").getAttributes().put(OperatorContext.COUNTERS_AGGREGATOR,
   *                                                            new AbstractFSDirectoryInputOperator.FileCountersAggregator());
   * </code>
   * <p/>
   * The value of the aggregated counter can be retrieved by issuing a get
   * request to the host running your gateway like this.
   * <p/>
   * <code>
   * http://&lt;your host&gt;:9090/ws/v1/applications/&lt;your app id&gt;/logicalPlan/operators/&lt;operatorname&gt;/aggregation
   * </code>
   * <p/>
   * @since 1.0.4
   */
  public final static class FileCountersAggregator implements CountersAggregator,
                                                        Serializable
  {
    private static final long serialVersionUID = 201409041428L;
    MutableLong totalLocalProcessedFiles = new MutableLong();
    MutableLong pendingFiles = new MutableLong();
    MutableLong totalLocalNumberOfFailures = new MutableLong();
    MutableLong totalLocalNumberOfRetries = new MutableLong();

    @Override
    @SuppressWarnings("unchecked")
    public Object aggregate(Collection<?> countersList)
    {
      if(countersList.isEmpty()) {
        return null;
      }

      BasicCounters<MutableLong> tempFileCounters = (BasicCounters<MutableLong>) countersList.iterator().next();
      MutableLong globalProcessedFiles = tempFileCounters.getCounter(FileCounters.GLOBAL_PROCESSED_FILES);
      MutableLong globalNumberOfFailures = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_FAILURES);
      MutableLong globalNumberOfRetries = tempFileCounters.getCounter(FileCounters.GLOBAL_NUMBER_OF_RETRIES);
      totalLocalProcessedFiles.setValue(0);
      pendingFiles.setValue(0);
      totalLocalNumberOfFailures.setValue(0);
      totalLocalNumberOfRetries.setValue(0);

      for(Object fileCounters: countersList) {
        BasicCounters<MutableLong> basicFileCounters = (BasicCounters<MutableLong>) fileCounters;
        totalLocalProcessedFiles.add(basicFileCounters.getCounter(FileCounters.LOCAL_PROCESSED_FILES));
        pendingFiles.add(basicFileCounters.getCounter(FileCounters.PENDING_FILES));
        totalLocalNumberOfFailures.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_FAILURES));
        totalLocalNumberOfRetries.add(basicFileCounters.getCounter(FileCounters.LOCAL_NUMBER_OF_RETRIES));
      }

      globalProcessedFiles.add(totalLocalProcessedFiles);
      globalProcessedFiles.subtract(pendingFiles);
      globalNumberOfFailures.add(totalLocalNumberOfFailures);
      globalNumberOfRetries.add(totalLocalNumberOfRetries);

      BasicCounters<MutableLong> aggregatedCounters = new BasicCounters<MutableLong>(MutableLong.class);
      aggregatedCounters.setCounter(AggregatedFileCounters.PROCESSED_FILES, globalProcessedFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.PENDING_FILES, pendingFiles);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_ERRORS, totalLocalNumberOfFailures);
      aggregatedCounters.setCounter(AggregatedFileCounters.NUMBER_OF_RETRIES, totalLocalNumberOfRetries);

      return aggregatedCounters;
    }
  }

  /**
   * Call to hold information required to replay data for idempotence.
   */
  public static final class IdempotenceRecoveryData implements Serializable
  {
    private static final long serialVersionUID = 101720140004L;

    /**
     * This is true if this chunk of data reached the end of the file.
     */
    boolean eof = false;
    /**
     * The tuple offset (not byte offset) in the file where tuples began to be emitted (inclusive).
     */
    long startOffset;
    /**
     * The tuple offset (not byte offset) in the file after the tuples were emitted inclusive.
     */
    long endOffset;
    /**
     * The path of the file to replay tuples from.
     */
    String filePath;

    public IdempotenceRecoveryData()
    {
    }

    /**
     * This method return true if this recovery data object captured no data. We know no data
     * was captured when the start offset and the end offset equal.
     * @return True if this recovery data object captured no data.
     */
    public boolean isEmpty()
    {
      return startOffset >= endOffset;
    }
  }

  /**
   * This Comparator compares FileStatuses by modification time.
   */
  private static final class ComparatorChronologicalFile implements Comparator<FileStatus>
  {
    //Singleton pattern
    private static final ComparatorChronologicalFile instance = new ComparatorChronologicalFile();

    private ComparatorChronologicalFile()
    {
      //Do nothing
    }

    public static final ComparatorChronologicalFile getInstance()
    {
      return instance;
    }

    @Override
    public int compare(FileStatus fileStatusA,
                       FileStatus fileStatusB)
    {
      long timeA = fileStatusA.getModificationTime();
      long timeB = fileStatusB.getModificationTime();

      long diff = timeA - timeB;

      if(diff < 0L) {
        return -1;
      }

      if(diff > 0L) {
        return 1;
      }

      return fileStatusA.getPath().compareTo(fileStatusB.getPath());
    }
  }

  /**
   * The class that is used to scan for new files in the directory for the
   * AbstractFSDirectoryInputOperator.
   */
  public static class DirectoryScanner
  {
    protected String filePatternRegexp;
    protected transient Pattern regex = null;
    protected int partitionIndex = 0;
    protected int partitionCount = 1;
    private final transient HashSet<String> ignoredFiles = new HashSet<String>();
    /**
     * This flag determines if the chronological scanner is used or not.
     */
    private boolean chronological = true;

    /**
     * Gets the regex to use when scanning for files. Only files which match this regex are accepted by the scanner.
     * @return The regex to use when scanning for files.
     */
    public String getFilePatternRegexp()
    {
      return filePatternRegexp;
    }

    /**
     * This method a flag indicating whether or not the chronological scanner is preferred.
     * @param chronological A flag indicating whether or not the chronological scanner is preferred.
     */
    public void setChronological(boolean chronological)
    {
      this.chronological = chronological;
    }

    /**
     * This method returns a flag indicating whether or not the chronological scanner is preferred.
     * @return A flag indicating whether or not the chronological scanner is preferred.
     */
    public boolean getChronological()
    {
      return chronological;
    }

    /**
     * Sets the regex to use when scanning for files. Only files which match this regex are accepted by the scanner.
     * @param filePatternRegexp The regex to use when scanning for files.
     */
    public void setFilePatternRegexp(String filePatternRegexp)
    {
      this.filePatternRegexp = filePatternRegexp;
      this.regex = null;
    }

    /**
     * Gets the regex to use when scanning for files. Only files which match this regex are accepted by the scanner.
     * @return The regex to use when scanning for files.
     */
    public Pattern getRegex() {
      if (this.regex == null && this.filePatternRegexp != null)
        this.regex = Pattern.compile(this.filePatternRegexp);
      return this.regex;
    }

    /**
     * This returns the number of scanners used by the host logical operator.
     * @return The number of scanners used by the host logical operator.
     */
    public int getPartitionCount() {
      return partitionCount;
    }

    /**
     * This method gets the index of the scanner.
     * This is a number in the interval 0 &lt;= partitionIndex &lt; partitionCount.
     * @return The index of the scanner.
     */
    public int getPartitionIndex() {
      return partitionIndex;
    }

    /**
     * This method scans for newly found files in the given directory and returns them.
     * @param fs The filesystem on which the directory to be scanned is stored.
     * @param filePath The directory which holds the files to be scanned and parsed.
     * @param consumedFiles The files in the directory which have been scanned.
     * @return A set of newly found files.
     */
    public LinkedHashSet<Path> scan(FileSystem fs,
                                    Path filePath,
                                    Set<String> consumedFiles)
    {
      LOG.debug("Using nonchronological scanner method.");

      List<FileStatus> fileStatuses = scanHelper(fs,
                                                 filePath,
                                                 consumedFiles);

      LinkedHashSet<Path> paths = Sets.newLinkedHashSet();

      for(FileStatus fileStatus: fileStatuses) {
        paths.add(fileStatus.getPath());
      }

      return paths;
    }

    /**
     * This method is a helper method for other scanner methods.
     * @param fs The filesystem on which the directory to be scanned is stored.
     * @param filePath The directory which holds the files to be scanned and parsed.
     * @param consumedFiles The files in the directory which have been scanned.
     * @return A list of FileStatuses for newly found files.
     */
    @SuppressWarnings({"rawtypes"})
    private List<FileStatus> scanHelper(FileSystem fs,
                                        Path filePath,
                                        Set<String> consumedFiles)
    {
      if (filePatternRegexp != null && this.regex == null) {
        this.regex = Pattern.compile(this.filePatternRegexp);
      }

      List<FileStatus> result = Lists.newArrayList();

      try {
        LOG.debug("Scanning {} with pattern {}", filePath, this.filePatternRegexp);
        FileStatus[] files = fs.listStatus(filePath);
        for (FileStatus status : files)
        {
          Path path = status.getPath();
          String filePathStr = path.toString();

          if (consumedFiles.contains(filePathStr)) {
            continue;
          }

          if (ignoredFiles.contains(filePathStr)) {
            continue;
          }

          if (acceptFile(filePathStr)) {
            LOG.debug("Found {}", filePathStr);
            result.add(status);
          } else {
            // don't look at it again
            ignoredFiles.add(filePathStr);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.warn("Failed to list directory {}", filePath, e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result;
    }

    /**
     * This method scans for new files in the given directory and returns them in chronological order.
     * @param fs The filesystem on which the directory to be scanned is stored.
     * @param filePath The directory which holds the files to be scanned and parsed.
     * @param consumedFiles The files in the directory which have been scanned.
     * @return A list of newly found paths sorted in chronological order.
     */
    public List<Path> scanChronological(FileSystem fs,
                                        Path filePath,
                                        Set<String> consumedFiles)
    {
      LOG.debug("Using chronological scanner method.");
      List<FileStatus> fileStatuses = scanHelper(fs,
                                                 filePath,
                                                 consumedFiles);
      List<Path> paths = Lists.newArrayList();

      Collections.sort(fileStatuses,
                       ComparatorChronologicalFile.getInstance());

      for(FileStatus fileStatus: fileStatuses) {
        paths.add(fileStatus.getPath());
      }

      return paths;
    }

    /**
     * This returns true if this is an accepted file path for this scanner.
     * @param filePathStr The path to check.
     * @return True if this is an accepted file path for this scanner.
     */
    protected boolean acceptFile(String filePathStr)
    {
      if (partitionCount > 1) {
        int i = filePathStr.hashCode();
        int mod = i % partitionCount;
        if (mod < 0) {
          mod += partitionCount;
        }
        LOG.debug("partition {} {} {} {}", partitionIndex, filePathStr, i, mod);

        if (mod != partitionIndex) {
          return false;
        }
      }
      if (filePatternRegexp != null && this.regex == null) {
        regex = Pattern.compile(this.filePatternRegexp);
      }

      if (regex != null)
      {
        Matcher matcher = regex.matcher(filePathStr);
        if (!matcher.matches()) {
          return false;
        }
      }
      return true;
    }

    /**
     * This creates a new set of directory scanners with the same settings as this
     * directory scanner and the given partition count.
     * @param count The partition count for the new set of DirectoryScanners.
     * @return The new set of directory scanners.
     */
    public List<DirectoryScanner> partition(int count)
    {
      ArrayList<DirectoryScanner> partitions = Lists.newArrayListWithExpectedSize(count);
      for (int i=0; i<count; i++) {
        partitions.add(this.createPartition(i, count));
      }
      return partitions;
    }

    /**
     * This creates a new set of directory scanners with the same settings as this
     * directory scanner and the given partition count.
     * @param count The partition count for the new set of DirectoryScanners.
     * @param scanners This parameter is not used at this time.
     * @return The new set of directory scanners.
     */
    public List<DirectoryScanner>  partition(int count , Collection<DirectoryScanner> scanners) {
      return partition(count);
    }

    /**
     * Creates a new scanner with the given partitionIndex and partitionCount, and with the
     * same settings as this scanner.
     * @param partitionIndex The index of the scanner.
     * @param partitionCount The number of scanners used by the host logical operator.
     * @return
     */
    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      DirectoryScanner that = new DirectoryScanner();
      that.setChronological(this.getChronological());
      that.filePatternRegexp = this.filePatternRegexp;
      that.regex = this.regex;
      that.partitionIndex = partitionIndex;
      that.partitionCount = partitionCount;
      return that;
    }

    @Override
    public String toString()
    {
      return "DirectoryScanner [filePatternRegexp=" + filePatternRegexp + " partitionIndex=" +
          partitionIndex + " partitionCount=" + partitionCount + "]";
    }
  }
}
