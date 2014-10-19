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
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator.DirectoryScanner;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator.IdempotenceRecoveryData;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.*;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.slf4j.LoggerFactory;

public class AbstractFSDirectoryInputOperatorTest
{
  public static final int OPERATOR_A_ID = 0;
  public static final int OPERATOR_B_ID = 1;

  public static final String APPLICATION_ID = "1";

  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextA;
  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextB;

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;

      AttributeMap attributeMapA = new DefaultAttributeMap();
      attributeMapA.put(DAG.APPLICATION_ID, APPLICATION_ID);

      AttributeMap attributeMapB = new DefaultAttributeMap();
      attributeMapB.put(DAG.APPLICATION_ID, APPLICATION_ID);

      operatorContextA = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_A_ID,
                                                                             attributeMapA);
      operatorContextB = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_B_ID,
                                                                             attributeMapB);
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      try {
        FileUtils.deleteDirectory(new File(this.dir));
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  };

  @Rule public TestMeta testMeta = new TestMeta();

  public static class TestFSDirectoryInputOperator extends AbstractFSDirectoryInputOperator<String> implements Cloneable
  {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TestFSDirectoryInputOperator.class);

    //@OutputPortFieldAnnotation(name = "output")
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
    private transient BufferedReader br = null;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      logger.debug("Opened file {}", path);
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));

      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      logger.debug("closed file");
      super.closeFile(is);
      br.close();
      br = null;
    }

    @Override
    protected String readEntity() throws IOException
    {
      logger.debug("Buffered Reader {}", br);
      String line = br.readLine();
      logger.debug("Read line {}", line);
      return line;
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
    }

    @Override
    public void teardown()
    {
      super.teardown();

      try {
        if(br != null) {
          br.close();
        }
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      br = null;
    }

    @Override
    public TestFSDirectoryInputOperator clone()
    {
      TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
      oper.currentPartitions = this.currentPartitions;
      oper.setMaxRetryCount(this.getMaxRetryCount());
      oper.partitionCount = this.partitionCount;
      oper.emitBatchSize = this.emitBatchSize;
      oper.scanIntervalMillis = this.scanIntervalMillis;
      oper.directory = this.directory;
      oper.scanner = this.scanner;
      oper.processedFiles = Sets.newHashSet(this.processedFiles);
      oper.lastRepartition = this.lastRepartition;
      oper.pendingFiles = Lists.newArrayList(this.pendingFiles);
      oper.unfinishedFiles = Lists.newLinkedList(this.unfinishedFiles);
      oper.failedFiles = Lists.newLinkedList(this.failedFiles);
      oper.offset = this.offset;
      oper.currentFile = this.currentFile;
      oper.globalNumberOfFailures = new MutableLong(this.globalNumberOfFailures.longValue());
      oper.localNumberOfFailures = new MutableLong(this.localNumberOfFailures.longValue());
      oper.globalNumberOfRetries = new MutableLong(this.globalNumberOfRetries.longValue());
      oper.localNumberOfRetries = new MutableLong(this.localNumberOfRetries.longValue());
      oper.setIdempotenceAgent(this.getIdempotenceAgent().clone());
      return oper;
    }
  }

  private List<String> fileHelper(int count)
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    }
    catch(IOException e) {
      throw new RuntimeException(e);
    }

    List<String> allLines = Lists.newArrayList();
    for (int file=0; file<count; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line=0; line<count; line++) {
        lines.add("f"+file+"l"+line);
      }
      allLines.addAll(lines);
      try {
        FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    return allLines;
  }

  public static class TestScanner extends AbstractFSDirectoryInputOperator.DirectoryScanner
  {
    @Override
    protected boolean acceptFile(String filePathStr)
    {
      if(getPartitionCount() == 1) {
        return true;
      }

      Path path = new Path(filePathStr);
      String fileName = path.getName();

      Pattern pattern = Pattern.compile("file(\\d+)");
      Matcher m = pattern.matcher(fileName);
      m.matches();
      int i = Integer.parseInt(m.group(1));

      return (i % getPartitionCount() == getPartitionIndex());
    }

    @Override
    protected DirectoryScanner createPartition(int partitionIndex, int partitionCount)
    {
      TestScanner that = new TestScanner();
      that.setChronological(this.getChronological());
      that.filePatternRegexp = this.filePatternRegexp;
      that.regex = this.regex;
      that.partitionIndex = partitionIndex;
      that.partitionCount = partitionCount;
      return that;
    }
  }

  //@Ignore
  @Test
  public void testSinglePartiton() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = fileHelper(2);

    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(operatorContextA);
    for (long wid=0; wid<3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  public void testSinglePartitonIdempotentSimple() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = fileHelper(2);

    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    FSIdempotenceAgent<IdempotenceRecoveryData> FSIA = new FSIdempotenceAgent<IdempotenceRecoveryData>();
    FSIA.setStreamCodec(new JavaSerializationStreamCodec<IdempotenceRecoveryData>());
    FSIA.setRecoveryDirectory(testMeta.dir);
    oper.setIdempotenceAgent(FSIA);
    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(operatorContextA);
    for (long wid=0; wid<3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, queryResults.collectedTuples);
  }

  private TestFSDirectoryInputOperator idempotentHelper(CollectorTestSink<String> queryResults)
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();

    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    FSIdempotenceAgent<IdempotenceRecoveryData> FSIA = new FSIdempotenceAgent<IdempotenceRecoveryData>();
    FSIA.setStreamCodec(new JavaSerializationStreamCodec<IdempotenceRecoveryData>());
    FSIA.setRecoveryDirectory(testMeta.dir);
    oper.setIdempotenceAgent(FSIA);
    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    return oper;
  }

  //@Ignore
  @Test
  public void testIdempotentCommitted()
  {
    fileHelper(2);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(2);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    File window0 = new File(testMeta.dir + "/" + APPLICATION_ID + "/" + OPERATOR_A_ID + "/0");
    Assert.assertTrue(window0.exists());

    oper.committed(0);

    Assert.assertFalse(window0.exists());
  }

  //@Ignore
  @Test
  public void testIdempotentRestartPartial()
  {
    fileHelper(5);

    List<String> lists;
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(2);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f0l0",
                               "f0l1");

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();

    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f0l2",
                               "f0l3");

    Assert.assertEquals(lists, queryResults.collectedTuples);

    oper.committed(0);
    oper.teardown();

    queryResults.collectedTuples.clear();

    oper.setup(operatorContextA);
    oper.beginWindow(1);
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f0l2",
                               "f0l3");

    Assert.assertEquals(lists, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  @SuppressWarnings("unchecked")
  public void testIdempotentMultiFileRestartWhole()
  {
    fileHelper(5);

    List<String> lists;
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    TestFSDirectoryInputOperator operBak;
    oper.setEmitBatchSize(2);

    operBak = oper.clone();

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    //Emit file 0
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    //Emit file 1
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    lists = Lists.newArrayList("f0l0",
                               "f0l1",
                               "f0l2",
                               "f0l3",
                               "f0l4",
                               "f1l0",
                               "f1l1",
                               "f1l2",
                               "f1l3",
                               "f1l4");

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();

    oper = operBak;
    oper.output.setSink((CollectorTestSink) queryResults);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    for(int counter = 0;
        counter < 20;
        counter++) {
      oper.emitTuples();
    }
    oper.endWindow();

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();
    operBak = oper.clone();

    oper.beginWindow(1);
    //read ahead
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    lists = Lists.newArrayList("f2l0",
                               "f2l1",
                               "f2l2",
                               "f2l3",
                               "f2l4");

    Assert.assertEquals(lists, queryResults.collectedTuples);

    oper = operBak;
    oper.output.setSink((CollectorTestSink) queryResults);
    queryResults.collectedTuples.clear();

    oper.setup(operatorContextA);
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    Assert.assertEquals(lists, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  @SuppressWarnings("unchecked")
  public void testIdempotentMultiFileRestartPartial()
  {
    fileHelper(5);

    List<String> lists;
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    TestFSDirectoryInputOperator operBak;
    oper.setEmitBatchSize(2);

    operBak = oper.clone();

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    //Emit file 0
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    //Emit file 1
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    //Emit file 2
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    lists = Lists.newArrayList("f0l0",
                               "f0l1",
                               "f0l2",
                               "f0l3",
                               "f0l4",
                               "f1l0",
                               "f1l1",
                               "f1l2",
                               "f1l3",
                               "f1l4",
                               "f2l0",
                               "f2l1");

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();

    oper = operBak;
    oper.output.setSink((CollectorTestSink) queryResults);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    for(int counter = 0;
        counter < 20;
        counter++) {
      oper.emitTuples();
    }
    oper.endWindow();

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();
    operBak = oper.clone();

    oper.beginWindow(1);
    //read ahead
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    lists = Lists.newArrayList("f2l2",
                               "f2l3");

    Assert.assertEquals(lists, queryResults.collectedTuples);

    oper = operBak;
    oper.output.setSink((CollectorTestSink) queryResults);
    queryResults.collectedTuples.clear();

    oper.setup(operatorContextA);
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    Assert.assertEquals(lists, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  public void testIdempotentRestartWhole()
  {
    fileHelper(5);

    List<String> lists;
    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(5);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f0l0",
                               "f0l1",
                               "f0l2",
                               "f0l3",
                               "f0l4");

    Assert.assertEquals(lists, queryResults.collectedTuples);
    queryResults.collectedTuples.clear();

    oper.beginWindow(1);
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f1l0",
                               "f1l1",
                               "f1l2",
                               "f1l3",
                               "f1l4");

    Assert.assertEquals(lists, queryResults.collectedTuples);

    oper.committed(0);
    oper.teardown();

    queryResults.collectedTuples.clear();

    oper.setup(operatorContextA);
    oper.beginWindow(1);
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();

    lists = Lists.newArrayList("f1l0",
                               "f1l1",
                               "f1l2",
                               "f1l3",
                               "f1l4");

    Assert.assertEquals(lists, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  public void testSinglePartitonIdempotentReplay()
  {
    fileHelper(5);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(2);

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    queryResults.collectedTuples.clear();

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();

    List<String> lists = Lists.newArrayList("f0l0",
                                            "f0l1");

    Assert.assertEquals(lists, queryResults.collectedTuples);
  }

  //@Ignore
  @Test
  public void testScannerPartitioning() throws Exception
  {
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setFilePatternRegexp(".*partition([\\d]*)");

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    FileSystem fs = FileSystem.get(FileContext.getLocalFSFileContext().getDefaultFileSystem().getUri(), new Configuration());
    List<DirectoryScanner> partitions = scanner.partition(2);
    Set<Path> allFiles = Sets.newHashSet();
    for (DirectoryScanner partition : partitions) {
      Set<Path> files = partition.scan(fs, path, Sets.<String>newHashSet());
      Assert.assertEquals("", 2, files.size());
      allFiles.addAll(files);
    }

    Assert.assertEquals("Found all files " + allFiles, 4, allFiles.size());
  }

  //@Ignore
  @Test
  public void testIdempotentPartitioning1()
  {
    fileHelper(5);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(5);
    oper.setScanner(new TestScanner());

    TestFSDirectoryInputOperator operBak = oper.clone();
    operBak.setScanner(new TestScanner());

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(operBak));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = oper.definePartitions(partitions, 1);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(2, oper.getCurrentPartitions());

    int counter = 0;
    TestFSDirectoryInputOperator oper0 = null;
    TestFSDirectoryInputOperator oper1 = null;

    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      p.getPartitionedInstance();

      if(counter == 0) {
        oper0 = (TestFSDirectoryInputOperator) p.getPartitionedInstance();
      }
      else {
        oper1 = (TestFSDirectoryInputOperator) p.getPartitionedInstance();
      }

      counter++;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sinkA = (CollectorTestSink) new CollectorTestSink<String>();
    oper0.output.setSink(sinkA);

    oper0.setup(operatorContextA);
    oper0.beginWindow(0);
    oper0.emitTuples();
    oper0.emitTuples();
    oper0.emitTuples();
    oper0.endWindow();

    List<String> listA = Lists.newArrayList("f0l0",
                                            "f0l1",
                                            "f0l2",
                                            "f0l3",
                                            "f0l4");

    Assert.assertEquals(listA, sinkA.collectedTuples);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sinkB = (CollectorTestSink) new CollectorTestSink<String>();
    oper1.output.setSink(sinkB);

    oper1.setup(operatorContextA);
    oper1.beginWindow(0);
    oper1.emitTuples();
    oper1.emitTuples();
    oper1.emitTuples();
    oper1.endWindow();

    List<String> listB = Lists.newArrayList();

    Assert.assertEquals(listB, sinkB.collectedTuples);
  }

  //@Ignore
  @Test
  public void testIdempotentPartitioning2()
  {
    fileHelper(5);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestFSDirectoryInputOperator oper = idempotentHelper(queryResults);
    oper.setEmitBatchSize(5);
    oper.setScanner(new TestScanner());

    TestFSDirectoryInputOperator operBak = oper.clone();
    operBak.setScanner(new TestScanner());

    oper.setup(operatorContextA);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.emitTuples();
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(operBak));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = oper.definePartitions(partitions, 1);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(2, oper.getCurrentPartitions());

    int counter = 0;
    TestFSDirectoryInputOperator oper0 = null;
    TestFSDirectoryInputOperator oper1 = null;

    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      p.getPartitionedInstance();

      if(counter == 0) {
        oper0 = (TestFSDirectoryInputOperator) p.getPartitionedInstance();
      }
      else {
        oper1 = (TestFSDirectoryInputOperator) p.getPartitionedInstance();
      }

      counter++;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sinkA = (CollectorTestSink) new CollectorTestSink<String>();
    oper0.output.setSink(sinkA);

    oper0.setup(operatorContextA);
    oper0.beginWindow(0);
    oper0.emitTuples();
    oper0.emitTuples();
    oper0.emitTuples();
    oper0.endWindow();

    List<String> listA = Lists.newArrayList("f0l0",
                                            "f0l1",
                                            "f0l2",
                                            "f0l3",
                                            "f0l4");

    Assert.assertEquals(listA, sinkA.collectedTuples);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sinkB = (CollectorTestSink) new CollectorTestSink<String>();
    oper1.output.setSink(sinkB);

    oper1.setup(operatorContextA);
    oper1.beginWindow(0);
    oper1.emitTuples();
    oper1.emitTuples();
    oper1.emitTuples();
    oper1.endWindow();

    List<String> listB = Lists.newArrayList("f1l0",
                                            "f1l1",
                                            "f1l2",
                                            "f1l3",
                                            "f1l4");

    Assert.assertEquals(listB, sinkB.collectedTuples);
  }

  //@Ignore
  @Test
  public void testPartitioning() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = oper.definePartitions(partitions, 1);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(2, oper.getCurrentPartitions());

    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
      Set<String> consumed = Sets.newHashSet();
      LinkedHashSet<Path> files = p.getPartitionedInstance().getScanner().scan(FileSystem.getLocal(new Configuration(false)), path, consumed);
      Assert.assertEquals("partition " + files, 2, files.size());
    }
  }

  /**
   * Test for testing dynamic partitioning.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read all records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit records again, expected result is no record is emitted, as all files are
   *   processed.
   * - Create another 4 files with 3 records each
   * - Try to emit records again, expected result total record emitted 4 * 3 = 12.
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testPartitioningStateTransfer() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);

    TestFSDirectoryInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file = 0;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    // Read all records to populate processedList in operator.
    oper.setup(operatorContextA);
    for(int i = 0; i < 10; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = initialState.definePartitions(partitions, 0);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFSDirectoryInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    List<AbstractFSDirectoryInputOperator<String>> opers = Lists.newArrayList();
    int contextCounter = 1;
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      TestFSDirectoryInputOperator oi = (TestFSDirectoryInputOperator)p.getPartitionedInstance();
      AttributeMap attributeMap = new DefaultAttributeMap();
      attributeMap.put(DAG.APPLICATION_ID, APPLICATION_ID);
      OperatorContextTestHelper.TestIdOperatorContext context =
      new OperatorContextTestHelper.TestIdOperatorContext(contextCounter,
                                                          attributeMap);
      oi.setup(context);
      oi.output.setSink(sink);
      opers.add(oi);
      contextCounter++;
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("No new tuples read ", 0, sink.collectedTuples.size());

    // Add four new files with 3 records each.
    for (; file<8; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // If all files are processed only once then number of records emitted should
    // be 12.
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());
  }

  /**
   * Test for testing dynamic partitioning.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read some records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit the remaining records.
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testPartitioningStateTransferInterrupted() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    TestFSDirectoryInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file = 0;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(operatorContextA);
    for(int i = 0; i < 5; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }

    Assert.assertEquals("Partial tuples read ", 6, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = initialState.definePartitions(partitions, 0);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFSDirectoryInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    int contextCounter = 1;
    List<AbstractFSDirectoryInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      TestFSDirectoryInputOperator oi = (TestFSDirectoryInputOperator)p.getPartitionedInstance();
      AttributeMap attributeMap = new DefaultAttributeMap();
      attributeMap.put(DAG.APPLICATION_ID, APPLICATION_ID);
      OperatorContextTestHelper.TestIdOperatorContext context =
      new OperatorContextTestHelper.TestIdOperatorContext(contextCounter,
                                                          attributeMap);
      oi.setup(context);
      oi.output.setSink(sink);
      opers.add(oi);
      contextCounter++;
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    Assert.assertEquals("Remaining tuples read ", 6, sink.collectedTuples.size());
  }

  /**
   * Test for testing dynamic partitioning interrupting ongoing read.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read some records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit the remaining records.
   * @throws Exception
   */
  //@Ignore
  @Test
  public void testPartitioningStateTransferFailure() throws Exception
  {
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    TestFSDirectoryInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file = 0;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(operatorContextA);
    for(int i = 0; i < 5; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }

    Assert.assertEquals("Partial tuples read ", 6, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = initialState.definePartitions(partitions, 0);
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFSDirectoryInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    int contextCounter = 1;
    List<AbstractFSDirectoryInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      TestFSDirectoryInputOperator oi = (TestFSDirectoryInputOperator)p.getPartitionedInstance();
      AttributeMap attributeMap = new DefaultAttributeMap();
      attributeMap.put(DAG.APPLICATION_ID, APPLICATION_ID);
      OperatorContextTestHelper.TestIdOperatorContext context =
      new OperatorContextTestHelper.TestIdOperatorContext(contextCounter,
                                                          attributeMap);
      oi.setup(context);
      oi.output.setSink(sink);
      opers.add(oi);
      contextCounter++;
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFSDirectoryInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("Remaining tuples read ", 6, sink.collectedTuples.size());
  }
}
