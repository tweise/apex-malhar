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

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.Slice;
import com.datatorrent.lib.codec.JavaSerializationStreamCodec;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotenceAgent;
import com.datatorrent.lib.io.fs.AbstractFSWriterTest.FSTestWatcher;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import java.io.*;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSIdempotenceAgentTest
{
  private static final Logger LOG = LoggerFactory.getLogger(FSIdempotenceAgentTest.class);

  public static final int OPERATOR_A_ID = 0;
  public static final int OPERATOR_B_ID = 1;
  public static final int OPERATOR_C_ID = 2;
  public static final int OPERATOR_D_ID = 3;

  public static final String APPLICATION_ID = "1";

  public static FSIdempotenceAgent<String> FSIA;
  public static FSIdempotenceAgent<String> FSIB;
  public static FSIdempotenceAgent<String> FSIC;
  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextA;
  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextB;
  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextC;
  public static OperatorContextTestHelper.TestIdOperatorContext operatorContextD;

  @Rule
  public PrivateTestWatcher testWatcher = new PrivateTestWatcher();

  public static class PrivateTestWatcher extends FSTestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);

      File file = new File(getDir());

      try {
        FileUtils.deleteDirectory(file);
      }
      catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }

      FSIA = new FSIdempotenceAgent<String>();
      FSIA.setRecoveryDirectory(getDir());
      FSIA.setStreamCodec(new JavaSerializationStreamCodec<String>());
      FSIB = new FSIdempotenceAgent<String>();
      FSIB.setRecoveryDirectory(getDir());
      FSIB.setStreamCodec(new JavaSerializationStreamCodec<String>());
      FSIC = new FSIdempotenceAgent<String>();
      FSIC.setRecoveryDirectory(getDir());
      FSIC.setStreamCodec(new JavaSerializationStreamCodec<String>());

      AttributeMap attributeMapA = new DefaultAttributeMap();
      attributeMapA.put(DAG.APPLICATION_ID, APPLICATION_ID);
      AttributeMap attributeMapB = new DefaultAttributeMap();
      attributeMapB.put(DAG.APPLICATION_ID, APPLICATION_ID);
      AttributeMap attributeMapC = new DefaultAttributeMap();
      attributeMapC.put(DAG.APPLICATION_ID, APPLICATION_ID);
      AttributeMap attributeMapD = new DefaultAttributeMap();
      attributeMapD.put(DAG.APPLICATION_ID, APPLICATION_ID);

      operatorContextA = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_A_ID,
                                                                             attributeMapA);
      operatorContextB = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_B_ID,
                                                                             attributeMapB);
      operatorContextC = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_C_ID,
                                                                             attributeMapC);
      operatorContextD = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_D_ID,
                                                                             attributeMapD);
    }

    @Override
    protected void finished(Description description)
    {
    }
  }

  private static void checkOutput(StreamCodec<String> streamCodec,
                                  Path filePath,
                                  List<String> strings)
  {
    File file = new File(filePath.toUri().getPath());
    FileInputStream fileInputStream = null;

    try {
      fileInputStream = new FileInputStream(file);
    }
    catch (FileNotFoundException ex) {
      DTThrowable.rethrow(ex);
    }

    List<String> savedStrings = new ArrayList<String>();

    try {
      byte[] byteCount = new byte[4];

      while(fileInputStream.read(byteCount) != -1) {
        int byteCountInt = ((((int) byteCount[0]) & 0xFF) << 24) |
                           ((((int) byteCount[1]) & 0xFF) << 16) |
                           ((((int) byteCount[2]) & 0xFF) << 8) |
                           (((int) byteCount[3]) & 0xFF);
        byte[] stringBytes = new byte[byteCountInt];
        int returnVal = fileInputStream.read(stringBytes);

        if(returnVal == -1) {
          Assert.fail("Incorrect number of bytes in " + file.getPath());
        }

        savedStrings.add((String) streamCodec.fromByteArray(new Slice(stringBytes,0, stringBytes.length)));
      }
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }

    Assert.assertArrayEquals(strings.toArray(), savedStrings.toArray());
  }

  ////@Ignore
  @Test
  public void testAgentSerialization()
  {
    Kryo kryo = new Kryo();
    FSIdempotenceAgent<String> agentA = FSIA;
    FSIdempotenceAgent<String> agentB = kryo.copy(agentA);
  }

  //@Ignore
  @Test
  public void testTupleSerialization()
  {
    FSIA.setup(operatorContextA);
    FSIA.beginWindow(0);
    FSIA.write("1\n");
    FSIA.write("2\n");
    FSIA.write("3\n");
    FSIA.endWindow();
    FSIA.teardown();

    List<String> strings = new ArrayList<String>();
    strings.add("1\n");
    strings.add("2\n");
    strings.add("3\n");

    checkOutput(FSIA.getStreamCodec(),
                new Path(testWatcher.getDir() + "/" +
                         APPLICATION_ID + "/" + OPERATOR_A_ID, "0"),
                strings);
  }

  private void createTwoWindows()
  {
    FSIA.setup(operatorContextA);
    FSIA.beginWindow(0);
    FSIA.write("1\n");
    FSIA.write("2\n");
    FSIA.write("3\n");
    FSIA.endWindow();

    FSIA.beginWindow(1);
    FSIA.write("4\n");
    FSIA.write("5\n");
    FSIA.write("6\n");
  }

  private void createTwoWindowsCompleteA()
  {
    createTwoWindows();
    FSIA.endWindow();
    FSIA.teardown();
  }

  private void createThreeWindowsCompleteB()
  {
    FSIB.setup(operatorContextB);
    FSIB.beginWindow(0);
    FSIB.write("11\n");
    FSIB.write("12\n");
    FSIB.write("13\n");
    FSIB.write("14\n");
    FSIB.write("15\n");
    FSIB.endWindow();

    FSIB.beginWindow(1);
    FSIB.write("15\n");
    FSIB.write("16\n");
    FSIB.write("11\n");
    FSIB.write("12\n");
    FSIB.write("13\n");
    FSIB.write("14\n");
    FSIB.write("15\n");
    FSIB.endWindow();

    FSIB.beginWindow(2);
    FSIB.write("15\n");
    FSIB.write("16\n");
    FSIB.write("11\n");
    FSIB.write("12\n");
    FSIB.endWindow();
    FSIB.teardown();
  }

  private void createFiveWindowsCompleteC()
  {
    FSIC.setup(operatorContextC);
    FSIC.beginWindow(0);
    FSIC.write("21\n");
    FSIC.write("22\n");
    FSIC.endWindow();

    FSIC.beginWindow(1);
    FSIC.write("25\n");
    FSIC.endWindow();

    FSIC.beginWindow(2);
    FSIC.write("25\n");
    FSIC.write("26\n");
    FSIC.write("21\n");
    FSIC.write("22\n");
    FSIC.endWindow();

    FSIC.beginWindow(3);
    FSIC.write("25\n");
    FSIC.write("26\n");
    FSIC.write("21\n");
    FSIC.write("22\n");
    FSIC.write("21\n");
    FSIC.write("22\n");
    FSIC.endWindow();

    FSIC.beginWindow(4);
    FSIC.write("23\n");
    FSIC.write("22\n");
    FSIC.write("21\n");
    FSIC.endWindow();
    FSIC.teardown();
  }

  //@Ignore
  @Test
  public void testMultiWindowSave()
  {
    createTwoWindowsCompleteA();

    FSIA.setup(operatorContextA);

    Assert.assertEquals("Expected 2 completedWindowsToRecover",
                        2,
                        FSIA.getNumberOfWindowsToRecover());

    Assert.assertEquals("Expected 1 to be the largest window id",
                        1,
                        FSIA.getLargestRecoveryWindow());

    //

    List<String> strings = new ArrayList<String>();
    strings.add("1\n");
    strings.add("2\n");
    strings.add("3\n");

    checkOutput(FSIA.getStreamCodec(),
                new Path(FSIA.idempotentAgentRecoveryDirectoryPath, "0"),
                strings);

    strings = new ArrayList<String>();
    strings.add("4\n");
    strings.add("5\n");
    strings.add("6\n");

    checkOutput(FSIA.getStreamCodec(),
                new Path(FSIA.idempotentAgentRecoveryDirectoryPath, "1"),
                strings);
  }

  //@Ignore
  @Test
  public void testReplayEmptyWindows()
  {
    FSIA.setup(operatorContextA);

    Assert.assertEquals("Expected " + Stateless.WINDOW_ID + " to be the largest window id",
                        Stateless.WINDOW_ID,
                        FSIA.getLargestRecoveryWindow());

    FSIA.beginWindow(0);
    FSIA.endWindow();


    Assert.assertEquals("Expected " + Stateless.WINDOW_ID + " to be the largest window id",
                        Stateless.WINDOW_ID,
                        FSIA.getLargestRecoveryWindow());

    FSIA.beginWindow(1);
    FSIA.endWindow();
    FSIA.teardown();

    FSIA.setup(operatorContextA);

    Assert.assertEquals("Expected 1 to be the largest window id",
                        1,
                        FSIA.getLargestRecoveryWindow());

    FSIA.beginWindow(0);
    Assert.assertFalse(FSIA.hasNext());
    FSIA.endWindow();

    FSIA.beginWindow(1);
    Assert.assertFalse(FSIA.hasNext());
    FSIA.endWindow();

    FSIA.beginWindow(2);
    FSIA.endWindow();
  }

  //@Ignore
  @Test
  public void testMultiWindowSaveFail()
  {
    createTwoWindows();
    FSIA.teardown();

    FSIA.setup(operatorContextA);

    Assert.assertEquals("Expected 1 completedWindowsToRecover",
                        1,
                        FSIA.getNumberOfWindowsToRecover());

    Assert.assertEquals("Expected 1 to be the largest window id",
                        0,
                        FSIA.getLargestRecoveryWindow());

    List<String> strings = new ArrayList<String>();
    strings.add("1\n");
    strings.add("2\n");
    strings.add("3\n");

    checkOutput(FSIA.getStreamCodec(),
                new Path(FSIA.idempotentAgentRecoveryDirectoryPath, "0"),
                strings);

    ////

    FSIA.beginWindow(0);
    String window0String = "";
    while(FSIA.hasNext()) {
      window0String += FSIA.next();
    }
    FSIA.endWindow();

    Assert.assertEquals("1\n" +
                        "2\n" +
                        "3\n",
                        window0String);

    ////

    FSIA.beginWindow(1);
    FSIA.write("14\n");
    FSIA.write("15\n");
    FSIA.write("16\n");
    FSIA.endWindow();
    FSIA.teardown();

    FSIA.setup(operatorContextA);
    FSIA.beginWindow(1);
    String window1String = "";
    while(FSIA.hasNext()) {
      window1String += FSIA.next();
    }
    FSIA.endWindow();

    Assert.assertEquals("14\n" +
                        "15\n" +
                        "16\n",
                        window1String);

    FSIA.beginWindow(2);
    String window2String = "";
    while(FSIA.hasNext()) {
      window2String += FSIA.next();
    }
    FSIA.endWindow();

    Assert.assertEquals("",
                        window2String);
  }

  //@Ignore
  @Test
  public void testSimpleCommitted()
  {
    createTwoWindowsCompleteA();

    File tempFile0 = new File(testWatcher.getDir() + "/" +
                             APPLICATION_ID + "/" + OPERATOR_A_ID + "/0");
    File tempFile1 = new File(testWatcher.getDir() + "/" +
                              APPLICATION_ID + "/" + OPERATOR_A_ID + "/1");

    Assert.assertTrue(tempFile0.exists());
    Assert.assertTrue(tempFile1.exists());

    FSIA.setup(operatorContextA);
    FSIA.committed(0);
    FSIA.teardown();

    Assert.assertFalse(tempFile0.exists());
    Assert.assertTrue(tempFile1.exists());

    FSIA.setup(operatorContextA);
    FSIA.committed(1);
    FSIA.teardown();

    Assert.assertFalse(tempFile0.exists());
    Assert.assertFalse(tempFile1.exists());
  }

  //@Ignore
  @Test
  public void testReplayWindows()
  {
    createTwoWindowsCompleteA();
    FSIA.setup(operatorContextA);
    Assert.assertEquals(null, FSIA.isRecovering());

    ////

    FSIA.beginWindow(0);
    Assert.assertTrue(FSIA.isRecovering());

    String window1String = "";

    while(FSIA.hasNext()) {
      window1String += FSIA.next();
    }

    Assert.assertEquals("1\n" +
                        "2\n" +
                        "3\n", window1String);

    FSIA.endWindow();
    Assert.assertTrue(FSIA.isRecovering());

    ////

    FSIA.committed(0);

    ////

    FSIA.beginWindow(1);
    Assert.assertTrue(FSIA.isRecovering());

    String window2String = "";

    while(FSIA.hasNext()) {
      window2String += FSIA.next();
    }

    Assert.assertEquals("4\n" +
                        "5\n" +
                        "6\n", window2String);

    FSIA.endWindow();
    Assert.assertFalse(FSIA.isRecovering());

    ////

    FSIA.beginWindow(2);
    Assert.assertFalse(FSIA.hasNext());
    FSIA.endWindow();

    ////

    FSIA.beginWindow(3);
    FSIA.write("7\n");
    FSIA.write("8\n");
    FSIA.write("9\n");
    FSIA.endWindow();

    ////

    FSIA.committed(2);

    ////

    FSIA.teardown();

    FSIA.setup(operatorContextA);
    FSIA.beginWindow(3);

    String window3String = "";

    while(FSIA.hasNext()) {
      window3String += FSIA.next();
    }

    FSIA.endWindow();

    Assert.assertEquals("7\n" +
                        "8\n" +
                        "9\n", window3String);

    ////

    FSIA.committed(3);

    ////
  }

  private List<IdempotenceAgent<String>> testRepartitionHelper(int newPartitionCount)
  {
    createTwoWindowsCompleteA();
    createThreeWindowsCompleteB();
    createFiveWindowsCompleteC();

    List<IdempotenceAgent<String>> lists = Lists.newArrayList();
    lists.add(FSIA);
    lists.add(FSIB);
    lists.add(FSIC);

    Collection<IdempotenceAgent<String>> collection = IdempotenceAgent.repartitionEven(lists,
                                                                                       newPartitionCount);

    List<IdempotenceAgent<String>> newLists = Lists.newArrayList();
    newLists.addAll(collection);

    return newLists;
  }

  @Test
  public void testRepartition1()
  {
    List<IdempotenceAgent<String>> agents = testRepartitionHelper(1);

    List<String> window0 = Lists.newArrayList("1\n", "2\n", "3\n",
                                              "11\n", "12\n", "13\n",
                                              "14\n", "15\n", "21\n",
                                              "22\n");

    List<String> window1 = Lists.newArrayList("4\n", "5\n", "6\n",
                                              "15\n", "16\n", "11\n",
                                              "12\n", "13\n", "14\n",
                                              "15\n", "25\n");

    List<String> window2 = Lists.newArrayList("15\n", "16\n", "11\n",
                                              "12\n", "25\n", "26\n",
                                              "21\n", "22\n");

    List<String> window3 = Lists.newArrayList("25\n", "26\n", "21\n",
                                              "22\n", "21\n", "22\n");

    List<String> window4 = Lists.newArrayList("23\n", "22\n", "21\n");

    IdempotenceAgent<String> agent = agents.get(0);
    List<String> tempTupleList = Lists.newArrayList();

    agent.setup(operatorContextD);

    Assert.assertEquals(4, agent.getLargestRecoveryWindow());

    ////

    agent.beginWindow(0);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window0, tempTupleList);

    ////

    agent.beginWindow(1);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window1, tempTupleList);

    ////

    agent.beginWindow(2);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window2, tempTupleList);

    ////

    agent.beginWindow(3);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window3, tempTupleList);

    ////

    agent.beginWindow(4);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window4, tempTupleList);

    ////

    agent.committed(0);

    File windowA0 = new File(testWatcher.getDir() + "/" + APPLICATION_ID + "/" +
                             OPERATOR_A_ID + "/" + 0);
    Assert.assertFalse(windowA0.exists());
    File windowB0 = new File(testWatcher.getDir() + "/" + APPLICATION_ID + "/" +
                             OPERATOR_B_ID + "/" + 0);
    Assert.assertFalse(windowB0.exists());
    File windowC0 = new File(testWatcher.getDir() + "/" + APPLICATION_ID + "/" +
                             OPERATOR_C_ID + "/" + 0);
    Assert.assertFalse(windowC0.exists());

    ////

    agent.teardown();
    agent.setup(operatorContextA);

    agent.beginWindow(1);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window1, tempTupleList);

    ////

    agent.beginWindow(2);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window2, tempTupleList);

    ////

    agent.beginWindow(3);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window3, tempTupleList);

    ////

    agent.beginWindow(4);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window4, tempTupleList);

    agent.committed(2);

    agent.teardown();
    agent.setup(operatorContextA);

    agent.beginWindow(5);
    agent.write("1\n");
    agent.write("2\n");
    agent.endWindow();

    agent.teardown();

    List<String> window5 = Lists.newArrayList("1\n", "2\n");

    agent.setup(operatorContextA);

    ////

    agent.beginWindow(3);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window3, tempTupleList);

    ////

    agent.beginWindow(4);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    ////

    agent.beginWindow(5);
    tempTupleList.clear();
    while(agent.hasNext()) {
      tempTupleList.add(agent.next());
    }
    agent.endWindow();

    Assert.assertEquals(window5, tempTupleList);

    agent.beginWindow(6);
    Assert.assertFalse(agent.hasNext());
    agent.endWindow();
    agent.teardown();
  }
}