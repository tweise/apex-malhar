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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;

public class FSIdempotenceAgent implements IdempotenceAgent
{
  @NotNull
  protected String recoveryDirectory;
  protected boolean ignoreTempFile;

  @NotNull
  private transient final List<Long> completedWindowsToRecover;
  private transient long partialWindowToRecover;
  private transient long largestRecoveryWindow;

  private transient Configuration configuration;
  private transient FileSystem recoveryFs;

  private transient long currentWindowId;
  private transient FSDataInputStream input;
  private transient FSDataOutputStream output;
  private transient long currentFileSize;
  private transient long currentPos;
  private transient boolean uncleanEnd;

  public FSIdempotenceAgent()
  {
    recoveryDirectory = "recovery";
    completedWindowsToRecover = Lists.newArrayList();
    partialWindowToRecover = Stateless.WINDOW_ID;
    largestRecoveryWindow = Stateless.WINDOW_ID;
  }

  @Override
  public void setRecoveryDirectory(@NotNull String recoveryDirectory)
  {
    this.recoveryDirectory = recoveryDirectory;
  }

  @Override
  public String getRecoveryDirectory()
  {
    return recoveryDirectory;
  }

  @Override
  public void setIgnoreTempFile(boolean ignoreTempFile)
  {
    this.ignoreTempFile = ignoreTempFile;
  }

  @Override
  public boolean isIgnoreTempFile()
  {
    return ignoreTempFile;
  }

  @Override
  public void openWindow(long windowId)
  {
    currentWindowId = windowId;
    LOG.debug("current {} largest {}", windowId, largestRecoveryWindow);
    try {
      //Recover windows
      if (windowId <= largestRecoveryWindow) {
        Path windowPath;
        if (windowId == partialWindowToRecover) {
          windowPath = new Path(recoveryDirectory, Long.toString(windowId) + ".tmp");
        }
        else {
          windowPath = new Path(recoveryDirectory, Long.toString(windowId));
        }
        FileStatus status = recoveryFs.getFileStatus(windowPath);
        currentFileSize = status.getLen();
        if (currentFileSize <= 0) {
          return;
        }
        LOG.debug("recovery {}", windowId);
        input = recoveryFs.open(windowPath);
        currentPos = 0;
      }
      //When not recovering then persist data for recovery
      else {
        Path tmpWindowPath = new Path(recoveryDirectory, Long.toString(currentWindowId) + ".tmp");
        output = recoveryFs.create(tmpWindowPath);
      }
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public boolean hasNext()
  {
    if (input == null) {
      return false;
    }
    return currentPos < currentFileSize && !uncleanEnd;
  }

  @Override
  public byte[] next()
  {
    int bytesToRead;
    try {
      bytesToRead = input.readInt();
      byte[] buffer = new byte[bytesToRead];
      input.read(buffer);
      currentPos = input.getPos();
      return buffer;
    }
    catch (EOFException e) {
      uncleanEnd = true;
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
  }

  @Override
  public void write(byte[] bytes)
  {
    try {
      output.writeInt(bytes.length);
      output.write(bytes);
      if (!ignoreTempFile) {
        output.flush();
      }
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void closeWindow()
  {
    try {
      if (input != null) {
        input.close();
        input = null;
      }
      if (output != null) {
        output.close();
        output = null;
      }
      if (currentWindowId > largestRecoveryWindow) {
        recoveryFs.rename(new Path(recoveryDirectory, Long.toString(currentWindowId) + ".tmp"), new Path(recoveryDirectory, Long.toString(currentWindowId)));
      }
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void deleteWindow()
  {
    try {
      recoveryFs.delete(new Path(recoveryDirectory, Long.toString(currentWindowId)), true);
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public RecoveryType getRecoveryType()
  {
    return currentWindowId > largestRecoveryWindow ? RecoveryType.NOT_REQUIRED :
      (currentWindowId == partialWindowToRecover ? RecoveryType.PARTIAL : RecoveryType.COMPLETE);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      Path recoveryPath = new Path(recoveryDirectory);
      configuration = new Configuration();
      recoveryFs = FileSystem.newInstance(recoveryPath.toUri(), configuration);

      if (recoveryFs.exists(recoveryPath)) {
        FileStatus[] statuses = recoveryFs.listStatus(recoveryPath);
        for (FileStatus status : statuses) {
          String fileName = status.getPath().getName();
          if (fileName.endsWith(".tmp")) {
            if (ignoreTempFile) {
              recoveryFs.delete(new Path(recoveryPath, fileName), true);
            }
            else {
              partialWindowToRecover = Long.parseLong(fileName.substring(0, fileName.indexOf(".")));
            }
          }
          else {
            completedWindowsToRecover.add(Long.parseLong(fileName));
          }
        }
        largestRecoveryWindow = partialWindowToRecover != Stateless.WINDOW_ID ? partialWindowToRecover :
          (completedWindowsToRecover.isEmpty() ? Stateless.WINDOW_ID : completedWindowsToRecover.get(completedWindowsToRecover.size() - 1));
      }
      LOG.debug("partialWin {} largestRecovery {}", partialWindowToRecover, largestRecoveryWindow);
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    Collections.sort(completedWindowsToRecover);
  }

  @Override
  public void teardown()
  {
    try {
      recoveryFs.close();
    }
    catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    Iterator<Long> iterator = completedWindowsToRecover.iterator();
    while (iterator.hasNext()) {
      long sWindow = iterator.next();
      if (sWindow <= windowId) {
        Path windowPath = new Path(recoveryDirectory, Long.toString(sWindow));
        try {
          LOG.debug("delete recovery {}", sWindow);
          recoveryFs.delete(windowPath, true);
          iterator.remove();
        }
        catch (IOException e) {
          DTThrowable.rethrow(e);
        }
      }
      else {
        break;
      }
    }

    if (partialWindowToRecover != Stateless.WINDOW_ID && partialWindowToRecover <= windowId) {
      Path windowPath = new Path(recoveryDirectory, Long.toString(partialWindowToRecover));
      try {
        LOG.debug("delete recovery {}", partialWindowToRecover);
        recoveryFs.delete(windowPath, true);
        partialWindowToRecover = Stateless.WINDOW_ID;
      }
      catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FSIdempotenceAgent.class);
}
