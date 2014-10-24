package com.datatorrent.contrib.goldengate.lib;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datatorrent.lib.io.fs.AbstractHDFSInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.DTThrowable;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 10/23/14.
 */
public class CSVFileInput extends AbstractHDFSInputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(CSVFileInput.class);
  private transient Runnable fileReader;
  private transient Thread fileHelperTh;
  private volatile boolean fileThStop;
  private transient BufferedReader bufferedReader;
  private ArrayBlockingQueue<String> lines;
  private int lineBufferCapacity = 100;
  private int maxLineEmit = 100;

  public CSVFileInput() {
    fileReader = new FileReader();
    fileHelperTh = new Thread(fileReader);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    FSDataInputStream inputStream = openFile(getFilePath());
    bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    lines = new ArrayBlockingQueue<String>(lineBufferCapacity);
    fileThStop = false;
    fileHelperTh.start();
  }

  @Override
  public void teardown()
  {
    fileThStop = true;
    try {
      fileHelperTh.join();
    } catch (InterruptedException e) {
      logger.error("Wait interrupted", e);
      DTThrowable.rethrow(e);
    } finally {
      try {
        bufferedReader.close();
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
    super.teardown();
  }

  public transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();

  private class FileReader implements Runnable {
    @Override
    public void run()
    {
      while (!fileThStop) {
        try {
          String line = bufferedReader.readLine();
          if (line != null) {
            lines.add(line);
          }
        } catch (IOException e) {
          DTThrowable.rethrow(e);
        }
      }
    }
  }

  @Override
  public void emitTuples(FSDataInputStream stream)
  {
    String line = null;
    int count = 0;
    while (((line = lines.poll()) != null) && (count < maxLineEmit)) {
      outputPort.emit(line);
      ++count;
    }
  }

  public int getMaxLineEmit()
  {
    return maxLineEmit;
  }

  public void setMaxLineEmit(int maxLineEmit)
  {
    this.maxLineEmit = maxLineEmit;
  }

  public int getLineBufferCapacity()
  {
    return lineBufferCapacity;
  }

  public void setLineBufferCapacity(int lineBufferCapacity)
  {
    this.lineBufferCapacity = lineBufferCapacity;
  }
}
