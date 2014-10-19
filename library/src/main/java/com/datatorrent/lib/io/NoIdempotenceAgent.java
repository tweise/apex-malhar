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

import com.datatorrent.api.annotation.Stateless;

/**
 * This IdempotenceAgent will never do recovery. This is a convenience class so that operators
 * can use the same logic for maintaining idempotency and not maintaining idempotency.
 */
public class NoIdempotenceAgent<T> extends IdempotenceAgent<T>
{
  public NoIdempotenceAgent()
  {
    //Do nothing
  }

  @Override
  public void beginWindowPri(long windowId)
  {
    //Do nothing
  }

  @Override
  public void endWindowPri()
  {
    //Do nothing
  }

  @Override
  public boolean hasNext()
  {
    //There will never be a next element
    return false;
  }

  @Override
  public void write(byte[] bytes)
  {
    //Do nothing
  }

  @Override
  public void checkpointed(long windowId)
  {
    //Do nothing
  }

  @Override
  public void committed(long windowId)
  {
    //Do nothing
  }

  @Override
  public void teardown()
  {
    //Do nothing
  }

  @Override
  protected byte[] nextTupleBytes()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isActive()
  {
    return false;
  }

  @Override
  public IdempotenceAgent<T> clone()
  {
    return new NoIdempotenceAgent<T>();
  }

  @Override
  public long computeLargestRecoveryWindow()
  {
    return Stateless.WINDOW_ID;
  }
}
