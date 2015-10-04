/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.demos.echoserver;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;

/**
 * @since 2.1.0
 */
public class MessageResponder extends BaseOperator
{

  private String responseHeader = "Response: ";

  private int port = 9000;
  private int maxMesgSize = 512;
  private transient NetworkManager.ChannelAction<DatagramChannel> action;
  private transient ByteBuffer buffer;

  public transient final DefaultInputPort<Message> messageInput = new DefaultInputPort<Message>()
  {
    @Override
    public void process(Message message)
    {
      String sendMesg = responseHeader + message.message;
      SocketAddress address = message.socketAddress;
      buffer.put(sendMesg.getBytes());
      buffer.flip();
      try {
        action.channelConfiguration.channel.send(buffer, address);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      buffer.clear();
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      buffer = ByteBuffer.allocate(maxMesgSize);
      action = NetworkManager.getInstance().registerAction(port, NetworkManager.ConnectionType.UDP, null, 0);
    } catch (IOException e) {
      throw new RuntimeException("Error initializer responder", e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      NetworkManager.getInstance().unregisterAction(action);
    } catch (Exception e) {
      throw new RuntimeException("Error shutting down responder", e);
    }
  }
}
