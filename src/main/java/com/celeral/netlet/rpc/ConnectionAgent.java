/*
 * Copyright 2017 Celeral.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.celeral.netlet.rpc;

import java.net.InetSocketAddress;

import com.celeral.netlet.EventLoop;
import com.celeral.netlet.Listener.ClientListener;

/**
 *
 * @author Chetan Narsude <chetan@apache.org>
 */
public interface ConnectionAgent
{
  void connect(ClientListener client);
  void disconnect(ClientListener client);

  class SimpleConnectionAgent implements ConnectionAgent
  {
    private final InetSocketAddress address;
    private final EventLoop eventloop;
    public SimpleConnectionAgent(InetSocketAddress address, EventLoop eventLoop)
    {
      this.address = address;
      this.eventloop = eventLoop;
    }

    @Override
    public void connect(ClientListener client)
    {
      eventloop.connect(address, client);
    }

    @Override
    public void disconnect(ClientListener client)
    {
      eventloop.disconnect(client);
    }
  }
}
