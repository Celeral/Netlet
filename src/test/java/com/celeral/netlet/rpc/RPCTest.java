/*
 * Copyright 2017 Celeral <netlet@celeral.com>.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.celeral.netlet.AbstractServer;
import com.celeral.netlet.DefaultEventLoop;
import com.celeral.netlet.rpc.ProxyClient.ExecutingClient;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude  <chetan@apache.org>
 */
public class RPCTest
{

  public static interface HelloWorld
  {
    void greet();
    boolean hasGreeted();
  }

  public static class HelloWorldImpl implements HelloWorld
  {
    boolean greeted;

    @Override
    public void greet()
    {
      logger.debug("Hello World!");
      greeted = true;
      throw new RuntimeException("Hello World!");
    }

    @Override
    public boolean hasGreeted()
    {
      return greeted;
    }

    private static final Logger logger = LoggerFactory.getLogger(HelloWorld.class);
  }

  public static class Server extends AbstractServer
  {
    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      return new ExecutingClient(new HelloWorldImpl(), new Class<?>[]{HelloWorld.class});
    }

    @Override
    public void registered(SelectionKey key)
    {
      super.registered(key);
      synchronized (this) {
        notify();
      }
    }

  }


  @Test
  public void testRPC() throws IOException, InterruptedException
  {
    DefaultEventLoop el = DefaultEventLoop.createEventLoop("rpc");
    el.start();
    try {
      Server server = new Server();
      el.start(new InetSocketAddress(0), server);

      SocketAddress si;
      synchronized (server) {
        while ((si = server.getServerAddress()) == null) {
          server.wait();
        }
      }

      try {
        ProxyClient client = new ProxyClient((InetSocketAddress)si, el);
        HelloWorld helloWorld = (HelloWorld)client.newProxyInstance(HelloWorld.class.getClassLoader(), new Class<?>[]{HelloWorld.class});
        Assert.assertFalse("Before Greeted!", helloWorld.hasGreeted());

        try {
          helloWorld.greet();
        }
        catch (RuntimeException ex) {
          Assert.assertEquals("Hello World!", ex.getMessage());
        }

        Assert.assertTrue("After Greeted!", helloWorld.hasGreeted());
      }
      finally {
        el.stop(server);
      }
    }
    finally {
      el.stop();
    }
  }
}
