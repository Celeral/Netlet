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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.celeral.netlet.AbstractServer;
import com.celeral.netlet.DefaultEventLoop;
import com.celeral.netlet.rpc.ConnectionAgent.SimpleConnectionAgent;
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
      logger.debug("greet = Hello World!");
      greeted = true;
      throw new RuntimeException("Hello World!");
    }

    @Override
    public boolean hasGreeted()
    {
      logger.debug("greeted = {}", greeted);
      return greeted;
    }

    private static final Logger logger = LoggerFactory.getLogger(HelloWorld.class);
  }

  public static class Server extends AbstractServer
  {
    private final Executor executor;

    public Server(Executor executor)
    {
      this.executor = executor;
    }

    @Override
    public ClientListener getClientConnection(SocketChannel client, ServerSocketChannel server)
    {
      return new ExecutingClient(new HelloWorldImpl(), new Class<?>[]{HelloWorld.class}, executor);
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
  public void testRPCMultiThreaded() throws IOException, InterruptedException
  {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      testRPC(executor);
    }
    finally {
      executor.shutdown();
    }
  }

  @Test
  public void testRPCSingleThreaded() throws IOException, InterruptedException
  {
    testRPC(new Executor()
    {
      @Override
      public void execute(Runnable command)
      {
        command.run();
      }
    });
  }

  public void testRPC(Executor executor) throws IOException, InterruptedException
  {
    DefaultEventLoop el = DefaultEventLoop.createEventLoop("rpc");
    el.start();
    try {
      Server server = new Server(executor);
      el.start(new InetSocketAddress(0), server);

      SocketAddress si;
      synchronized (server) {
        while ((si = server.getServerAddress()) == null) {
          server.wait();
        }
      }

      try {
        ProxyClient client = new ProxyClient(new SimpleConnectionAgent((InetSocketAddress)si, el),
                                             TimeoutPolicy.NO_TIMEOUT_POLICY,
                                             executor);
        try {
          HelloWorld helloWorld = (HelloWorld)client.create(HelloWorld.class.getClassLoader(), new Class<?>[]{HelloWorld.class});
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
          client.close();
        }
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
