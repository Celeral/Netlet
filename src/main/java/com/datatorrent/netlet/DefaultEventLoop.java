/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.netlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.Listener.ClientListener;
import com.datatorrent.netlet.Listener.ServerListener;
import com.datatorrent.netlet.util.CircularBuffer;

/**
 * <p>DefaultEventLoop class.</p>
 *
 * @since 1.0.0
 */
public class DefaultEventLoop implements Runnable, EventLoop
{
  public final String id;
  private boolean alive;
  private int refCount;
  private final Selector selector;
  private Thread eventThread;
  private final CircularBuffer<Runnable> tasks = new CircularBuffer<Runnable>(1024, 5);

  public DefaultEventLoop(String id) throws IOException
  {
    this.id = id;
    selector = Selector.open();
  }

  public synchronized void start()
  {
    if (++refCount == 1) {
      new Thread(this, id).start();
    }
  }

  public void stop()
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        synchronized (DefaultEventLoop.this) {
          if (--refCount == 0) {
            alive = false;
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("stop{%d}", refCount);
      }

    });
  }

  @Override
  public void run()
  {
    try {
      runEventLoop();
    }
    finally {
      if (alive == true) {
        alive = false;
        logger.warn("Unexpected termination of {}", this);
      }
    }
  }

  @SuppressWarnings({"SleepWhileInLoop", "ConstantConditions"})
  private void runEventLoop()
  {
    //logger.debug("Starting {}", this);
    alive = true;
    eventThread = Thread.currentThread();
    boolean wait = true;

    SelectionKey sk = null;
    Set<SelectionKey> selectedKeys = null;
    Iterator<SelectionKey> iterator = null;

    do {
      try {
        do {
          if (wait) {
            if (selector.selectNow() > 0) {
              selectedKeys = selector.selectedKeys();
              iterator = selectedKeys.iterator();
            }
            else {
              iterator = null;
            }
          }

          if (iterator != null) {
            wait = false;

            while (iterator.hasNext()) {
              if (!(sk = iterator.next()).isValid()) {
                continue;
              }

              ClientListener l;
              switch (sk.readyOps()) {
                case SelectionKey.OP_ACCEPT:
                  ServerSocketChannel ssc = (ServerSocketChannel)sk.channel();
                  SocketChannel sc = ssc.accept();
                  sc.configureBlocking(false);
                  ServerListener sl = (ServerListener)sk.attachment();
                  l = sl.getClientConnection(sc, (ServerSocketChannel)sk.channel());
                  register(sc, SelectionKey.OP_READ | SelectionKey.OP_WRITE, l);
                  break;

                case SelectionKey.OP_CONNECT:
                  if (((SocketChannel)sk.channel()).finishConnect()) {
                    ((ClientListener)sk.attachment()).connected();
                  }
                  break;

                case SelectionKey.OP_READ:
                  ((ClientListener)sk.attachment()).read();
                  break;

                case SelectionKey.OP_WRITE:
                  ((ClientListener)sk.attachment()).write();
                  break;

                case SelectionKey.OP_READ | SelectionKey.OP_WRITE:
                  (l = (ClientListener)sk.attachment()).write();
                  l.read();
                  break;

                case SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT:
                case SelectionKey.OP_READ | SelectionKey.OP_CONNECT:
                case SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT:
                  if (((SocketChannel)sk.channel()).finishConnect()) {
                    ((ClientListener)sk.attachment()).connected();
                    if (sk.isWritable()) {
                      ((ClientListener)sk.attachment()).write();
                    }
                    if (sk.isReadable()) {
                      ((ClientListener)sk.attachment()).read();
                    }
                  }
                  break;

                default:
                  logger.warn("!!!!!! not sure what interest this is {} !!!!!!", Integer.toBinaryString(sk.readyOps()));
                  break;
              }
            }

            selectedKeys.clear();
          }

          int size = tasks.size();
          if (size > 0) {
            wait = false;

            do {
              Runnable task = tasks.pollUnsafe();
              //logger.debug("{}.run{{}}", task, this);
              task.run();
            }
            while (--size > 0);
          }

          if (wait) {
            Thread.sleep(5);
          }
          else {
            wait = true;
          }
        }
        while (alive);
      }
      catch (InterruptedException ie) {
        throw new RuntimeException("Interrupted!", ie);
      }
      catch (Exception io) {
        if (sk == null) {
          logger.warn("Unexpected exception not related to SelectionKey", io);
        }
        else {
          logger.warn("Exception on unregistered SelectionKey {}", sk, io);
          Listener l = (Listener)sk.attachment();
          if (l != null) {
            l.handleException(io, this);
          }
        }

        if (selectedKeys.isEmpty()) {
          //logger.debug("idle {}", this);
          wait = true;
        }
      }
    }
    while (alive);
    //logger.debug("Terminated {}", this);
  }

  @Override
  public void submit(Runnable r)
  {
    Thread currentThread = Thread.currentThread();
    //logger.debug("{}.{}.{}", currentThread, r, eventThread);
    if (tasks.isEmpty() && eventThread == currentThread) {
      r.run();
    }
    else {
      synchronized (tasks) {
        tasks.add(r);
      }
    }
  }

  private void register(final SelectableChannel c, final int ops, final Listener l)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          l.registered(c.register(selector, ops, l));
        }
        catch (ClosedChannelException cce) {
          l.handleException(cce, DefaultEventLoop.this);
        }
      }

      @Override
      public String toString()
      {
        return String.format("register(%s, %d, %s)", c, ops, l);
      }

    });
  }

  //@Override
  public void unregister(final SelectableChannel c)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (SelectionKey key : selector.keys()) {
          if (key.channel() == c) {
            ((Listener)key.attachment()).unregistered(key);
            key.interestOps(0);
            key.attach(Listener.NOOP_LISTENER);
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("unregister(%s)", c);
      }

    });
  }

  //@Override
  public void register(ServerSocketChannel channel, Listener l)
  {
    register(channel, SelectionKey.OP_ACCEPT, l);
  }

  //@Override
  public void register(SocketChannel channel, int ops, Listener l)
  {
    register((AbstractSelectableChannel)channel, ops, l);
  }

  @Override
  public final void connect(final InetSocketAddress address, final ClientListener l)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        SocketChannel channel = null;
        try {
          channel = SocketChannel.open();
          channel.configureBlocking(false);
          if (channel.connect(address)) {
            l.connected();
            register(channel, SelectionKey.OP_READ, l);
          }
          else {
            /*
             * According to the spec SelectionKey.OP_READ is not necessary here, but without it
             * occasionally channel key will not be selected after connection is established and finishConnect()
             * will return true.
             */
            register(channel, SelectionKey.OP_CONNECT | SelectionKey.OP_READ, new ClientListener()
            {
              private SelectionKey key;

              @Override
              public void read() throws IOException
              {
                logger.debug("missing OP_CONNECT {}", l);
                connected();
                l.read();
              }

              @Override
              public void write() throws IOException
              {
                logger.debug("missing OP_CONNECT {}", l);
                connected();
                l.write();
              }

              @Override
              public void connected()
              {
                logger.debug("{}->{}", this, l);
                key.attach(l);
                l.connected();
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
              }

              @Override
              public void disconnected()
              {
                /*
                 * Expectation is that connected() or read() will be called and this ClientListener will be replaced
                 * by the original ClientListener in the key attachment before disconnect is initiated. In any case
                 * as original Client Listener was never attached to the key, this method will never be called. Please
                 * see DefaultEventLoop.disconnect().
                 */
                logger.debug("missing OP_CONNECT {}", l);
                throw new NotYetConnectedException();
              }

              @Override
              public void handleException(Exception exception, EventLoop eventloop)
              {
                key.attach(l);
                l.handleException(exception, eventloop);
              }

              @Override
              public void registered(SelectionKey key)
              {
                l.registered(this.key = key);
              }

              @Override
              public void unregistered(SelectionKey key)
              {
                l.unregistered(key);
              }

              @Override
              public String toString()
              {
                return "Pre-connect Client listener for " + l.toString();
              }

            });
          }
        }
        catch (IOException ie) {
          l.handleException(ie, DefaultEventLoop.this);
          if (channel != null && channel.isOpen()) {
            try {
              channel.close();
            }
            catch (IOException io) {
              l.handleException(io, DefaultEventLoop.this);
            }
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("connect(%s, %s)", address, l);
      }

    });
  }

  @Override
  public final void disconnect(final ClientListener l)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (SelectionKey key : selector.keys()) {
          if (key.attachment() == l) {
            try {
              l.unregistered(key);
            }
            finally {
              if (key.isValid()) {
                if ((key.interestOps() & SelectionKey.OP_WRITE) != 0) {
                  key.attach(new Listener.DisconnectingListener(key));
                  return;
                }
              }

              try {
                key.attach(Listener.NOOP_CLIENT_LISTENER);
                key.channel().close();
              }
              catch (IOException io) {
                l.handleException(io, DefaultEventLoop.this);
              }
            }
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("disconnect(%s)", l);
      }

    });
  }

  @Override
  public final void start(final String host, final int port, final ServerListener l)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        ServerSocketChannel channel = null;
        try {
          channel = ServerSocketChannel.open();
          channel.configureBlocking(false);
          channel.socket().bind(host == null ? new InetSocketAddress(port) : new InetSocketAddress(host, port), 128);
          register(channel, SelectionKey.OP_ACCEPT, l);
        }
        catch (IOException io) {
          l.handleException(io, DefaultEventLoop.this);
          if (channel != null && channel.isOpen()) {
            try {
              channel.close();
            }
            catch (IOException ie) {
              l.handleException(ie, DefaultEventLoop.this);
            }
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("start(%s, %d, %s)", host, port, l);
      }

    });
  }

  @Override
  public final void stop(final ServerListener l)
  {
    submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (SelectionKey key : selector.keys()) {
          if (key.attachment() == l) {
            if (key.isValid()) {
              l.unregistered(key);
              key.cancel();
            }
            key.attach(Listener.NOOP_LISTENER);
            try {
              key.channel().close();
            }
            catch (IOException io) {
              l.handleException(io, DefaultEventLoop.this);
            }
          }
        }
      }

      @Override
      public String toString()
      {
        return String.format("stop(%s)", l);
      }

    });
  }

  public boolean isActive()
  {
    return eventThread != null && eventThread.isAlive();
  }

  @Override
  public String toString()
  {
    return "{id=" + id + ", " + tasks + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(DefaultEventLoop.class);
}
