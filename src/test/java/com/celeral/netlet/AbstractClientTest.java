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
package com.celeral.netlet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;

import static java.lang.Thread.sleep;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.celeral.netlet.ServerTest.ServerImpl;
import com.celeral.netlet.util.CircularBuffer;
import com.celeral.netlet.util.Slice;

public class AbstractClientTest
{
  public AbstractClientTest()
  {
  }

  @Override
  public String toString()
  {
    return "ClientTest{" + '}';
  }

  public class ClientImpl extends AbstractClient
  {
    public static final int BUFFER_CAPACITY = 8 * 1024 + 1;
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);
    boolean read;

    @Override
    public String toString()
    {
      return "ClientImpl{" + "buffer=" + buffer + ", read=" + read + '}';
    }

    @Override
    public ByteBuffer buffer()
    {
      return buffer;
    }

    @Override
    public void read(int len)
    {
      if (buffer.position() == buffer.capacity()) {
        buffer.flip();
        read = true;

        int i = 0;
        LongBuffer lb = buffer.asLongBuffer();
        while (lb.hasRemaining()) {
          Assert.assertEquals(i++, lb.get());
        }

        assert (i == BUFFER_CAPACITY / 8);
      }
    }

    @Override
    public void connected()
    {
    }

    @Override
    public void disconnected()
    {
    }

    private CircularBuffer<Slice> getSendBuffer4Polls()
    {
      return sendBuffer4Polls;
    }

    private void setSendBuffer4Polls(CircularBuffer<Slice> circularBuffer)
    {
      sendBuffer4Polls = circularBuffer;
    }

    private CircularBuffer<Slice> getSendBuffer4Offers()
    {
      return sendBuffer4Offers;
    }

    private void setSendBuffer4Offers(CircularBuffer<Slice> circularBuffer)
    {
      sendBuffer4Offers = circularBuffer;
    }

    private ByteBuffer getWriteBuffer()
    {
      return writeBuffer;
    }

  }

  public void sendData()
  {
  }

  @SuppressWarnings( {"SleepWhileInLoop", "AssertEqualsBetweenInconvertibleTypes"})
  private void verifySendReceive(final DefaultEventLoop el) throws IOException, InterruptedException
  {
    ServerImpl si = new ServerImpl();
    ClientImpl ci = new ClientImpl();

    new Thread(el).start();

    el.start(new InetSocketAddress("localhost", 0), si);

    SocketAddress address;
    synchronized (si) {
      if ((address = si.getServerAddress()) == null) {
        si.wait();
        address = si.getServerAddress();
      }
    }

    el.connect((InetSocketAddress)address, ci);

    ByteBuffer outboundBuffer = ByteBuffer.allocate(ClientImpl.BUFFER_CAPACITY);
    LongBuffer lb = outboundBuffer.asLongBuffer();

    int i = 0;
    while (lb.hasRemaining()) {
      lb.put(i++);
    }

    boolean odd = false;
    outboundBuffer.position(i * 8);
    while (outboundBuffer.hasRemaining()) {
      outboundBuffer.put((byte)(odd ? 0x55 : 0xaa));
      odd = !odd;
    }

    byte[] array = outboundBuffer.array();
    while (!ci.send(array, 0, array.length)) {
      sleep(5);
    }

    sleep(100);

    el.disconnect(ci);
    el.stop(si);
    el.stop();
    Assert.assertTrue(ci.read);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithDefault() throws IOException, InterruptedException
  {
    verifySendReceive(new DefaultEventLoop("test"));
  }

  @Test
  public void testWithOptimized() throws IOException, InterruptedException
  {
    verifySendReceive(new OptimizedEventLoop("test"));
  }

  @Test
  @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
  public void testCreateEventLoop() throws IOException
  {
    Assert.assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "");
    Assert.assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "false");
    Assert.assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "true");
    Assert.assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "no");
    Assert.assertEquals(OptimizedEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
    System.setProperty(DefaultEventLoop.eventLoopPropertyName, "yes");
    Assert.assertEquals(DefaultEventLoop.class, DefaultEventLoop.createEventLoop("test").getClass());
  }

  @Test
  public void testOneSlice() throws IOException
  {
    ClientImpl ci = new ClientImpl();
    ci.setSendBuffer4Polls(new CircularBufferWrapper(1));
    ci.setSendBuffer4Offers(ci.getSendBuffer4Polls());
    ci.key = new SelectionKey()
    {
      private int interestOps;

      SocketChannel channel = new SocketChannel(null)
      {
        @Override
        public SocketChannel bind(SocketAddress local) throws IOException
        {
          return null;
        }

        @Override
        public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException
        {
          return null;
        }

        @Override
        public SocketChannel shutdownInput() throws IOException
        {
          return null;
        }

        @Override
        public SocketChannel shutdownOutput() throws IOException
        {
          return null;
        }

        @Override
        public Socket socket()
        {
          return null;
        }

        @Override
        public boolean isConnected()
        {
          return false;
        }

        @Override
        public boolean isConnectionPending()
        {
          return false;
        }

        @Override
        public boolean connect(SocketAddress remote) throws IOException
        {
          return false;
        }

        @Override
        public boolean finishConnect() throws IOException
        {
          return false;
        }

        @Override
        public SocketAddress getRemoteAddress() throws IOException
        {
          return null;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
          return 0;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException
        {
          return 0;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
          final int remaining = src.remaining();
          src.position(src.position() + remaining);
          return remaining;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
        {
          return 0;
        }

        @Override
        protected void implCloseSelectableChannel() throws IOException
        {

        }

        @Override
        protected void implConfigureBlocking(boolean block) throws IOException
        {

        }

        @Override
        public SocketAddress getLocalAddress() throws IOException
        {
          return null;
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException
        {
          return null;
        }

        @Override
        public Set<SocketOption<?>> supportedOptions()
        {
          return null;
        }
      };

      @Override
      public SelectableChannel channel() {
        return channel;
      }

      @Override
      public Selector selector()
      {
        return null;
      }

      @Override
      public boolean isValid()
      {
        return true;
      }

      @Override
      public void cancel()
      {

      }

      @Override
      public int interestOps()
      {
        return interestOps;
      }

      @Override
      public SelectionKey interestOps(int ops)
      {
        if ((ops & ~OP_WRITE) != 0)
        {
          throw new IllegalArgumentException();
        }
        interestOps = ops;
        return this;
      }

      @Override
      public int readyOps()
      {
        return OP_WRITE;
      }
    };
    ci.send(new byte[ci.getWriteBuffer().remaining()]);
    ci.write();
  }

  private static class CircularBufferWrapper extends CircularBuffer<Slice>
  {
    private CircularBufferWrapper(int capacity)
    {
      super(capacity);
    }

    @Override
    public Slice pollUnsafe()
    {
      Slice f = super.pollUnsafe();
      Assert.assertTrue("Unexpected slice length: " + f.length, f.length > 0);
      return f;
    }

    @Override
    public Slice peekUnsafe()
    {
      Slice f = super.peekUnsafe();
      Assert.assertTrue("Unexpected slice length: " + f.length, f.length > 0);
      return f;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractClientTest.class);
}
