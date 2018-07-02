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

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.celeral.netlet.AbstractLengthPrependerClient;
import com.celeral.netlet.codec.DefaultStatefulStreamCodec;
import com.celeral.netlet.codec.StatefulStreamCodec.DataStatePair;
import com.celeral.netlet.util.Slice;

/**
 *
 * @author Chetan Narsude {@literal <chetan@apache.org>}
 *
 * @param <T> - Type of the object that's received by the client.
 */
public abstract class Client<T> extends AbstractLengthPrependerClient
{
  Slice state;
  private final DefaultStatefulStreamCodec<Object> serdes;
  private transient Executor executors;

  private static class DirectExecutor implements Executor
  {
    @Override
    public void execute(Runnable command)
    {
      command.run();
    }
  }

  private Client()
  {
    this(new DirectExecutor());
  }

  public Client(Executor executors)
  {
    this.executors = executors;

    serdes = new DefaultStatefulStreamCodec<Object>();
    /* setup the classes that we know about before hand */
    serdes.register(Ack.class);
    serdes.register(RPC.class);
    serdes.register(ExtendedRPC.class);
    serdes.register(RR.class);
  }

  public void addDefaultSerializer(Class<?> type, Serializer<?> serializer)
  {
    serdes.register(type, serializer);
  }

  public abstract void onMessage(T message);

  class Sender implements Runnable
  {
    Object object;

    Sender(Object object)
    {
      this.object = object;
    }

    @Override
    public void run()
    {
      DataStatePair pair;
      synchronized (serdes) {
        pair = serdes.toDataStatePair(object);
      }
      writeObject(pair);
    }
  }

  protected void send(final Object object)
  {
    executors.execute(new Sender(object));
  }

  private synchronized void writeObject(DataStatePair pair)
  {
    if (pair.state != null) {
      logger.trace("sending state = {}", pair.state);
      write(pair.state.buffer, pair.state.offset, pair.state.length);
    }

    logger.trace("sending data = {}", pair.data);
    write(pair.data.buffer, pair.data.offset, pair.data.length);
  }

  class Receiver implements Runnable
  {
    DataStatePair pair;

    Receiver(DataStatePair pair)
    {
      this.pair = pair;
    }

    @Override
    public void run()
    {
      Object object;
      synchronized (serdes) {
        object = serdes.fromDataStatePair(pair);
      }
      onMessage((T)object);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void onMessage(byte[] buffer, int offset, int size)
  {
    if (size > 0) {
      if (buffer[offset] == DefaultStatefulStreamCodec.MessageType.STATE.getByte()) {
        state = new Slice(buffer, offset, size);
        logger.trace("Idenfied state = {}", state);
      }
      else {
        final DataStatePair pair = new DataStatePair();
        pair.state = state;
        state = null;
        pair.data = new Slice(buffer, offset, size);
        logger.trace("Identified data = {}", pair.data);
        executors.execute(new Receiver(pair));
      }
    }
  }

  public static class Ack
  {
    protected static final AtomicInteger counter = new AtomicInteger();
    /**
     * Unique Identifier for the calls and responses.
     * Receiving object with this type is sufficient to establish successful delivery
     * of corresponding call or response from the other end.
     */
    protected int id;

    public Ack(int id)
    {
      this.id = id;
    }

    public Ack()
    {
      /* for serialization */
    }

    public int getId()
    {
      return id;
    }

    @Override
    public String toString()
    {
      return "Ack{" + "id=" + id + '}';
    }

    @Override
    public int hashCode()
    {
      return id;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Ack other = (Ack)obj;
      return this.id == other.id;
    }

  }

  /**
   * Compact method to communicate which method to be called with the arguments.
   * Before this method is understood by the other party, it needs to receive
   * ExtendedRPC which communicates the mapping of methodId with the method.
   */
  public static class RPC extends Ack
  {
    int methodId;
    Object identifier;
    Object[] args;

    protected RPC()
    {
      /* for serialization */
    }

    public RPC(int methodId, Object identifier, Object[] args)
    {
      this(counter.incrementAndGet(), methodId, identifier, args);
    }

    public RPC(int id, int methodId, Object identifier, Object[] args)
    {
      super(id);
      this.methodId = methodId;
      this.identifier = identifier;
      this.args = args;
    }

    @Override
    public String toString()
    {
      return "RPC{" + "methodId=" + methodId + ", identifier=" + identifier.toString() + ", args=" + Arrays.toString(args) + '}' + super.toString();
    }

  }

  /**
   * The first time a method is invoked by the client, this structure will be
   * sent to the remote end.
   */
  public static class ExtendedRPC extends RPC
  {
    public Object serializableMethod;

    protected ExtendedRPC()
    {
      /* for serialization */
    }

    public ExtendedRPC(int id, Object method, int methodId, Object identifier, Object[] args)
    {
      super(id, methodId, identifier, args);
      serializableMethod = method;
    }

    public ExtendedRPC(Object method, int methodId, Object identifier, Object[] args)
    {
      this(counter.incrementAndGet(), method, methodId, identifier, args);
    }

    @Override
    public String toString()
    {
      return "ExtendedRPC@" + System.identityHashCode(this) + '{' + "methodGenericstring=" + serializableMethod + '}' + super.toString();
    }

  }

  public static class RR extends Ack
  {
    Object response;

    @Bind(JavaSerializer.class)
    Throwable exception;

    protected RR()
    {
      /* for serialization */
    }

    public RR(int id, Object response, Throwable exception)
    {
      super(id);
      this.response = response;
      this.exception = exception;
    }

    public RR(int id, Object response)
    {
      this(id, response, null);
    }

    @Override
    public String toString()
    {
      return "RR{" + "exception=" + exception + ", response=" + response + '}' + super.toString();
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(Client.class);
}
