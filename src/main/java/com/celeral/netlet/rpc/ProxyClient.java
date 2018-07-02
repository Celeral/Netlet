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

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.esotericsoftware.kryo.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.celeral.netlet.rpc.Client.ExtendedRPC;
import com.celeral.netlet.rpc.Client.RPC;
import com.celeral.netlet.rpc.Client.RR;

/**
 * The class is abstract so that we can resolve the type T at runtime.
 *
 * @author Chetan Narsude {@literal <chetan@apache.org>}
 */
public class ProxyClient
{
  private final TimeoutPolicy policy;
  private final ConnectionAgent agent;
  private final Executor executors;
  private final MethodSerializer<Object> methodSerializer;

  /**
   * Future for tracking the asynchronous responses to the RPC call.
   */
  public static class RPCFuture implements Future<Object>
  {
    private final RPC rpc;
    AtomicReference<RR> rr;

    public RPCFuture(RPC rpc, RR rr)
    {
      this.rpc = rpc;
      this.rr = new AtomicReference<RR>(rr);
    }

    public RPCFuture(RPC rpc)
    {
      this(rpc, null);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return false;
    }

    @Override
    public boolean isCancelled()
    {
      return false;
    }

    @Override
    public boolean isDone()
    {
      return rr.get() != null;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException
    {
      RR r = rr.get();
      if (r.exception != null) {
        throw new ExecutionException(r.exception);
      }

      return r.response;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
      if (rr.get() == null) {
        long diff = unit.toMillis(timeout);
        long waitUntil = System.currentTimeMillis() + diff;
        do {
          synchronized (rpc) {
            rpc.wait(diff);
          }

          if (rr.get() != null) {
            break;
          }
        }
        while ((diff = waitUntil - System.currentTimeMillis()) > 0);
      }

      RR r = rr.get();
      if (r == null) {
        throw new TimeoutException();
      }

      if (r.exception != null) {
        throw new ExecutionException(r.exception);
      }

      return r.response;
    }

  }

  public ProxyClient(ConnectionAgent agent, TimeoutPolicy policy, MethodSerializer<?> methodSerializer, Executor executors)
  {
    this.policy = policy;
    this.methodSerializer = (MethodSerializer<Object>)methodSerializer;
    this.agent = agent;
    this.executors = executors;
  }

  public <T> T create(Object identifier, ClassLoader loader, Class<?>[] interfaces)
  {
    // we can pass the interfaces to the InvocationHandlerImpl, and it can make sure that all the interfaces are supported
    return (T)Proxy.newProxyInstance(loader, interfaces, new InvocationHandlerImpl(identifier));
  }

  public <T> T create(Object identifier, Class<T> iface)
  {
    return create(identifier, Thread.currentThread().getContextClassLoader(), new Class<?>[]{iface});
  }

  private class InvocationHandlerImpl implements InvocationHandler, Closeable
  {
    final Object identity;
    final ConcurrentLinkedQueue<RPCFuture> futureResponses;
    DelegatingClient client;

    public InvocationHandlerImpl(Object id)
    {
      identity = id;
      futureResponses = new ConcurrentLinkedQueue<RPCFuture>();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
      do {
        if (client == null) {
          client = new DelegatingClient(futureResponses, methodSerializer, executors);
          for (Map.Entry<Class<?>, Serializer<?>> entry : serializers.entrySet()) {
            client.addDefaultSerializer(entry.getKey(), entry.getValue());
          }
          agent.connect(client);
        }
        else if (!client.isConnected()) {
          agent.connect(client);
        }

        RPCFuture future = new RPCFuture(client.send(identity, method, args));
        futureResponses.add(future);

        try {
          return future.get(policy.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
          policy.handleTimeout(ProxyClient.this, ex);
        }
        catch (ExecutionException ex) {
          throw ex.getCause();
        }
      }
      while (true);
    }

    @Override
    public void close()
    {
      if (client != null) {
        if (client.isConnected()) {
          agent.disconnect(client);
        }
        client = null;
      }
    }

  }

  private LinkedHashMap<Class<?>, Serializer<?>> serializers = new LinkedHashMap<Class<?>, Serializer<?>>();

  public void addDefaultSerializer(Class<?> type, Serializer<?> serializer)
  {
    serializers.put(type, serializer);
  }

  public static class DelegatingClient extends Client<RR>
  {
    Map<Method, Integer> methodMap;
    Map<Object, Integer> identityMap;

    private final ConcurrentLinkedQueue<RPCFuture> futureResponses;
    private final MethodSerializer<Object> methodSerializer;

    DelegatingClient(ConcurrentLinkedQueue<RPCFuture> futureResponses, MethodSerializer<Object> methodSerializer, Executor executors)
    {
      super(executors);
      this.futureResponses = futureResponses;
      this.methodSerializer = methodSerializer;
      methodMap = Collections.synchronizedMap(new WeakHashMap<Method, Integer>());
      identityMap = Collections.synchronizedMap(new WeakHashMap<Object, Integer>());
    }

    @Override
    public void onMessage(RR rr)
    {
      Iterator<RPCFuture> iterator = futureResponses.iterator();
      while (iterator.hasNext()) {
        RPCFuture next = iterator.next();
        int id = next.rpc.id;
        if (id == rr.id) {
          next.rr.set(rr);
          synchronized (next.rpc) {
            next.rpc.notifyAll();
          }
          iterator.remove();
          break;
        }
      }
    }

    static final AtomicInteger methodIdGenerator = new AtomicInteger();

    public RPC send(Object identifier, Method method, Object[] args)
    {
      RPC rpc;

      Integer i = methodMap.get(method);
      if (i == null) {
        int id = methodIdGenerator.incrementAndGet();
        methodMap.put(method, id);
        rpc = new ExtendedRPC(methodSerializer.toSerializable(method), id, identifier, args);
      }
      else {
        rpc = new RPC(i, identifier, args);
      }

      send(rpc);
      return rpc;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(ProxyClient.class);
}
