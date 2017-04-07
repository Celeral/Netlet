/*
 * Copyright 2017 Chetan Narsude  <chetan@apache.org>.
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
package com.datatorrent.netlet.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.rpc.Client.ExtendedRPC;
import com.datatorrent.netlet.rpc.Client.RPC;
import com.datatorrent.netlet.rpc.Client.RR;
import java.lang.reflect.InvocationTargetException;

/**
 * The class is abstract so that we can resolve the type T at runtime.
 *
 * @author Chetan Narsude  <chetan@apache.org>
 */
public class ProxyClient implements InvocationHandler
{
  InetSocketAddress address;
  EventLoop eventLoop;
  DelegatingClient client;

  ConcurrentLinkedQueue<RPCFuture> futureResponses = new ConcurrentLinkedQueue<RPCFuture>();

  /**
   * Future for tracking the asynchronous responses to the RPC call.
   */
  public static class RPCFuture implements Future<Object>
  {
    RPC rpc;
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
        long expiryMillis = System.currentTimeMillis() + unit.toMillis(timeout);

        RPC r = rpc;
        do {
          long waitMillis = expiryMillis - System.currentTimeMillis();
          if (waitMillis > 0) {
            synchronized (r) {
              r.wait(waitMillis);
            }
          }
        }
        while (rr.get() == null);
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

  ProxyClient(InetSocketAddress address, EventLoop loop)
  {
    this.address = address;
    eventLoop = loop;
  }

  public Object newProxyInstance(ClassLoader loader, Class<?>[] interfaces)
  {
    // is there a value in making sure that the passed interfaces contain the type T
//    TypeVariable<? extends Class<?>>[] typeParameters = ProxyClient.class.getTypeParameters();
//    if (typeParameters != null) {
//      boolean typefound = false;
//      typeloop:
//      for (TypeVariable<? extends Class<?>> parameter: typeParameters) {
//        for (Class<?> iface: interfaces) {
//          if (iface.equals(parameter.getGenericDeclaration())) {
//            typefound = true;
//            break typeloop;
//          }
//        }
//      }
//
//      if (!typefound) {
//        logger.error("interface mismatch generics = {} and interfaces = {}", typeParameters, interfaces);
//        throw new IllegalArgumentException("// Interface mismatch - refer to the debug statements");
//      }
//    }

    client = new DelegatingClient(futureResponses);
    return Proxy.newProxyInstance(loader, interfaces, this);
  }

  // whenenver method on the proxy instance is called, this method gets called.
  // it's imperial that we are able to serialize all this information and send
  // it over the pipe!
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
  {
    if (!client.isConnected()) {
      eventLoop.connect(address, client);
    }

    RPCFuture future = new RPCFuture(client.send(method, args));
    futureResponses.add(future);
    while (future.isDone() == false) {
      synchronized (future) {
        future.wait();
      }
    }

    RR rr = future.rr.get();
    if (rr.exception != null) {
      throw rr.exception;
    }

    return rr.response;
  }

  public static class DelegatingClient extends Client<RR>
  {
    HashMap<Method, Integer> methodMap;
    private final ConcurrentLinkedQueue<RPCFuture> futureResponses;

    DelegatingClient(ConcurrentLinkedQueue<RPCFuture> futureResponses)
    {
      super();
      this.futureResponses = futureResponses;
      methodMap = new HashMap<Method, Integer>();
    }

    @Override
    public void onMessage(RR rr)
    {
      logger.trace("rr = {}", rr);
      Iterator<RPCFuture> iterator = futureResponses.iterator();
      while (iterator.hasNext()) {
        RPCFuture next = iterator.next();
        int id = next.rpc.id;
        if (id == rr.id) {
          next.rr.set(rr);
          synchronized (next) {
            next.notifyAll();
          }
          iterator.remove();
          break;
        }
      }
    }

    public RPC send(Method method, Object[] args)
    {
      RPC rpc;
      Integer i = methodMap.get(method);
      if (i == null) {
        int id = methodMap.size() + 1;
        methodMap.put(method, id);
        rpc = new ExtendedRPC(method.toGenericString(), id, args);
      }
      else {
        rpc = new RPC(i, args);
      }

      logger.trace("sending rpc = {}", rpc);

      send(rpc);
      return rpc;
    }
  }


  public static class ExecutingClient extends Client<RPC>
  {
    HashMap<Integer, Method> methodMap;
    public final Object executor;
    private final Class<?>[] interfaces;

    public ExecutingClient(Object executor, Class<?>[] interfaces)
    {
      super();
      this.executor = executor;
      this.interfaces = interfaces;
      this.methodMap = new HashMap<Integer, Method>();
    }

    @Override
    @SuppressWarnings("UseSpecificCatch")
    public void onMessage(RPC message)
    {
      RR rr;

      try {
        Method method;
        if (message instanceof ExtendedRPC) {
          method = null;

          /* go for a linear search */
          String methodGenericstring = ((ExtendedRPC)message).methodGenericstring;

          for (Class<?> intf : interfaces) {
            for (Method m: intf.getMethods()) {
              logger.trace("genericString = {}", m.toGenericString());
              if (methodGenericstring.equals(m.toGenericString())) {
                methodMap.put(message.methodId, m);
                method = m;
                break;
              }
            }
          }

          if (method == null) {
            throw new NoSuchMethodException("Missing method for " + message);
          }
        }
        else {
          method = methodMap.get(message.methodId);

          if (method == null) {
            throw new IllegalStateException("Missing mapping for " + message);
          }
        }

        rr = new RR(message.id, method.invoke(executor, message.args));
      }
      catch (InvocationTargetException ex) {
        rr = new RR(message.id, null, ex.getCause());
      }
      catch (Exception ex) {
        rr = new RR(message.id, null, ex);
      }

      send(rr);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ProxyClient.class);
}
