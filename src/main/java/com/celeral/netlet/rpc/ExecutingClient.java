/*
 * Copyright 2018 Celeral.
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import com.celeral.netlet.rpc.methodserializer.ExternalizableMethodSerializer;
import com.celeral.netlet.util.Throwables;

/**
 *
 * @author Chetan Narsude <chetan@celeral.com>
 */
public class ExecutingClient extends Client<Client.RPC>
{
  public final Bean<Object> bean;
  private final ConcurrentHashMap<Integer, Method> methodMap;
  private final ConcurrentHashMap<Integer, Integer> notifyMap;
  private final MethodSerializer<Object> methodSerializer;

  @SuppressWarnings("unchecked")
  public ExecutingClient(Bean<?> bean, MethodSerializer<?> methodSerializer, Executor executor)
  {
    super(executor);
    this.bean = (Bean<Object>)bean;
    this.methodSerializer = (MethodSerializer<Object>)methodSerializer;
    this.methodMap = new ConcurrentHashMap<>();
    this.notifyMap = new ConcurrentHashMap<>();
  }

  public ExecutingClient(Bean<?> bean, Executor executor)
  {
    this(bean, ExternalizableMethodSerializer.SINGLETON, executor);
  }

  @Override
  @SuppressWarnings("UseSpecificCatch")
  public void onMessage(Client.RPC message)
  {
    Client.RR rr;

    final Object object = bean.get(message.identifier);
    Integer methodId = message.methodId;
    try {
      Method method;
      if (message instanceof Client.ExtendedRPC) {
        method = methodSerializer.fromSerializable(((Client.ExtendedRPC)message).serializableMethod);
        if (method == null) {
          throw Throwables.throwFormatted(NoSuchMethodException.class,
                                          "Missing method {} for identifier {}!",
                                          ((Client.ExtendedRPC)message).serializableMethod, message.identifier);
        }
        else {
          methodMap.put(methodId, method);
          Integer waiters = notifyMap.remove(methodId);
          if (waiters != null) {
            synchronized (waiters) {
              waiters.notifyAll();
            }
          }
        }
      }
      else {
        method = methodMap.get(methodId);
        if (method == null) {
          Integer old = notifyMap.putIfAbsent(methodId, methodId);
          if (old != null) {
            methodId = old;
          }

          synchronized (methodId) {
            /* checking the method again, takes care of a race condition
             * between the time method was not found by this code and
             * another thread put the method and sent the signal.
             */
            while ((method = methodMap.get(methodId)) == null) {
              // arbitrary wait for 1 second; we should make this configurable!
              methodId.wait(1000);
            }
          }

          if (method == null) {
            throw Throwables.throwFormatted(IllegalStateException.class,
                                            "Missing mapping for message {}!",
                                            message);
          }
        }
      }

      rr = new Client.RR(message.id, method.invoke(object, message.args));
    }
    catch (InvocationTargetException ex) {
      rr = new Client.RR(message.id, null, ex.getCause());
    }
    catch (Exception ex) {
      rr = new Client.RR(message.id, null, ex);
    }

    send(rr);
  }

}
