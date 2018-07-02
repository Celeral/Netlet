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
package com.celeral.netlet.rpc.methodserializer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.ExternalizableSerializer;

import com.celeral.netlet.rpc.MethodSerializer;
import com.celeral.netlet.util.Throwables;

/**
 *
 * @author Chetan Narsude <chetan@celeral.com>
 */
public class ExternalizableMethodSerializer implements MethodSerializer<ExternalizableMethodSerializer.ExternalizableMethod>
{
  public static final MethodSerializer SINGLETON = new ExternalizableMethodSerializer();

  @Override
  public ExternalizableMethod toSerializable(Method method)
  {
    return new ExternalizableMethod(method);
  }

  @Override
  public Method fromSerializable(ExternalizableMethod serializable)
  {
    return serializable.getMethod();
  }

  @DefaultSerializer(value = ExternalizableSerializer.class)
  public static class ExternalizableMethod implements Externalizable
  {
    private static final long serialVersionUID = 6631604036553063657L;
    private Method method;

    public ExternalizableMethod(Method method)
    {
      super();
      this.method = method;
    }

    private ExternalizableMethod()
    {
      super();
    }

    public Method getMethod()
    {
      return method;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
      out.writeObject(method.getDeclaringClass());
      out.writeUTF(method.getName());
      out.writeObject(method.getParameterTypes());
    }

    @Override
    @SuppressWarnings("UseSpecificCatch")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
      Class<?> declaringClass = (Class<?>)in.readObject();
      String methodName = in.readUTF();
      Class<?>[] parameterTypes = (Class<?>[])in.readObject();
      try {
        method = declaringClass.getMethod(methodName, parameterTypes);
      }
      catch (Exception e) {
        throw Throwables.throwFormatted(e, IOException.class, "Error occurred resolving deserialized method '{}.{}'", declaringClass.getSimpleName(), methodName);
      }
    }
  }

}
