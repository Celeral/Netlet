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

import com.celeral.netlet.rpc.MethodSerializer;

import java.lang.reflect.Method;

/**
 *
 * @author Chetan Narsude <chetan@celeral.com>
 */
public class GenericStringBasedMethodSerializer implements MethodSerializer<String>
{
  private final Class<?>[] interfaces;

  public GenericStringBasedMethodSerializer(Class<?>[] recognizedInterfaces)
  {
    interfaces = recognizedInterfaces;
  }

  @Override
  public String toSerializable(Method method)
  {
    return method.toGenericString();
  }

  @Override
  public Method fromSerializable(String methodGenericstring)
  {
    for (Class<?> intf : interfaces) {
      for (Method m : intf.getMethods()) {
        if (m.toGenericString().equals(methodGenericstring)) {
          return m;
        }
      }
    }
    return null;
  }
  
}
