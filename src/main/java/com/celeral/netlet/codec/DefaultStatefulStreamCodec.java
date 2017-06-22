/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.celeral.netlet.codec;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;

import com.celeral.netlet.util.DTThrowable;
import com.celeral.netlet.util.Slice;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the StreamCodec.
 *
 * This implementation is used no codec is partitioned on the input streams of the operator.
 * It uses Kryo to serialize and deserialize the tuples and uses the hashcode of the tuple
 * object to faciliate the partitions.
 *
 * Requires kryo and its dependencies in deployment
 *
 * @param <T>
 * @since 0.3.2
 */
public class DefaultStatefulStreamCodec<T> extends Kryo implements StatefulStreamCodec<T>
{
  public static enum MessageType
  {
    DATA((byte)0),
    STATE((byte)1);

    public final byte getByte()
    {
      return value;
    }

    public static MessageType valueOf(byte value)
    {
      switch (value) {
        case 0:
          return DATA;
        case 1:
          return STATE;
        default:
          return null;
      }
    }

    private final byte value;

    MessageType(byte value)
    {
      this.value = value;
    }
  }

  private final Output data;
  private final Output state;
  private final Input input;

  @SuppressWarnings("OverridableMethodCallInConstructor")
  public DefaultStatefulStreamCodec()
  {
    super(new ClassResolver(), new MapReferenceResolver());
    data = new Output(4096, Integer.MAX_VALUE);
    state = new Output(4096, Integer.MAX_VALUE);
    input = new Input();

    register(Class.class);
    register(ClassIdPair.class);
    classResolver = (ClassResolver)getClassResolver();
    this.pairs = classResolver.pairs;
    classResolver.init();
  }

  @Override
  @SuppressWarnings("UseSpecificCatch")
  public Object fromDataStatePair(DataStatePair dspair)
  {
    if (dspair.state != null) {
      try {
        input.setBuffer(dspair.state.buffer, dspair.state.offset, dspair.state.length);

        /* read the first byte which is just MessageType.STATE.value */
        if (input.readByte() != MessageType.STATE.value) {
          logger.warn("Rewinding buffer as the magic value at the head of the state buffer is missing: {}", dspair.state);
          input.rewind();
        }

        while (input.position() < input.limit()) {
          ClassIdPair pair = (ClassIdPair)readClassAndObject(input);
          classResolver.registerExplicit(pair);
        }
      }
      catch (Exception ex) {
        throw writeDebuggingData(dspair, ex);
      }
      finally {
        dspair.state = null;
      }
    }

    input.setBuffer(dspair.data.buffer, dspair.data.offset, dspair.data.length);

    /* read the first byte which is just MessageType.DATA.value */
    if (input.readByte() != MessageType.DATA.value) {
      logger.warn("Rewinding buffer as the magic value at the head of the data buffer is missing: {}", dspair.data);
      input.rewind();
    }

    try {
      return readClassAndObject(input);
    }
    catch (Exception ex) {
      throw writeDebuggingData(dspair, ex);
    }
  }

  @SuppressWarnings("UseSpecificCatch")
  private RuntimeException writeDebuggingData(DataStatePair dspair, Exception ex)
  {
    /**
     * write the erroneous tuple to the disk in a hope that it will aid in debugging.
     */
    try {
      File f = File.createTempFile("data", ".bin");
      logger.info("Writing the erroneous data to {}", f);
      Class<?> klass = Class.forName("com.google.common.io.Files");
      Method method = klass.getMethod("write", byte[].class, File.class);
      method.invoke(null, Arrays.copyOfRange(dspair.data.buffer, dspair.data.offset, dspair.data.offset + dspair.data.length), f);
    }
    catch (Exception ex1) {
      logger.info("Cascading issue while writing debugging data to address original exception", ex1);
    }

    throw DTThrowable.wrapIfChecked(ex);
  }

  @Override
  public DataStatePair toDataStatePair(T o)
  {
    DataStatePair pair = new DataStatePair();
    data.setPosition(0);
    /* code the first byte to signify that we have written data */
    data.writeByte(MessageType.DATA.value);

    writeClassAndObject(data, o);

    if (!pairs.isEmpty()) {
      state.setPosition(0);
      /* code the first byte to signify that we have written state */
      state.writeByte(MessageType.STATE.value);

      for (ClassIdPair cip : pairs) {
        writeClassAndObject(state, cip);
      }
      pairs.clear();

      byte[] bytes = state.toBytes();
      pair.state = new Slice(bytes, 0, bytes.length);
    }

    byte[] bytes = data.toBytes();
    pair.data = new Slice(bytes, 0, bytes.length);
    return pair;
  }

  @Override
  public void resetState()
  {
    classResolver.unregisterImplicitlyRegisteredTypes();
  }

  final ClassResolver classResolver;
  final ArrayList<ClassIdPair> pairs;

  static class ClassIdPair
  {
    final int id;
    final String classname;

    ClassIdPair()
    {
      id = 0;
      classname = null;
    }

    ClassIdPair(int id, String classname)
    {
      this.id = id;
      this.classname = classname;
    }

  }

  public static class ClassResolver extends DefaultClassResolver
  {
    int firstAvailableRegistrationId;
    int nextAvailableRegistrationId;
    final ArrayList<ClassIdPair> pairs = new ArrayList<ClassIdPair>();

    public void unregister(int classId)
    {
      Registration registration = idToRegistration.remove(classId);
      classToRegistration.remove(registration.getType());
      getRegistration(int.class); /* make sure that we bust the memoized cache in superclass */
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Registration registerImplicit(Class type)
    {
      while (getRegistration(nextAvailableRegistrationId) != null) {
        nextAvailableRegistrationId++;
      }

      //logger.debug("adding new classid pair {} => {}", nextAvailableRegistrationId, type.getName());
      pairs.add(new ClassIdPair(nextAvailableRegistrationId, type.getName()));
      return register(new Registration(type, kryo.getDefaultSerializer(type), nextAvailableRegistrationId++));
    }

    void registerExplicit(ClassIdPair pair) throws ClassNotFoundException
    {
      //logger.debug("registering class {} => {}", pair.classname, pair.id);
      //pairs.add(pair);
      Class<?> type = Class.forName(pair.classname, false, Thread.currentThread().getContextClassLoader());
      register(new Registration(type, kryo.getDefaultSerializer(type), pair.id));
      if (nextAvailableRegistrationId <= pair.id) {
        nextAvailableRegistrationId = pair.id + 1;
      }
    }

    public void init()
    {
      firstAvailableRegistrationId = kryo.getNextRegistrationId();
      nextAvailableRegistrationId = firstAvailableRegistrationId;
    }

    public void unregisterImplicitlyRegisteredTypes()
    {
      while (nextAvailableRegistrationId > firstAvailableRegistrationId) {
        unregister(--nextAvailableRegistrationId);
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(DefaultStatefulStreamCodec.class);
}
