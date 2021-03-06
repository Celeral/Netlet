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

import java.net.SocketAddress;

import com.celeral.netlet.Listener.ClientListener;
import com.celeral.netlet.Listener.ServerListener;

/**
 * EventLoop interface.
 *
 * @since 1.0.0
 */
//Convert the return types to future.
public interface EventLoop
{
  void connect(SocketAddress address, ClientListener l);

  void disconnect(ClientListener l);

  void start(SocketAddress address, ServerListener l);

  void stop(ServerListener l);

  void submit(Runnable r);

}
