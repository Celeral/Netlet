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
package com.celeral.netlet;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 *
 * @author Chetan Narsude  <chetan@apache.org>
 */
public class ProtocolSpecificAddress extends InetSocketAddress
{
  public final Protocol protocol;

  /**
   * Protocols which are supported by this library. AUTO protocol indicates
   * that based on the listener and InetSocketAddress characteristics, the
   * library will use the most optimal protocol. e.g. if the class D address
   * is used, the library will automatically use the multicast protocol. If
   * other protocols are used but the address/port combination is not suitable
   * the library will give an error.
   */
  public enum Protocol
  {
    AUTO,
    TCP,
    UDP,
    MULTICAST
  }

  public ProtocolSpecificAddress(String host, int port, Protocol protocol)
  {
    super(host, port);
    this.protocol = protocol;
  }

  public ProtocolSpecificAddress(InetAddress host, int port, Protocol protocol)
  {
    super(host, port);
    this.protocol = protocol;
  }

  public ProtocolSpecificAddress(int port, Protocol protocol)
  {
    super(port);
    this.protocol = protocol;
  }

  private static final long serialVersionUID = 201601182220L;
}
