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

import java.util.concurrent.TimeoutException;

/**
 *
 * @author Chetan Narsude <chetan@apache.org>
 */
public interface TimeoutPolicy
{
  long getTimeoutMillis();

  void handleTimeout(ProxyClient proxy, TimeoutException ex) throws TimeoutException;

  class SimpleTimeoutPolicy implements TimeoutPolicy
  {
    private long timeout;

    public SimpleTimeoutPolicy(long millis)
    {
      timeout = millis;
    }

    @Override
    public long getTimeoutMillis()
    {
      return getTimeout();
    }

    @Override
    public void handleTimeout(ProxyClient proxy, TimeoutException ex) throws TimeoutException
    {
      throw ex;
    }

    /**
     * @return the timeout
     */
    public long getTimeout()
    {
      return timeout;
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(long timeout)
    {
      this.timeout = timeout;
    }

  }

  public static final TimeoutPolicy NO_TIMEOUT_POLICY = new SimpleTimeoutPolicy(Long.MAX_VALUE)
  {
    @Override
    public void setTimeout(long timeout)
    {
      throw new UnsupportedOperationException();
    }

  };
}
