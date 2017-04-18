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
package org.getopt.util.hash;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MurmurHashTest
{
  static int NUM = 1000;


  @Test
  public void testHash()
  {
    byte[] bytes = new byte[4];
    for (int i = 0; i < NUM; i++) {
      bytes[0] = (byte)(i & 0xff);
      bytes[1] = (byte)((i & 0xff00) >> 8);
      bytes[2] = (byte)((i & 0xff0000) >> 16);
      bytes[3] = (byte)((i & 0xff000000) >> 24);
      logger.debug(Integer.toHexString(i) + " " + Integer.toHexString(MurmurHash.hash(bytes, 1)));
      // do some kind of test here!
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(MurmurHashTest.class);
}
