/*
  Copyright 2018, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.cloudstore;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

class Crc32cInputStream
  extends FilterInputStream {

  private Crc32c crc;

  public Crc32cInputStream(InputStream in)
  {
    super(in);
    crc = new Crc32c();
  }

  @Override
  public int read()
    throws IOException
  {
    int res = in.read();
    if(res != -1)
    {
      crc.update(res);
    }
    return res;
  }

  @Override
  public int read(byte[] b)
    throws IOException
  {
    int count = in.read(b);
    if(count != -1)
    {
      crc.update(b, 0, count);
    }
    return count;
  }

  @Override
  public int read(byte[] b, int off, int len)
    throws IOException
  {
    int count = in.read(b, off, len);
    if(count != -1)
    {
      crc.update(b, off, count);
    }
    return count;
  }

  /**
   * Returns the value of the checksum.
   *
   * @return the 4-byte array representation of the checksum in network byte order (big endian).
   */
  public byte[] getValueAsBytes() {
    return crc.getValueAsBytes();
  }

  /**
   * Returns the value of the checksum.
   *
   * @return the long representation of the checksum (high bits set to zero).
   */
  public long getValue() {
    return crc.getValue();
  }
}
