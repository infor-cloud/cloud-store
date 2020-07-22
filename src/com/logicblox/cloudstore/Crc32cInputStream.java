/*
  Copyright 2020, Infor Inc.

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

import com.logicblox.cloudstore.guava.*;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * It computes the CRC-32c checksum of the underlying input stream as it is being read, in a
 * streaming way.
 */
class Crc32cInputStream
  extends FilterInputStream {

  private Crc32c _crc;
  private Hasher _crcGuava;

  public Crc32cInputStream(InputStream in)
  {
    super(in);
    _crc = new Crc32c();
    _crcGuava = Hashing.crc32c().newHasher();
  }

  @Override
  public int read()
    throws IOException
  {
    int res = in.read();
    if(res != -1)
    {
      _crc.update(res);
      _crcGuava.putByte((byte) res);
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
      _crc.update(b, 0, count);
      _crcGuava.putBytes(b, 0, count);
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
      _crc.update(b, off, count);
      _crcGuava.putBytes(b, off, count);
    }
    return count;
  }

  /**
   * Returns the value of the checksum.
   *
   * @return the 4-byte array representation of the checksum in network byte order (big endian).
   */
  public byte[] getValueAsBytes() {
    return _crc.getValueAsBytes();
  }

  public byte[] getGuavaValueAsBytes() {
//    return _crcGuava.hash().asBytes();
    long value = _crcGuava.hash().asInt();
    byte[] result = new byte[4];
    for(int i = 3; i >= 0; i--)
    {
      result[i] = (byte) (value & 0xffL);
      value >>= 8;
    }
    return result;

  }

  /**
   * Returns the value of the checksum.
   *
   * @return the long representation of the checksum (high bits set to zero).
   */
  public long getValue() {
    return _crc.getValue();
  }
}
