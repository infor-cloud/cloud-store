/*
  Copyright 2017, Infor Inc.

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

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;

class CipherWithInlineIVInputStream
  extends FilterInputStream
{
  private int _opmode;
  private int _ivBytesWritten = 0;
  private int _ivLen;
  private byte[] _iv;

  public CipherWithInlineIVInputStream(InputStream in, Cipher cipher, int opmode, Key key)
    throws IOException, InvalidKeyException, InvalidAlgorithmParameterException
  {
    super(in);

    _ivLen = cipher.getBlockSize();

    _opmode = opmode;

    switch(_opmode)
    {
      case Cipher.DECRYPT_MODE:
        _iv = new byte[_ivLen];
        int offset = 0;
        // !!! Should this be in a background thread?
        while(offset < _ivLen)
        {
          int result = in.read(_iv, offset, _ivLen - offset);
          if(result == -1)
          {
            // !!! What should we really do here?
            throw new RuntimeException();
          }
          offset += result;
        }
        cipher.init(_opmode, key, new IvParameterSpec(_iv));
        _iv = null;
        break;
      case Cipher.ENCRYPT_MODE:
        cipher.init(_opmode, key);
        _iv = cipher.getIV();
        break;
      default:
        throw new IllegalArgumentException(CipherWithInlineIVInputStream.class.getCanonicalName() +
          " can only be constructed in DECRYPT_MODE or ENCRYPT_MODE");
    }
    this.in = new CipherInputStream(this.in, cipher);
  }

  @Override
  public int available()
    throws IOException
  {
    if(_opmode == Cipher.ENCRYPT_MODE && _ivBytesWritten < _ivLen)
    {
      return _ivLen - _ivBytesWritten;
    }
    return in.available();
  }

  @Override
  public int read()
    throws IOException
  {
    if(_opmode == Cipher.ENCRYPT_MODE && _ivBytesWritten < _ivLen)
    {
      _ivBytesWritten++;
      return (int) _iv[_ivBytesWritten - 1] & 0xFF;
    }
    return in.read();
  }

  @Override
  public int read(byte[] b)
    throws IOException
  {
    if(_opmode == Cipher.ENCRYPT_MODE && _ivBytesWritten < _ivLen)
    {
      int readCount = Math.min(b.length, _ivLen - _ivBytesWritten);
      System.arraycopy(_iv, _ivBytesWritten, b, 0, readCount);
      _ivBytesWritten += readCount;
      return readCount;
    }
    return in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len)
    throws IOException
  {
    if(_opmode == Cipher.ENCRYPT_MODE && _ivBytesWritten < _ivLen)
    {
      int readCount = Math.min(len, _ivLen - _ivBytesWritten);
      System.arraycopy(_iv, _ivBytesWritten, b, off, readCount);
      _ivBytesWritten += readCount;
      return readCount;
    }
    return in.read(b, off, len);
  }

  @Override
  public long skip(long n)
    throws IOException
  {
    if(_opmode == Cipher.ENCRYPT_MODE && _ivBytesWritten < _ivLen)
    {
      long skipped = Math.min(_ivLen - _ivBytesWritten, n);
      _ivBytesWritten += skipped;
      return skipped;
    }
    return in.skip(n);
  }
}
