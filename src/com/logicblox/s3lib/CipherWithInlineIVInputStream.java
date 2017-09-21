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

package com.logicblox.s3lib;

import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.security.Key;
import java.security.InvalidKeyException;
import java.security.InvalidAlgorithmParameterException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;

class CipherWithInlineIVInputStream extends FilterInputStream {
  private int opmode;
  private int ivBytesWritten = 0;
  private int ivLen;
  private byte[] iv;

  public CipherWithInlineIVInputStream(InputStream in, Cipher cipher, int opmode, Key key)
  throws IOException, InvalidKeyException, InvalidAlgorithmParameterException
  {
    super(in);

    ivLen = cipher.getBlockSize();

    this.opmode = opmode;

    switch (this.opmode) {
      case Cipher.DECRYPT_MODE:
        iv = new byte[ivLen];
        int offset = 0;
        // !!! Should this be in a background thread?
        while (offset < ivLen) {
          int result = in.read(iv, offset, ivLen - offset);
          if (result == -1) {
            // !!! What should we really do here?
            throw new RuntimeException();
          }
          offset += result;
        }
        cipher.init(this.opmode, key, new IvParameterSpec(iv));
        iv = null;
        break;
      case Cipher.ENCRYPT_MODE:
        cipher.init(this.opmode, key);
        iv = cipher.getIV();
        break;
      default:
        throw new IllegalArgumentException(CipherWithInlineIVInputStream.class.getCanonicalName() + " can only be constructed in DECRYPT_MODE or ENCRYPT_MODE");
    }
    this.in = new CipherInputStream(this.in, cipher);
  }

  @Override
  public int available() throws IOException {
    if (this.opmode == Cipher.ENCRYPT_MODE && ivBytesWritten < ivLen) {
      return ivLen - ivBytesWritten;
    }
    return in.available();
  }

  @Override
  public int read() throws IOException {
    if (this.opmode == Cipher.ENCRYPT_MODE && ivBytesWritten < ivLen) {
      ivBytesWritten++;
      return (int) iv[ivBytesWritten - 1] & 0xFF;
    }
    return in.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    if (this.opmode == Cipher.ENCRYPT_MODE && ivBytesWritten < ivLen) {
      int readCount = Math.min(b.length, ivLen - ivBytesWritten);
      System.arraycopy(iv, ivBytesWritten, b, 0, readCount);
      ivBytesWritten += readCount;
      return readCount;
    }
    return in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (this.opmode == Cipher.ENCRYPT_MODE && ivBytesWritten < ivLen) {
      int readCount = Math.min(len, ivLen - ivBytesWritten);
      System.arraycopy(iv, ivBytesWritten, b, off, readCount);
      ivBytesWritten += readCount;
      return readCount;
    }
    return in.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    if (this.opmode == Cipher.ENCRYPT_MODE && ivBytesWritten < ivLen) {
      long skipped = Math.min(ivLen - ivBytesWritten, n);
      ivBytesWritten += skipped;
      return skipped;
    }
    return in.skip(n);
  }
}
