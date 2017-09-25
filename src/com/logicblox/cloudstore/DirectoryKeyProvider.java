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

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.regex.Pattern;

public class DirectoryKeyProvider
  implements KeyProvider
{
  private static final Pattern beginPublic = Pattern.compile(
    "^----[\\-\\ ]BEGIN PUBLIC KEY---[-]+$");
  private static final Pattern endPublic = Pattern.compile("^----[\\-\\ ]END PUBLIC KEY---[-]+$");

  private static final Pattern beginPrivate = Pattern.compile(
    "^----[\\-\\ ]BEGIN PRIVATE KEY---[-]+$");
  private static final Pattern endPrivate = Pattern.compile("^----[\\-\\ ]END PRIVATE KEY---[-]+$");

  private static final Pattern beginCertificate = Pattern.compile(
    "^----[\\-\\ ]BEGIN CERTIFICATE---[-]+$");
  private static final Pattern endCertificate = Pattern.compile(
    "^----[\\-\\ ]END CERTIFICATE---[-]+$");

  private File _directory;

  public DirectoryKeyProvider(File directory)
  {
    _directory = directory;
  }

  public PrivateKey getPrivateKey(String alias)
    throws NoSuchKeyException
  {
    try
    {
      byte[] bytes = extractKey(getFile(alias, "pem"), beginPrivate, endPrivate);
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(bytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(keySpec);
    }
    catch(NoSuchAlgorithmException exc)
    {
      throw new RuntimeException(exc);
    }
    catch(InvalidKeySpecException exc)
    {
      throw new NoSuchKeyException(exc);
    }
    catch(IOException exc)
    {
      throw new NoSuchKeyException(exc);
    }
  }

  public PublicKey getPublicKey(String alias)
    throws NoSuchKeyException
  {
    try
    {
      byte[] bytes = extractKey(getFile(alias, "pem"), beginPublic, endPublic);
      X509EncodedKeySpec keySpec = new X509EncodedKeySpec(bytes);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePublic(keySpec);
    }
    catch(NoSuchAlgorithmException exc)
    {
      throw new RuntimeException(exc);
    }
    catch(InvalidKeySpecException exc)
    {
      throw new NoSuchKeyException(exc);
    }
    catch(IOException exc)
    {
      throw new NoSuchKeyException(exc);
    }
  }

  public Certificate getCertificate(String alias)
    throws NoSuchKeyException
  {
    try
    {
      byte[] bytes = extractKey(getFile(alias, "cer"), beginCertificate, endCertificate);
      CertificateFactory certFact = CertificateFactory.getInstance("X.509");
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      return certFact.generateCertificate(bis);
    }
    catch(CertificateException exc)
    {
      throw new NoSuchKeyException(exc);
    }
    catch(IOException exc)
    {
      throw new NoSuchKeyException(exc);
    }
  }

  /**
   * Returns the .pem file for the given alias.
   */
  private File getFile(String alias, String extension)
    throws NoSuchKeyException
  {
    File result = null;

    if(!_directory.exists() || !_directory.isDirectory())
    {
      throw new NoSuchKeyException("Invalid key directory: " + _directory.getPath());
    }

    // iterate of the actual files to avoid security issues with alias
    // that are not simple file names.
    for(File file : _directory.listFiles())
    {
      if(file.getName().equals(alias + "." + extension))
      {
        result = file;
      }
    }

    if(result == null)
    {
      throw new NoSuchKeyException("No such key: " + alias);
    }

    return result;
  }

  private byte[] extractKey(File file, Pattern begin, Pattern end)
    throws NoSuchKeyException, IOException
  {
    int state = 0;
    StringBuilder keyPem = new StringBuilder();

    BufferedReader in = null;
    boolean threw = true;
    try
    {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charsets.UTF_8));
      String line;
      while(((line = in.readLine()) != null))
      {
        if(begin.matcher(line).matches() && state == 0)
        {
          state = 1;
          continue;
        }
        else if(end.matcher(line).matches() && state == 1)
        {
          state = 2;
        }

        if(state == 1)
        {
          keyPem.append(line);
          keyPem.append("\n");
        }
      }

      threw = false;
    }
    finally
    {
      Closeables.close(in, threw);
    }

    if(state != 2)
    {
      throw new NoSuchKeyException("Incorrect file format: " + file.getPath());
    }


    byte[] keyBytes = DatatypeConverter.parseBase64Binary(keyPem.toString());
    return keyBytes;
  }
}
