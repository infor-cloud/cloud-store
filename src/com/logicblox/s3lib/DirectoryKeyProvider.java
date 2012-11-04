package com.logicblox.s3lib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;

public class DirectoryKeyProvider implements KeyProvider
{
  private static final Pattern beginPublic = Pattern.compile("^----[\\-\\ ]BEGIN PUBLIC KEY---[-]+$");
  private static final Pattern endPublic = Pattern.compile("^----[\\-\\ ]END PUBLIC KEY---[-]+$");

  private static final Pattern beginPrivate = Pattern.compile("^----[\\-\\ ]BEGIN PRIVATE KEY---[-]+$");
  private static final Pattern endPrivate = Pattern.compile("^----[\\-\\ ]END PRIVATE KEY---[-]+$");

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
      byte[] bytes = extractKey(getFile(alias), beginPrivate, endPrivate);
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
      byte[] bytes = extractKey(getFile(alias), beginPublic, endPublic);
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

  /**
   * Returns the .pem file for the given alias.
   */
  private File getFile(String alias)
  throws NoSuchKeyException
  {
    File result = null;

    // iterate of the actual files to avoid security issues with alias
    // that are not simple file names.
    for(File file : _directory.listFiles())
    {
      if(file.getName().equals(alias + ".pem"))
        result = file;
    }

    if(result == null)
      throw new NoSuchKeyException("No such key: " + alias);

    return result;
  }

  public byte[] extractKey(File file, Pattern begin, Pattern end)
  throws NoSuchKeyException, IOException
  {
    int state = 0;
    StringBuilder keyPem = new StringBuilder();
    BufferedReader in = new BufferedReader(new FileReader(file));

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

    if(state != 2)
      throw new NoSuchKeyException("Incorrect file format: " + file.getPath());


    byte[] keyBytes = DatatypeConverter.parseBase64Binary(keyPem.toString());
    return keyBytes;
  }
}