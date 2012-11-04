package com.logicblox.s3lib;

import java.security.PrivateKey;
import java.security.PublicKey;

public interface KeyProvider
{
  public PrivateKey getPrivateKey(String alias) throws NoSuchKeyException;
  public PublicKey getPublicKey(String alias) throws NoSuchKeyException;
}
