package com.logicblox.s3lib;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;

public interface KeyProvider
{
  public PrivateKey getPrivateKey(String alias) throws NoSuchKeyException;
  public PublicKey getPublicKey(String alias) throws NoSuchKeyException;
  public Certificate getCertificate(String alias) throws NoSuchKeyException;
}
