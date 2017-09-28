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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

public class KeyGenCommand
{

  private KeyPairGenerator _keyPairGenerator;
  private KeyPair _keyPair;
  private PublicKey _publicKey;
  private PrivateKey _privateKey;

  public KeyGenCommand(String algo, int nbits)
    throws NoSuchAlgorithmException
  {
    _keyPairGenerator = KeyPairGenerator.getInstance(algo);
    _keyPairGenerator.initialize(nbits);
    _keyPair = _keyPairGenerator.generateKeyPair();
    _publicKey = _keyPair.getPublic();
    _privateKey = _keyPair.getPrivate();
  }

  public void savePemKeypair(File pemf)
    throws IOException
  {
    String pem = getPemPublicKey() + "\n" + getPemPrivateKey();

    FileUtils.writeStringToFile(pemf, pem, "UTF-8");
  }

  public String getPemPrivateKey()
  {
    // pkcs8_der is PKCS#8-encoded binary (DER) private key
    byte[] pkcs8_der = _privateKey.getEncoded();

    // DER to PEM conversion
    String pem_encoded = pemEncode(pkcs8_der, "-----BEGIN PRIVATE KEY-----\n",
      "-----END PRIVATE KEY-----\n");

    return pem_encoded;
  }

  public String getPemPublicKey()
  {
    // x509_der is X.509-encoded binary (DER) public key
    byte[] x509_der = _publicKey.getEncoded();

    // DER to PEM conversion
    String pem_encoded = pemEncode(x509_der, "-----BEGIN PUBLIC KEY-----\n",
      "-----END PUBLIC KEY-----\n");

    return pem_encoded;
  }

  private String pemEncode(byte[] keyBytes, String startArmour, String endArmour)
  {
    int lineLength = Base64.PEM_CHUNK_SIZE;
    byte[] lineSeparator = {'\n'};
    Base64 b64 = new Base64(lineLength, lineSeparator);
    String encoded = b64.encodeToString(keyBytes);

    return startArmour + encoded + endArmour;
  }

}
