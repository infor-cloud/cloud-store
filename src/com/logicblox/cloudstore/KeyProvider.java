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

import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;

/**
 * KeyProvider is an interface used to retrieve public/private key information
 * given the name of key pair.
 *
 * @see CloudStoreClient#getKeyProvider
 */

public interface KeyProvider
{
  /**
   * Return the private part of a key pair for a given key name or throw
   * {@link NoSuchKeyException} if the key pair cannot be located.
   *
   * @param alias The name of the key pair to search for.
   * @return PrivateKey for the given alias
   * @throws NoSuchKeyException
   */
  public PrivateKey getPrivateKey(String alias)
    throws NoSuchKeyException;

  /**
   * Return the public part of a key pair for a given key name or throw
   * {@link NoSuchKeyException} if the key pair cannot be located.
   *
   * @param alias The name of the key pair to search for.
   * @return PublicKey for the given alias
   * @throws NoSuchKeyException
   */
  public PublicKey getPublicKey(String alias)
    throws NoSuchKeyException;

  /**
   * Return the certificate associated with a given key name or throw
   * {@link NoSuchKeyException} if the key pair cannot be located.
   *
   * @param alias The name of the key pair to search for.
   * @return Certificate for the given alias
   * @throws NoSuchKeyException
   */
  public Certificate getCertificate(String alias)
    throws NoSuchKeyException;
}
