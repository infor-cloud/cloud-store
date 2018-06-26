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

/**
 * StorageClassHandler provides an abstract interface over storage class
 * functionality that may or may not be supported by a particular service.
 *
 * @see CloudStoreClient#getStorageClassHandler()
 */
public interface StorageClassHandler
{
  /**
   * Return true if the specified storage class name is supported by
   * a cloud store service.
   *
   * @param storageClass Name of a storage class to be checked for validity
   * @return validity of a storage class name
   */
  boolean isStorageClassValid(String storageClass);
}
