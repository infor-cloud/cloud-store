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
 * Exception thrown by {@link KeyProvider KeyProviders} when a specified key name
 * cannot be found in the provider.
 */
public class NoSuchKeyException
  extends Exception
{
  /**
   * Create a new exception with the specified error message.
   *
   * @param msg The error message to be contained in the exception.
   */
  public NoSuchKeyException(String msg)
  {
    super(msg);
  }

  /**
   * Create a new exception with the specified cause.
   *
   * @param cause Another exception that is the root cause of this exception.
   */
  public NoSuchKeyException(Exception cause)
  {
    super(cause);
  }
}
