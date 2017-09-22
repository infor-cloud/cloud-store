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

/**
 * A usage exception is thrown for incorrect usage of the
 * tool. Reporting a stack trace is not considered useful, and only
 * the message needs to be reported to the user/
 */
public class UsageException extends RuntimeException
{
  public UsageException(String msg)
  {
    super(msg);
  }

  public UsageException(String msg, Throwable cause)
  {
    super(msg, cause);
  }
}
