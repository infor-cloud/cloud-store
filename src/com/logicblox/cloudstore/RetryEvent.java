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


public class RetryEvent
{
  private String _callableId;
  private Throwable _throwable;
  
  RetryEvent(String callableId, Throwable t)
  {
    _callableId = callableId;
    _throwable = t;
  }

  public String getCallableId()
  {
    return _callableId;
  }

  public Throwable getThrowable()
  {
    return _throwable;
  }
}
