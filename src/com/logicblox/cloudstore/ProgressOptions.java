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

public class ProgressOptions
{
  private final String _objectUri;
  private final String _operation;
  private final long _fileSizeInBytes;

  ProgressOptions(String objectUri, String operation, long fileSizeInBytes)
  {
    _objectUri = objectUri;
    _operation = operation;
    _fileSizeInBytes = fileSizeInBytes;
  }

  public String getObjectUri()
  {
    return _objectUri;
  }

  public String getOperation()
  {
    return _operation;
  }

  public long getFileSizeInBytes()
  {
    return _fileSizeInBytes;
  }
}
