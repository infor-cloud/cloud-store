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

public class ConsoleProgressListenerFactory
  implements OverallProgressListenerFactory
{

  private long _intervalInBytes = -1;

  private long getDefaultIntervalInBytes(long totalSizeInBytes)
  {
    long mb = 1024 * 1024;
    return (totalSizeInBytes >= 50 * mb) ? 10 * mb : mb;
  }

  public ConsoleProgressListenerFactory setIntervalInBytes(long intervalInBytes)
  {
    _intervalInBytes = intervalInBytes;
    return this;
  }

  public OverallProgressListener create(ProgressOptions progressOptions)
  {
    if(_intervalInBytes <= 0)
    {
      _intervalInBytes = getDefaultIntervalInBytes(progressOptions.getFileSizeInBytes());
    }
    return new ConsoleProgressListener(progressOptions, _intervalInBytes);
  }
}
