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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Notification of a progress change on a part transfer. Typically this means notice that another
 * chunk of bytes of the specified part was transferred.
 */
public class PartProgressEvent
{
  final String partId;
  private AtomicLong lastTransferBytes = new AtomicLong();
  private AtomicLong transferredBytes = new AtomicLong();

  PartProgressEvent(String partId)
  {
    this.partId = partId;
  }

  public String getPartId()
  {
    return partId;
  }

  /**
   * We declare it as {@code synchronized} because, typically, it can be called by two threads that
   * transfer different chunks of the same part.
   */
  synchronized public void setLastTransferBytes(long lastTransferBytes)
  {
    this.lastTransferBytes.set(lastTransferBytes);
    this.transferredBytes.addAndGet(lastTransferBytes);
  }

  public void setTransferredBytes(long transferredBytes)
  {
    this.transferredBytes.set(transferredBytes);
  }

  public long getTransferredBytes()
  {
    return transferredBytes.get();
  }
}
