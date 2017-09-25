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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class ConsoleProgressListener
  implements OverallProgressListener
{
  /**
   * This map contains the progress of each part.
   * <p>
   * Currently, it is not a ConcurrentMap since it is used by {@code progress}, which is {@code
   * synchronized}, and {@code getTotalTransferredBytes}, which is called only by {@code progress}.
   */
  protected Map<String, PartProgressEvent> partsProgressEvents
    = new HashMap<String, PartProgressEvent>();
  protected final long intervalInBytes;
  protected final ProgressOptions options;
  protected AtomicLong lastReportBytes = new AtomicLong();

  ConsoleProgressListener(ProgressOptions options, long intervalInBytes)
  {
    this.options = options;
    this.intervalInBytes = intervalInBytes;
  }

  synchronized public void progress(PartProgressEvent partProgressEvent)
  {
    partsProgressEvents.put(partProgressEvent.getPartId(), partProgressEvent);

    String opEd = options.getOperation();
    opEd = opEd.endsWith("y") ? opEd.substring(0, opEd.length() - 1) + "ied" : opEd + "ed";

    long totalTransferredBytes = getTotalTransferredBytes();
    long unreportedBytes = getUnreportedBytes(totalTransferredBytes);
    if(isReportTime(unreportedBytes) ||
      (isTransferComplete(totalTransferredBytes) && !allBytesReported()))
    {
      System.out.println(
        MessageFormat.format("{0}: ({1}%) {2} {3}/{4} bytes...", options.getObjectUri(),
          100 * totalTransferredBytes / options.getFileSizeInBytes(), opEd, totalTransferredBytes,
          options.getFileSizeInBytes()));
      lastReportBytes.set(totalTransferredBytes);
    }
  }

  private long getTotalTransferredBytes()
  {
    long bytes = 0L;
    for(Map.Entry<String, PartProgressEvent> e : partsProgressEvents.entrySet())
    {
      bytes += e.getValue().getTransferredBytes();
    }

    return bytes;
  }

  private long getUnreportedBytes(long totalTransferredBytes)
  {
    return totalTransferredBytes - lastReportBytes.get();
  }

  private boolean allBytesReported()
  {
    return lastReportBytes.get() == options.getFileSizeInBytes();
  }

  private boolean isReportTime(long unreportedBytes)
  {
    return unreportedBytes >= intervalInBytes;
  }

  private boolean isTransferComplete(long totalTransferredBytes)
  {
    return totalTransferredBytes == options.getFileSizeInBytes();
  }
}
