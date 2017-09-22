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
 * Listener interface for part-transfer progress events.
 */
public interface OverallProgressListener
{
  /**
   * Called to notify that progress has been changed for a specific part of the transferred file.
   * <p>
   * This methods might be called from multiple different threads, each one transferring different
   * part of the file, so it has to be implemented in a thread-safe manner. Declaring it as {@code
   * synchronized} is suggested.
   */
  public void progress(PartProgressEvent partProgressEvent);
}
