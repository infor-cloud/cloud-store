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

class S3ProgressListener
  implements com.amazonaws.event.ProgressListener
{
  final private OverallProgressListener opl;
  final private PartProgressEvent ppe;

  public S3ProgressListener(OverallProgressListener opl, PartProgressEvent ppe)
  {
    this.opl = opl;
    this.ppe = ppe;
  }

  @Override
  public void progressChanged(com.amazonaws.event.ProgressEvent event)
  {
    ppe.setLastTransferBytes(event.getBytesTransferred());
    opl.progress(ppe);
  }
}
