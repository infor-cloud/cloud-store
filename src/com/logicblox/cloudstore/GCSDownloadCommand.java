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

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;

class GCSDownloadCommand
  extends DownloadCommand
{
  public GCSDownloadCommand(DownloadOptions options)
    throws IOException
  {
    super(options);
  }

  @Override
  protected ListenableFuture<Download> initiateDownload()
  {
    GCSDownloadFactory factory = new GCSDownloadFactory(_options, getGCSClient(),
      _client.getApiExecutor(), _client.getInternalExecutor());
    return factory.startDownload();
  }
}
