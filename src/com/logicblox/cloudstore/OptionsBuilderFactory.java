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

public class OptionsBuilderFactory
{
  private final CloudStoreClient _client;

  OptionsBuilderFactory(CloudStoreClient client)
  {
    _client = client;
  }

  public CopyOptionsBuilder newCopyOptionsBuilder()
  {
    return new CopyOptionsBuilder(_client);
  }

  public DeleteOptionsBuilder newDeleteOptionsBuilder()
  {
    return new DeleteOptionsBuilder(_client);
  }

  public DownloadOptionsBuilder newDownloadOptionsBuilder()
  {
    return new DownloadOptionsBuilder(_client);
  }

  public EncryptionKeyOptionsBuilder newEncryptionKeyOptionsBuilder()
  {
    return new EncryptionKeyOptionsBuilder(_client);
  }

  public ExistsOptionsBuilder newExistsOptionsBuilder()
  {
    return new ExistsOptionsBuilder(_client);
  }

  public ListOptionsBuilder newListOptionsBuilder()
  {
    return new ListOptionsBuilder(_client);
  }

  public PendingUploadsOptionsBuilder newPendingUploadsOptionsBuilder()
  {
    return new PendingUploadsOptionsBuilder(_client);
  }

  public RenameOptionsBuilder newRenameOptionsBuilder()
  {
    return new RenameOptionsBuilder(_client);
  }

  public UploadOptionsBuilder newUploadOptionsBuilder()
  {
    return new UploadOptionsBuilder(_client);
  }
}
