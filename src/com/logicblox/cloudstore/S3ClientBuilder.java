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

import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class S3ClientBuilder
{
  private AmazonS3Client s3Client;
  private ListeningExecutorService apiExecutor;
  private ListeningScheduledExecutorService internalExecutor;
  private KeyProvider keyProvider;

  public S3ClientBuilder setInternalS3Client(AmazonS3Client s3Client)
  {
    this.s3Client = s3Client;
    return this;
  }

  public S3ClientBuilder setApiExecutor(ListeningExecutorService apiExecutor)
  {
    this.apiExecutor = apiExecutor;
    return this;
  }

  public S3ClientBuilder setInternalExecutor(ListeningScheduledExecutorService internalExecutor)
  {
    this.internalExecutor = internalExecutor;
    return this;
  }

  public S3ClientBuilder setKeyProvider(KeyProvider keyProvider)
  {
    this.keyProvider = keyProvider;
    return this;
  }

  public S3Client createS3Client()
  {
    if (s3Client == null)
    {
      setInternalS3Client(new AmazonS3Client());
    }
    if (apiExecutor == null)
    {
      setApiExecutor(Utils.createApiExecutor(10));
    }
    if (internalExecutor == null)
    {
      setInternalExecutor(Utils.createInternalExecutor(50));
    }
    if (keyProvider == null)
    {
      setKeyProvider(Utils.createKeyProvider(Utils.getDefaultKeyDirectory()));
    }
    return new S3Client(s3Client, apiExecutor, internalExecutor, keyProvider);
  }
}
