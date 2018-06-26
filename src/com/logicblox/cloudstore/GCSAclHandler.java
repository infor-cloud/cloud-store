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

import com.amazonaws.services.s3.AmazonS3;
import com.google.api.services.storage.Storage;
import com.google.common.util.concurrent.ListenableFuture;

public class GCSAclHandler
  implements AclHandler
{
  private final Storage _gcsClient;
  private final AmazonS3 _s3Client;

  GCSAclHandler(Storage gcsClient, AmazonS3 s3Client)
  {
    _gcsClient = gcsClient;
    _s3Client = s3Client;
  }

  @Override
  public boolean isCannedAclValid(String cannedAcl)
  {
    return GCSClient.ALL_CANNED_ACLS.contains(cannedAcl);
  }

  @Override
  public String getDefaultCannedAcl()
  {
    return "bucketOwnerFullControl";
  }

  @Override
  public ListenableFuture<Acl> getObjectAcl(String bucketName, String objectKey)
  {
    throw new UnsupportedOperationException("getObjectAcl is not supported.");
  }
}
