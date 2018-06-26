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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Grant;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

public class S3AclHandler
  implements AclHandler
{
  private final AmazonS3 _s3Client;

  S3AclHandler(AmazonS3 client)
  {
    _s3Client = client;
  }

  @Override
  public boolean isCannedAclValid(String cannedAcl)
  {
    return S3Client.ALL_CANNED_ACLS.contains(cannedAcl);
  }

  @Override
  public String getDefaultCannedAcl()
  {
    return "bucket-owner-full-control";
  }

  @Override
  public ListenableFuture<Acl> getObjectAcl(String bucketName, String objectKey)
    throws AmazonS3Exception
  {
    AccessControlList s3Acl;
    s3Acl = _s3Client.getObjectAcl(bucketName, objectKey);

    Owner owner = new Owner(s3Acl.getOwner().getId(), s3Acl.getOwner().getDisplayName());

    List<AclGrant> grants = new ArrayList<>();
    for (Grant grant : s3Acl.getGrantsAsList())
    {
      AclGrantee grantee = new AclGrantee(grant.getGrantee().getIdentifier());
      AclPermission permission = new AclPermission(grant.getPermission().toString());

      grants.add(new AclGrant(grantee, permission));
    }

    return Futures.immediateFuture(new Acl(owner, grants));

  }
}
