package com.logicblox.s3lib;

public class S3AclHandler implements AclHandler
{
  @Override
  public boolean isCannedAclValid(String cannedAcl)
  {
    return S3Client.ALL_CANNED_ACLS.contains(cannedAcl);
  }

  @Override
  public String getDefaultAcl()
  {
    return "bucket-owner-full-control";
  }
}