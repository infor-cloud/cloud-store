package com.logicblox.s3lib;

public class GCSAclHandler implements AclHandler
{
  @Override
  public boolean isCannedAclValid(String cannedAcl)
  {
    return GCSClient.ALL_CANNED_ACLS.contains(cannedAcl);
  }

  @Override
  public String getDefaultAcl()
  {
    return "bucketOwnerFullControl";
  }
}
