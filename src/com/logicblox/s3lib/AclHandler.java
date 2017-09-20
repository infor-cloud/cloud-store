package com.logicblox.s3lib;

interface AclHandler
{
  boolean isCannedAclValid(String cannedAcl);

  String getDefaultAcl();
}
