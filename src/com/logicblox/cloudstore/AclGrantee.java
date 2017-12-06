package com.logicblox.cloudstore;

class AclGrantee
{
  private final String _id;

  AclGrantee(String id)
  {
    _id = id;
  }

  public String getId()
  {
    return _id;
  }
}
