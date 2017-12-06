package com.logicblox.cloudstore;

class AclPermission
{
  private final String _perm;

  AclPermission(String perm)
  {
    _perm = perm;
  }

  public String toString()
  {
    return _perm;
  }
}
