package com.logicblox.cloudstore;

class AclGrant
{
  private final AclGrantee _grantee;
  private final AclPermission _permission;

  AclGrant(AclGrantee grantee, AclPermission permission)
  {
    _grantee = grantee;
    _permission = permission;
  }

  public AclGrantee getGrantee()
  {
    return _grantee;
  }

  public AclPermission getPermission()
  {
    return _permission;
  }
}

