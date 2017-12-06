package com.logicblox.cloudstore;

import java.util.ArrayList;
import java.util.List;

class Acl
{
  private final Owner _owner;
  private List<AclGrant> _grants;

  Acl(Owner owner, List<AclGrant> grants)
  {
    _owner = owner;
    _grants = new ArrayList<>(grants);
  }

  public Owner getOwner()
  {
    return _owner;
  }

  public List<AclGrant> getGrants()
  {
    return _grants;
  }
}
