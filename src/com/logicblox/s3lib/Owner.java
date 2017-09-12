package com.logicblox.s3lib;

/**
 * Represents the owner of a resource in a cloud storage service.
 */
public class Owner
{
  protected String _id = null;
  protected String _displayName = null;

  /**
   * Create a new Owner.
   */
  public Owner(String id, String displayName)
  {
    _id = id;
    _displayName = displayName;
  }

  /**
   * Return this Owner's ID.
   */
  public String getId()
  {
    return _id;
  }

  /**
   * Return this Owner's name.
   */
  public String getDisplayName()
  {
    return _displayName;
  }
}
