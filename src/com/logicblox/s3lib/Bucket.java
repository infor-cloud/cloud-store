package com.logicblox.s3lib;

import java.util.Date;


/**
 * Represents a bucket used to store objects in a cloud store service.
 */
public class Bucket
{
  protected String _name = null;
  protected Date _creationDate = null;
  protected Owner _owner = null;
  

  /**
   * Create a new bucket.
   */
  public Bucket(String name, Date creationDate, Owner owner)
  {
    _name = name;
    _creationDate = creationDate;
    _owner = owner;
  }

  
  /**
   * Return this bucket's name.
   */
  public String getName()
  {
    return _name;
  }


  /**
   * Return the date this bucket was created.
   */
  public Date getCreationDate()
  {
    return _creationDate;
  }

  /**
   * Return information about the bucket's owner.
   */
  public Owner getOwner()
  {
    return _owner;
  }
}
