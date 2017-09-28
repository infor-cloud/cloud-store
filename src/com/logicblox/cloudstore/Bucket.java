/*
  Copyright 2017, Infor Inc.

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
   *
   * @param name Name of a bucket.
   * @param creationDate Date the bucket was created
   * @param owner Owner of the bucket
   */
  public Bucket(String name, Date creationDate, Owner owner)
  {
    _name = name;
    _creationDate = creationDate;
    _owner = owner;
  }


  /**
   * Get this bucket's name.
   *
   * @return Returns bucket name.
   */
  public String getName()
  {
    return _name;
  }


  /**
   * Get the date this bucket was created.
   *
   * @return Returns bucket creating date.
   */
  public Date getCreationDate()
  {
    return _creationDate;
  }

  /**
   * Get information about the bucket's owner.
   *
   * @return Returns the bucket owner.
   */
  public Owner getOwner()
  {
    return _owner;
  }
}
