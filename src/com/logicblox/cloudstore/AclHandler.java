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

/**
 * AclHandler is an interface that abstracts over the access control functionality
 * provided by different cloud store services.  
 *
 * @see CloudStoreClient#getAclHandler()
 */
public interface AclHandler
{
  /**
   * Return true if the specified {@code cannedAcl} is a valid known ACL name
   * for a cloud store service.
   *
   * @param cannedAcl Name of an access control list to check for validity
   */
  boolean isCannedAclValid(String cannedAcl);

  /**
   * Return the name of the default access control list used by a store service.
   */
  String getDefaultAcl();
}
