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

package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;


/**
 * Provides an abstract interface to object metadata used by various cloud
 * store services
 */
public class Metadata
{
  protected ObjectMetadata _s3Metadata = null;

  
  Metadata(ObjectMetadata s3Data)
  {
    _s3Metadata = s3Data;
  }


  /**
   * Gets the optional Cache-Control HTTP header which allows the user to 
   * specify caching behavior along the HTTP request/reply chain.
   */
  public String getCacheControl()
  {
    return _s3Metadata.getCacheControl();
  }

  /**
   * Gets the optional Content-Disposition HTTP header, which specifies 
   * presentation information for the object such as the recommended 
   * filename for the object to be saved as.
   */
  public String getContentDisposition()
  {
    return _s3Metadata.getContentDisposition();
  }

  /**
   * Gets the optional Content-Encoding HTTP header specifying what content 
   * encodings have been applied to the object and what decoding mechanisms 
   * must be applied in order to obtain the media-type referenced by the 
   * Content-Type field.
   */
  public String getContentEncoding()
  {
    return _s3Metadata.getContentEncoding();
  }

  /**
   * Gets the Content-Language HTTP header, which describes the natural 
   * language(s) of the intended audience for the enclosed entity.
   */
  public String getContentLanguage()
  {
    return _s3Metadata.getContentLanguage();
  }

  /**
   * Gets the Content-Length HTTP header indicating the size of the 
   * associated object in bytes.
   */
  public long getContentLength()
  {
    return _s3Metadata.getContentLength();
  }

  /**
   * Gets the base64 encoded 128-bit MD5 digest of the associated object 
   * (content - not including headers) according to RFC 1864.
   */
  public String getContentMD5()
  {
    return _s3Metadata.getContentMD5();
  }

  /**
   * Returns the content range of the object if response contains the 
   * Content-Range header.
   */
  public Long[] getContentRange()
  {
    return _s3Metadata.getContentRange();
  }

  /**
   * Gets the Content-Type HTTP header, which indicates the type of content 
   * stored in the associated object.
   */
  public String getContentType()
  {
    return _s3Metadata.getContentType();
  }

  /**
   * Gets the hex encoded 128-bit MD5 digest of the associated object 
   * according to RFC 1864.
   */
  public String getETag()
  {
    return _s3Metadata.getETag();
  }

  /**
   * Returns the time this object will expire and be completely removed 
   * from the store.
   */
  public Date getExpirationTime()
  {
    return _s3Metadata.getExpirationTime();
  }

  /**
   * Returns the store-specific rule ID for this object's expiration, or 
   * null if it doesn't expire.
   */
  public String getExpirationTimeRuleId()
  {
    return _s3Metadata.getExpirationTimeRuleId();
  }

  /**
   * Returns the date when the object is no longer cacheable.
   */
  public Date getHttpExpiresDate()
  {
    return _s3Metadata.getHttpExpiresDate();
  }

  /**
   * Returns the physical length of the entire object in the store.
   */
  public long getInstanceLength()
  {
    return _s3Metadata.getInstanceLength();
  }

  /**
   * Gets the value of the Last-Modified header, indicating the date and 
   * time at which the store last recorded a modification to the associated 
   * object.
   */
  public Date getLastModified()
  {
    return _s3Metadata.getLastModified();
  }

  /**
   * Returns the boolean value which indicates whether there is ongoing 
   * restore request.
   */
  public Boolean getOngoingRestore()
  {
    return _s3Metadata.getOngoingRestore();
  }

  /**
   * Returns the number of parts used to upload the object.
   */
  public Integer getPartCount()
  {
    return _s3Metadata.getPartCount();
  }

  /**
   * Gets a map of the raw metadata/headers for the associated object.
   */
  public Map<String,Object> getRawMetadata()
  {
    return _s3Metadata.getRawMetadata();
  }

  /**
   *
   */
  public String getReplicationStatus()
  {
    return _s3Metadata.getReplicationStatus();
  }

  /**
   * Returns the time at which an object that has been temporarily restored 
   * from a long term backing store (i.e. AWS Glacier) will expire, and will
   * need to be restored again in order to be accessed.
   */
  public Date getRestoreExpirationTime()
  {
    return _s3Metadata.getRestoreExpirationTime();
  }

  /**
   * Returns the server-side encryption algorithm when encrypting the object
   * using keys managed by the store.
   */
  public String getSSEAlgorithm()
  {
    return _s3Metadata.getSSEAlgorithm();
  }

  /**
   * Returns the server-side encryption algorithm if the object is encrypted
   * using customer-provided keys.
   */
  public String getSSECustomerAlgorithm()
  {
    return _s3Metadata.getSSECustomerAlgorithm();
  }

  /**
   *
   */
  public String getStorageClass()
  {
    return _s3Metadata.getStorageClass();
  }

  /**
   * Gets the custom user-metadata for the associated object.
   */
  public Map<String,String> getUserMetadata()
  {
    return _s3Metadata.getUserMetadata();
  }

  /**
   * Gets the version ID of the associated object if available.
   */
  public String getVersionId()
  {
    return _s3Metadata.getVersionId();
  }

  /**
   * Returns true if the user has enabled Requester Pays option when 
   * conducting this operation from Requester Pays Bucket; else false.
   */
  public boolean isRequesterCharged()
  {
    return _s3Metadata.isRequesterCharged();
  }

  

  // only used internally
  void print(PrintStream stream)
  {
    stream.println("Cache-Control: " + getCacheControl());
    stream.println("Content-Disposition: " + getContentDisposition());
    stream.println("Content-Encoding: " + getContentEncoding());
    stream.println("Content-Length: " + getContentLength());
    stream.println("Content-MD5: " + getContentMD5());
    stream.println("Content-Type: " + getContentType());
    stream.println("ETag: " + getETag());
    stream.println("Expiration-Time: " + getExpirationTime());
    stream.println("Expiration-Time-Rule-Id: " + getExpirationTimeRuleId());
    stream.println("Http-Expires: " + getHttpExpiresDate());
    stream.println("Last-Modified: " + getLastModified());
    stream.println("Server-Side-Encryption: " + getSSEAlgorithm());
    stream.println("Version-Id: " + getVersionId());
    stream.println("");
    for(Map.Entry<String, String> entry : getUserMetadata().entrySet())
    {
      stream.println(entry.getKey() + ": " + entry.getValue());
    }
  }
  
}
