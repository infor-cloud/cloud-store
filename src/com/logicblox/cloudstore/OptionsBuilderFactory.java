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
 * OptionsBuilderFactory is used to create builder objects for all cloud-store
 * command options.  These option builders are the only way to parameterize
 * a cloud-store command.  The OptionsBuilderFactory must be retrieved from
 * a {@link CloudStoreClient} interface.
 *
 * @see CloudStoreClient#getOptionsBuilderFactory()
 */
public class OptionsBuilderFactory
{
  private final CloudStoreClient _client;

  /**
   * Create a factory for a particular CloudStoreClient interface.
   */
  OptionsBuilderFactory(CloudStoreClient client)
  {
    _client = client;
  }

  /**
   * Return a new builder for {@link CopyOptions}.
   * 
   * @return builder for CopyOptions
   */
  public CopyOptionsBuilder newCopyOptionsBuilder()
  {
    return new CopyOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link DeleteOptions}.
   * 
   * @return builder for DeleteOptions
   */
  public DeleteOptionsBuilder newDeleteOptionsBuilder()
  {
    return new DeleteOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link DownloadOptions}.
   * 
   * @return builder for DownloadOptions
   */
  public DownloadOptionsBuilder newDownloadOptionsBuilder()
  {
    return new DownloadOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link EncryptionKeyOptions}.
   * 
   * @return builder for EncryptionKeyOptions
   */
  public EncryptionKeyOptionsBuilder newEncryptionKeyOptionsBuilder()
  {
    return new EncryptionKeyOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link ExistsOptions}.
   * 
   * @return builder for ExistsOptions
   */
  public ExistsOptionsBuilder newExistsOptionsBuilder()
  {
    return new ExistsOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link ListOptions}.
   * 
   * @return builder for ListOptions
   */
  public ListOptionsBuilder newListOptionsBuilder()
  {
    return new ListOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link PendingUploadsOptions}.
   * 
   * @return builder for PendingUploadsOptions
   */
  public PendingUploadsOptionsBuilder newPendingUploadsOptionsBuilder()
  {
    return new PendingUploadsOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link RenameOptions}.
   * 
   * @return builder for RenameOptions
   */
  public RenameOptionsBuilder newRenameOptionsBuilder()
  {
    return new RenameOptionsBuilder(_client);
  }

  /**
   * Return a new builder for {@link UploadOptions}.
   * 
   * @return builder for UploadOptions
   */
  public UploadOptionsBuilder newUploadOptionsBuilder()
  {
    return new UploadOptionsBuilder(_client);
  }
}
