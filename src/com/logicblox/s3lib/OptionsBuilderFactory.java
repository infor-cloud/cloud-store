package com.logicblox.s3lib;

class OptionsBuilderFactory
{
  private final CloudStoreClient _client;

  OptionsBuilderFactory(CloudStoreClient client)
  {
    _client = client;
  }

  CopyOptionsBuilder newCopyOptionsBuilder()
  {
    return new CopyOptionsBuilder(_client);
  }

  DeleteOptionsBuilder newDeleteOptionsBuilder()
  {
    return new DeleteOptionsBuilder(_client);
  }

  DownloadOptionsBuilder newDownloadOptionsBuilder()
  {
    return new DownloadOptionsBuilder(_client);
  }

  EncryptionKeyOptionsBuilder newEncryptionKeyOptionsBuilder()
  {
    return new EncryptionKeyOptionsBuilder(_client);
  }

  ExistsOptionsBuilder newExistsOptionsBuilder()
  {
    return new ExistsOptionsBuilder(_client);
  }

  ListOptionsBuilder newListOptionsBuilder()
  {
    return new ListOptionsBuilder(_client);
  }

  PendingUploadsOptionsBuilder newPendingUploadsOptionsBuilder()
  {
    return new PendingUploadsOptionsBuilder(_client);
  }

  RenameOptionsBuilder newRenameOptionsBuilder()
  {
    return new RenameOptionsBuilder(_client);
  }

  UploadOptionsBuilder newUploadOptionsBuilder()
  {
    return new UploadOptionsBuilder(_client);
  }
}
