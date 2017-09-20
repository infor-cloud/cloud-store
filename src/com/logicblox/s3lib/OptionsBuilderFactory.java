package com.logicblox.s3lib;

public class OptionsBuilderFactory
{
  private final CloudStoreClient _client;

  OptionsBuilderFactory(CloudStoreClient client)
  {
    _client = client;
  }

  public CopyOptionsBuilder newCopyOptionsBuilder()
  {
    return new CopyOptionsBuilder(_client);
  }

  public DeleteOptionsBuilder newDeleteOptionsBuilder()
  {
    return new DeleteOptionsBuilder(_client);
  }

  public DownloadOptionsBuilder newDownloadOptionsBuilder()
  {
    return new DownloadOptionsBuilder(_client);
  }

  public EncryptionKeyOptionsBuilder newEncryptionKeyOptionsBuilder()
  {
    return new EncryptionKeyOptionsBuilder(_client);
  }

  public ExistsOptionsBuilder newExistsOptionsBuilder()
  {
    return new ExistsOptionsBuilder(_client);
  }

  public ListOptionsBuilder newListOptionsBuilder()
  {
    return new ListOptionsBuilder(_client);
  }

  public PendingUploadsOptionsBuilder newPendingUploadsOptionsBuilder()
  {
    return new PendingUploadsOptionsBuilder(_client);
  }

  public RenameOptionsBuilder newRenameOptionsBuilder()
  {
    return new RenameOptionsBuilder(_client);
  }

  public UploadOptionsBuilder newUploadOptionsBuilder()
  {
    return new UploadOptionsBuilder(_client);
  }
}
