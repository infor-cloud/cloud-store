package com.logicblox.s3lib;

import java.util.Map;

import com.google.common.util.concurrent.ListenableFuture;
import com.amazonaws.services.s3.model.CannedAccessControlList;

interface UploadFactory
{
  ListenableFuture<Upload> startUpload(String bucketName, String key, Map<String,String> meta, CannedAccessControlList cannedAcl);
}
