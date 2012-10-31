package com.logicblox.s3lib;

import com.google.common.util.concurrent.ListenableFuture;

public interface DownloadFactory {
	ListenableFuture<Download> startDownload(String bucketName, String key);
}
