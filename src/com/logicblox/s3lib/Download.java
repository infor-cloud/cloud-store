package com.logicblox.s3lib;

import java.util.Map;
import java.io.InputStream;

import com.google.common.util.concurrent.ListenableFuture;

public interface Download {
	ListenableFuture<InputStream> getPart(long start, long end);

	ListenableFuture<Void> abort();

	Map<String,String> getMeta();
}
