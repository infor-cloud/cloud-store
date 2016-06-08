package com.logicblox.s3lib;


import com.google.common.util.concurrent.ListenableFuture;

interface RetriableTask<V> {
  ListenableFuture<V> retry();
}
