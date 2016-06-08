package com.logicblox.s3lib;


import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import java.util.concurrent.Callable;

class ThrowableRetriableTask<V> implements RetriableTask<V>{
  private final Callable<ListenableFuture<V>> callable;
  private final ListeningScheduledExecutorService executor;
  private final ThrowableRetryPolicy retryPolicy;

  ThrowableRetriableTask(Callable<ListenableFuture<V>> callable,
                         ListeningScheduledExecutorService executor,
                         ThrowableRetryPolicy retryPolicy)
  {
    this.callable = callable;
    this.executor = executor;
    this.retryPolicy = retryPolicy;
  }

  public ListenableFuture<V> retry()
  {
    retryPolicy.sleep();

    ListenableFuture<V> future;
    try
    {
      future = callable.call();
    }
    catch(Exception exc)
    {
      future = Futures.immediateFailedFuture(exc);
    }

    return Futures.withFallback(
      future,
      new FutureFallback<V>()
      {
        public ListenableFuture<V> create(Throwable t)
        {
          retryPolicy.errorOccurred(t);
          if(retryPolicy.shouldRetry())
          {
            String msg = "Info: Retriable exception: " + callable.toString() +
              ": " + t.getMessage();

            System.err.println(msg);
            return retry();
          }
          else
          {
            return Futures.immediateFailedFuture(t);
          }
        }
      });
  }
}