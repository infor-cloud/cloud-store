/*
  Copyright 2018, Infor Inc.

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


//import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class ThrowableRetriableTask<V>
  implements Callable<ListenableFuture<V>>
{
  private final Callable<ListenableFuture<V>> _callable;
  private final ListeningScheduledExecutorService _executor;
  private final ThrowableRetryPolicy _retryPolicy;
  private int _retryCount;

  // for testing
  private static Set<RetryListener> _retryListeners = new HashSet<RetryListener>();

  public ThrowableRetriableTask(
    Callable<ListenableFuture<V>> callable, ListeningScheduledExecutorService executor,
    ThrowableRetryPolicy retryPolicy)
  {
    _callable = callable;
    _executor = executor;
    _retryPolicy = retryPolicy;
  }

  @Override
  public ListenableFuture<V> call()
  {
    ListenableFuture<V> future;
    try
    {
      future = _callable.call();
    }
    catch(Exception exc)
    {
      future = Futures.immediateFailedFuture(exc);
    }

//    return Futures.withFallback(future, new FutureFallback<V>()
    return Futures.catchingAsync(
      future, 
      Throwable.class, 
      new AsyncFunction<Throwable, V>()
      {
//      public ListenableFuture<V> create(Throwable t)
        public ListenableFuture<V> apply(Throwable t)
        {
          _retryCount++;
          if(_retryPolicy.shouldRetry(t, _retryCount))
          {
            String msg = "Info: Retriable exception: " + _callable.toString() + ": " + t.getMessage();
            System.err.println(msg);
            sendRetryNotifications(_callable.toString(), t);

            long delay = _retryPolicy.getDelay(t, _retryCount);
            // TODO: actually use the scheduled executor once Guava 15 is out
            // Futures.dereference(_executor.schedule(_callable, delay, TimeUnit.MILLISECONDS));
            sleep(delay);

            return call();
          }
          else
          {
            return Futures.immediateFailedFuture(t);
          }
        }
      },
      MoreExecutors.directExecutor());
  }

  // for testing
  private synchronized void sendRetryNotifications(String callableId, Throwable t)
  {
    for(RetryListener l : _retryListeners)
      l.retryTriggered(new RetryEvent(callableId, t));
  }

  // for testing
  synchronized static void addRetryListener(RetryListener l)
  {
    _retryListeners.add(l);
  }

  private void sleep(long delay)
  {
    if(delay > 0)
    {
      try
      {
        Thread.sleep(delay);
      }
      catch(InterruptedException ignored)
      {
      }
    }
  }
}
