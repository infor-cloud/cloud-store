package com.logicblox.s3lib;

import java.util.concurrent.Executor;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;

  /**
   * A future that falls back on a second, generated future, in case its
   * original future fails.
   */
  public class FallbackFuture<V> extends AbstractFuture<V>
  {
    private volatile ListenableFuture<? extends V> running;

    FallbackFuture(ListenableFuture<? extends V> input,
        final FutureFallback<? extends V> fallback,
        final Executor executor) {
      running = input;
      Futures.addCallback(running, new FutureCallback<V>() {
        @Override
        public void onSuccess(V value) {
          set(value);
        }

        @Override
        public void onFailure(Throwable t) {
          if (isCancelled()) {
            return;
          }
          try {
            running = fallback.create(t);

            /* TODO does not work with guava 14.0
            if (isCancelled()) { // in case cancel called in the meantime
              running.cancel(wasInterrupted());
              return;
            }
            */

            Futures.addCallback(running, new FutureCallback<V>() {
              @Override
              public void onSuccess(V value) {
                set(value);
              }

              @Override
              public void onFailure(Throwable t) {
                if (running.isCancelled()) {
                  cancel(false);
                } else {
                  setException(t);
                }
              }
            }, sameThreadExecutor());
          } catch (Exception e) {
            setException(e);
          } catch (Error e) {
            setException(e); // note: rethrows
          }
        }
      }, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (super.cancel(mayInterruptIfRunning)) {
        running.cancel(mayInterruptIfRunning);
        return true;
      }
      return false;
    }
  }
