package com.logicblox.cloudstore.guava;

import java.io.Serializable;

public interface Funnel<T> extends Serializable {

  /**
   * Sends a stream of data from the {@code from} object into the sink {@code into}. There is no
   * requirement that this data be complete enough to fully reconstitute the object later.
   *
   * @since 12.0 (in Guava 11.0, {@code PrimitiveSink} was named {@code Sink})
   */
  void funnel(T from, PrimitiveSink into);
}
