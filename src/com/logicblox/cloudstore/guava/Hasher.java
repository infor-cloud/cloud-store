
package com.logicblox.cloudstore.guava;

import java.nio.charset.Charset;

public interface Hasher extends PrimitiveSink {
  @Override
  Hasher putByte(byte b);

  @Override
  Hasher putBytes(byte[] bytes);

  @Override
  Hasher putBytes(byte[] bytes, int off, int len);

  @Override
  Hasher putShort(short s);

  @Override
  Hasher putInt(int i);

  @Override
  Hasher putLong(long l);

  /**
   * Equivalent to {@code putInt(Float.floatToRawIntBits(f))}.
   */
  @Override
  Hasher putFloat(float f);

  /**
   * Equivalent to {@code putLong(Double.doubleToRawLongBits(d))}.
   */
  @Override
  Hasher putDouble(double d);

  /**
   * Equivalent to {@code putByte(b ? (byte) 1 : (byte) 0)}.
   */
  @Override
  Hasher putBoolean(boolean b);

  @Override
  Hasher putChar(char c);

  /**
   * Equivalent to processing each {@code char} value in the {@code CharSequence}, in order. In
   * other words, no character encoding is performed; the low byte and high byte of each {@code
   * char} are hashed directly (in that order). The input must not be updated while this method is
   * in progress.
   *
   * <p><b>Warning:</b> This method will produce different output than most other languages do when
   * running the same hash function on the equivalent input. For cross-language compatibility, use
   * {@link #putString}, usually with a charset of UTF-8. For other use cases, use {@code
   * putUnencodedChars}.
   *
   * @since 15.0 (since 11.0 as putString(CharSequence)).
   */
  @Override
  Hasher putUnencodedChars(CharSequence charSequence);

  /**
   * Equivalent to {@code putBytes(charSequence.toString().getBytes(charset))}.
   *
   * <p><b>Warning:</b> This method, which reencodes the input before hashing it, is useful only for
   * cross-language compatibility. For other use cases, prefer {@link #putUnencodedChars}, which is
   * faster, produces the same output across Java releases, and hashes every {@code char} in the
   * input, even if some are invalid.
   */
  @Override
  Hasher putString(CharSequence charSequence, Charset charset);

  /**
   * A simple convenience for {@code funnel.funnel(object, this)}.
   */
  <T> Hasher putObject(T instance, Funnel<? super T> funnel);

  /**
   * Computes a hash code based on the data that have been provided to this hasher. The result is
   * unspecified if this method is called more than once on the same instance.
   */
  HashCode hash();

  /**
   * {@inheritDoc}
   *
   * @deprecated This returns {@link Object#hashCode()}; you almost certainly mean to call
   *     {@code hash().asInt()}.
   */
  @Override
  @Deprecated
  int hashCode();
}
