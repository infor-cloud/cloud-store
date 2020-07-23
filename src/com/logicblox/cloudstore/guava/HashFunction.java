package com.logicblox.cloudstore.guava;

import com.google.common.primitives.Ints;
import java.nio.charset.Charset;

public interface HashFunction {
  /**
   * Begins a new hash code computation by returning an initialized, stateful {@code
   * Hasher} instance that is ready to receive data. Example: <pre>   {@code
   *
   *   HashFunction hf = Hashing.md5();
   *   HashCode hc = hf.newHasher()
   *       .putLong(id)
   *       .putBoolean(isActive)
   *       .hash();}</pre>
   */
  Hasher newHasher();

  /**
   * Begins a new hash code computation as {@link #newHasher()}, but provides a hint of the expected
   * size of the input (in bytes). This is only important for non-streaming hash functions (hash
   * functions that need to buffer their whole input before processing any of it).
   */
  Hasher newHasher(int expectedInputSize);

  /**
   * Shortcut for {@code newHasher().putInt(input).hash()}; returns the hash code for the given
   * {@code int} value, interpreted in little-endian byte order. The implementation <i>might</i>
   * perform better than its longhand equivalent, but should not perform worse.
   *
   * @since 12.0
   */
  HashCode hashInt(int input);

  /**
   * Shortcut for {@code newHasher().putLong(input).hash()}; returns the hash code for the given
   * {@code long} value, interpreted in little-endian byte order. The implementation <i>might</i>
   * perform better than its longhand equivalent, but should not perform worse.
   */
  HashCode hashLong(long input);

  /**
   * Shortcut for {@code newHasher().putBytes(input).hash()}. The implementation <i>might</i>
   * perform better than its longhand equivalent, but should not perform worse.
   */
  HashCode hashBytes(byte[] input);

  /**
   * Shortcut for {@code newHasher().putBytes(input, off, len).hash()}. The implementation
   * <i>might</i> perform better than its longhand equivalent, but should not perform worse.
   *
   * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > bytes.length} or
   *     {@code len < 0}
   */
  HashCode hashBytes(byte[] input, int off, int len);

  /**
   * Shortcut for {@code newHasher().putUnencodedChars(input).hash()}. The implementation
   * <i>might</i> perform better than its longhand equivalent, but should not perform worse. Note
   * that no character encoding is performed; the low byte and high byte of each {@code char} are
   * hashed directly (in that order).
   *
   * <p><b>Warning:</b> This method will produce different output than most other languages do when
   * running the same hash function on the equivalent input. For cross-language compatibility, use
   * {@link #hashString}, usually with a charset of UTF-8. For other use cases, use {@code
   * hashUnencodedChars}.
   *
   * @since 15.0 (since 11.0 as hashString(CharSequence)).
   */
  HashCode hashUnencodedChars(CharSequence input);

  /**
   * Shortcut for {@code newHasher().putString(input, charset).hash()}. Characters are encoded using
   * the given {@link Charset}. The implementation <i>might</i> perform better than its longhand
   * equivalent, but should not perform worse.
   *
   * <p><b>Warning:</b> This method, which reencodes the input before hashing it, is useful only for
   * cross-language compatibility. For other use cases, prefer {@link #hashUnencodedChars}, which is
   * faster, produces the same output across Java releases, and hashes every {@code char} in the
   * input, even if some are invalid.
   */
  HashCode hashString(CharSequence input, Charset charset);

  /**
   * Shortcut for {@code newHasher().putObject(instance, funnel).hash()}. The implementation
   * <i>might</i> perform better than its longhand equivalent, but should not perform worse.
   *
   * @since 14.0
   */
  <T> HashCode hashObject(T instance, Funnel<? super T> funnel);

  /**
   * Returns the number of bits (a multiple of 32) that each hash code produced by this hash
   * function has.
   */
  int bits();
}
