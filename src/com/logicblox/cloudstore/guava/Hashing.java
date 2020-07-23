
package com.logicblox.cloudstore.guava;

public final class Hashing {

  /**
   * Returns a hash function implementing the CRC32C checksum algorithm (32 hash bits) as described
   * by RFC 3720, Section 12.1.
   *
   * @since 18.0
   */
  public static HashFunction crc32c() {
    return Crc32cHolder.CRC_32_C;
  }

  private static final class Crc32cHolder {
    static final HashFunction CRC_32_C = new Crc32cHashFunction();
  }

  private Hashing() {}
}
