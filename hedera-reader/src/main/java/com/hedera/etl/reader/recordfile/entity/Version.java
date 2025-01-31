package com.hedera.etl.reader.recordfile.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

public class Version implements Comparable<Version>, Serializable {

  private static final String VERSION_PARSE_ERROR =
      "Invalid version string; Could not parse segment %s within %s";

  /**
   * Returns the Java version of the running JVM.
   *
   * @return will never be {@literal null}.
   */
  public static Version javaVersion() {
    return parse(System.getProperty("java.version"));
  }

  /**
   * Parses the given string representation of a version into a {@link Version} object.
   *
   * @param version must not be {@literal null} or empty.
   * @return
   */
  public static Version parse(String version) {
    String[] parts = version.trim().split("\\.");
    int[] intParts = new int[parts.length];

    for (int i = 0; i < parts.length; i++) {

      String input = i == parts.length - 1 ? parts[i].replaceAll("\\D.*", "") : parts[i];

      if (StringUtils.isNotBlank(input)) {
        try {
          intParts[i] = Integer.parseInt(input);
        } catch (IllegalArgumentException o_O) {
          throw new IllegalArgumentException(
              String.format(VERSION_PARSE_ERROR, input, version), o_O);
        }
      }
    }

    return new Version(intParts);
  }

  private final int bugfix;
  private final int build;

  private final int major;

  private final int minor;

  /**
   * Creates a new {@link Version} from the given integer values. At least one value has to be given
   * but a maximum of 4.
   *
   * @param parts must not be {@literal null} or empty.
   */
  public Version(int... parts) {
    this.major = parts[0];
    this.minor = parts.length > 1 ? parts[1] : 0;
    this.bugfix = parts.length > 2 ? parts[2] : 0;
    this.build = parts.length > 3 ? parts[3] : 0;
  }

  public int compareTo(@SuppressWarnings("null") Version that) {

    if (major != that.major) {
      return major - that.major;
    }

    if (minor != that.minor) {
      return minor - that.minor;
    }

    if (bugfix != that.bugfix) {
      return bugfix - that.bugfix;
    }

    if (build != that.build) {
      return build - that.build;
    }

    return 0;
  }

  @Override
  public boolean equals(@Nullable Object obj) {

    if (this == obj) {
      return true;
    }

    if (!(obj instanceof Version)) {
      return false;
    }

    var that = (Version) obj;

    return this.major == that.major
        && this.minor == that.minor
        && this.bugfix == that.bugfix
        && this.build == that.build;
  }

  @Override
  public int hashCode() {

    int result = 17;
    result += 31 * major;
    result += 31 * minor;
    result += 31 * bugfix;
    result += 31 * build;
    return result;
  }

  /**
   * Returns whether the current {@link Version} is the same as the given one.
   *
   * @param version
   * @return
   */
  public boolean is(Version version) {
    return equals(version);
  }

  /**
   * Returns whether the current {@link Version} is greater (newer) than the given one.
   *
   * @param version
   * @return
   */
  public boolean isGreaterThan(Version version) {
    return compareTo(version) > 0;
  }

  /**
   * Returns whether the current {@link Version} is greater (newer) or the same as the given one.
   *
   * @param version
   * @return
   */
  public boolean isGreaterThanOrEqualTo(Version version) {
    return compareTo(version) >= 0;
  }

  /**
   * Returns whether the current {@link Version} is less (older) than the given one.
   *
   * @param version
   * @return
   */
  public boolean isLessThan(Version version) {
    return compareTo(version) < 0;
  }

  /**
   * Returns whether the current {@link Version} is less (older) or equal to the current one.
   *
   * @param version
   * @return
   */
  public boolean isLessThanOrEqualTo(Version version) {
    return compareTo(version) <= 0;
  }

  @Override
  public String toString() {

    List<Integer> digits = new ArrayList<>();
    digits.add(major);
    digits.add(minor);

    if (build != 0 || bugfix != 0) {
      digits.add(bugfix);
    }

    if (build != 0) {
      digits.add(build);
    }

    return digits.stream().map(Object::toString).collect(Collectors.joining("."));
  }
}
