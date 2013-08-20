package com.trifork.deltazip;

/** Represents version differences.
 */
public abstract class FormatVersion {
    public abstract boolean supportsMetadata();
    public abstract int versionSizeBits();

    public int versionSizeLimit() { return 1 << versionSizeBits();}

    public static FormatVersion VERSION_10 = new FormatVersion() {
        @Override public boolean supportsMetadata() { return false; }

        @Override public int versionSizeBits() { return 28; }
    };

    public static FormatVersion VERSION_11 = new FormatVersion() {
        @Override public boolean supportsMetadata() { return true; }

        @Override public int versionSizeBits() { return 27; }
    };
}
