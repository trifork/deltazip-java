package com.trifork.deltazip;

/** Represents version differences.
 */
public abstract class Version {
    public abstract boolean supportsMetadata();
    public abstract int versionSizeBits();

    public int versionSizeLimit() { return 1 << versionSizeBits();}

    public static Version VERSION_10 = new Version() {
        @Override public boolean supportsMetadata() { return false; }

        @Override public int versionSizeBits() { return 28; }
    };

    public static Version VERSION_11 = new Version() {
        @Override public boolean supportsMetadata() { return true; }

        @Override public int versionSizeBits() { return 27; }
    };
}
