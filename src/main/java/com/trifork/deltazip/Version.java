package com.trifork.deltazip;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/** Represents a content revision inside a DeltaZip archive.
 */
public class Version {
    private final ByteBuffer contents;
    private final List<Metadata.Item> metadata;

    public Version(ByteBuffer contents) {
        this(contents, Collections.<Metadata.Item>emptyList());
    }

    public Version(ByteBuffer contents, List<Metadata.Item> metadata) {
        this.contents = contents.asReadOnlyBuffer();
        this.metadata = metadata;
    }

    public ByteBuffer getContents() {
        return contents;
    }

    public List<Metadata.Item> getMetadata() {
        return metadata;
    }

}
