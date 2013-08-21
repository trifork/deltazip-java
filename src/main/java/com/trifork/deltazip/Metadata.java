package com.trifork.deltazip;

import com.sun.org.apache.xpath.internal.operations.And;
import com.sun.xml.internal.stream.writers.UTF8OutputStreamWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Code related to metadata associated with versions in DeltaZip archives.
 */
public class Metadata {

    // Defined metadata keytags:
    public static final int TIMESTAMP_KEYTAG  =  1;
    public static final int VERSION_ID_KEYTAG =  2;
    public static final int ANCESTOR_KEYTAG  =   3;

    private static final String[] KEYTAG_TO_NAME;
    private static final Map<String, Integer> NAME_TO_KEYTAG;
    static {
        KEYTAG_TO_NAME = new String[4];
        KEYTAG_TO_NAME[TIMESTAMP_KEYTAG]  = "timestamp";
        KEYTAG_TO_NAME[VERSION_ID_KEYTAG] = "version_id";
        KEYTAG_TO_NAME[ANCESTOR_KEYTAG]   = "ancestor";
        NAME_TO_KEYTAG = new HashMap();
        for (int i=0; i<KEYTAG_TO_NAME.length;i++) {
            String s = KEYTAG_TO_NAME[i];
            if (s!=null) NAME_TO_KEYTAG.put(s,i);
        }
    }

    public static final int START_OF_YEAR_2000_IN_UNIX_TIME = ((2000 - 1970) * 365 + 7) * 24 * 60 * 60;
    private static final Charset UTF8 = Charset.forName("UTF-8");

    public static int name_to_keytag(String name) {
        return NAME_TO_KEYTAG.get(name);
    }

    public static List<Metadata.Item> items(Metadata.Item... items) {
        return Arrays.asList(items);
    }

    public static void pack(List<Item> items, OutputStream dest) throws IOException {
        // First, pack the metadata:
        ByteArrayOutputStream meta_out = new ByteArrayOutputStream();
        for (Item item : items) {
            pack_item(item, meta_out);
        }
        byte[] packed_metadata = meta_out.toByteArray();

        // Then write it with a header and trailer:
        DZUtil.writeBytestring(dest, packed_metadata);

        DZUtil.varlen_encode(packed_metadata.length, meta_out); // Include in checksum
        dest.write(0xff - computeMod255Checksum(meta_out.toByteArray()));
    }

    private static void pack_item(Item item, OutputStream dest) throws IOException {
        DZUtil.varlen_encode(item.getNumericKeytag(), dest);
        byte[] value = item.getValue();
        DZUtil.writeBytestring(dest, value);
    }

    public static List<Item> unpack(ByteBuffer src) throws IOException {
        ByteBuffer packed_metadata = extract_metadata_src(src);
        return unpack_from_src(packed_metadata);
    }

    private static ByteBuffer extract_metadata_src(ByteBuffer src) throws IOException {
        int start_pos = src.position();
        byte[] packed_metadata_bytes = DZUtil.readBytestring(src);
        ByteBuffer packed_metadata = ByteBuffer.wrap(packed_metadata_bytes);

        byte _chksum_extra = src.get();
        int end_pos = src.position();
        int chksum = computeMod255Checksum(src, start_pos, end_pos);
        if (chksum != 0) throw new IOException("Checksum failed - was "+chksum+", not zero");
        return packed_metadata;
    }

    private static List<Item> unpack_from_src(ByteBuffer packed_metadata) throws IOException {
        List<Item> items = new ArrayList<Item>();
        while (packed_metadata.hasRemaining()) {
            items.add(unpack_item(packed_metadata));
        }
        return items;
    }

    private static Item unpack_item(ByteBuffer packed_metadata) throws IOException {
        int keytag = DZUtil.varlen_decode(packed_metadata);
        byte[] value = DZUtil.readBytestring(packed_metadata);
        switch (keytag) {
            case TIMESTAMP_KEYTAG:  if (value.length==4) return new Timestamp(value); else break;
            case VERSION_ID_KEYTAG: return new VersionID(value);
            case ANCESTOR_KEYTAG:   return new Ancestor(value);
        }
        return new Item(keytag, value);
    }

    private static int computeMod255Checksum(byte[] data) {
        long sum = 0;
        for (byte d : data) {
            sum += d & 0xFF;
        }
        return (int)(sum % 255);
    }
    private static int computeMod255Checksum(ByteBuffer data, int start_pos, int end_pos) {
        long sum = 0;
        for (int i=start_pos; i<end_pos; i++) {
            sum += data.get(i) & 0xFF;
        }
        return (int)(sum % 255);
    }

    public static class Item {
        private final int keytag;
        private final byte[] value;

        public Item(int keytag, byte[] value) {
            this.keytag = keytag;
            this.value = value;
        }

        public Item(int keytag, String value) {
            this(keytag, value.getBytes(UTF8));
        }

        public int getNumericKeytag() {
            return keytag;
        }

        public byte[] getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            Item item = (Item) o;

            return (keytag == item.keytag && Arrays.equals(value, item.value));
        }

        @Override
        public int hashCode() {
            int result = keytag;
            result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
            return result;
        }

        public String toString() {
            return "<"+keytag+","+Arrays.toString(value)+">";
        }
    }


    public static class Timestamp extends Item {
        private final Date date_value;

        public Timestamp(Date date) {
            super(TIMESTAMP_KEYTAG, encodeDate(date));
            this.date_value = date;
        }

        public Timestamp(byte[] bytes) {
            super(TIMESTAMP_KEYTAG, bytes);
            this.date_value = decodeDate(bytes);
        }

        public Date getDate() {
            return date_value;
        }

        private static byte[] encodeDate(Date date) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long secondsSinceUnixEpoch = date.getTime() / 1000;
            long secondsSinceY2K = secondsSinceUnixEpoch - START_OF_YEAR_2000_IN_UNIX_TIME;
            if (secondsSinceY2K >= 1L << 32) throw new IllegalArgumentException("Date overflow: "+secondsSinceY2K);
            try {
                new DataOutputStream(baos).writeInt((int)secondsSinceY2K); // uint32
            } catch (IOException ioe) {
                throw new RuntimeException("Can't happen: "+ioe);
            }
            return baos.toByteArray();
        }

        private Date decodeDate(byte[] bytes) {
            // We could in principle support 5-byte timestamps in the future, but right now we don't.
            if (bytes.length != 4) {
                throw new IllegalArgumentException("Timestamp field length error: expected 4 bytes, got "+bytes.length);
            }

            final long secondsSinceY2K;
            try {
                secondsSinceY2K = new DataInputStream(new ByteArrayInputStream(bytes)).readInt() & ((1L<<32)-1);
            } catch (IOException ioe) {
                throw new RuntimeException("Can't happen: "+ioe); // We did check the length, so shouldn't reach here.
            }

            long secondsSinceUnixEpoch = secondsSinceY2K + START_OF_YEAR_2000_IN_UNIX_TIME;
            return new Date(secondsSinceUnixEpoch * 1000);
        }
    }

    public static class VersionID extends Item {
        public static final int KEYTAG = VERSION_ID_KEYTAG;
        public VersionID(byte[] value) { super(KEYTAG, value); }
        public VersionID(String value) { super(KEYTAG, value); }
    }

    public static class Ancestor extends Item {
        public static final int KEYTAG = ANCESTOR_KEYTAG;
        public Ancestor(byte[] value) { super(KEYTAG, value); }
        public Ancestor(String value) { super(KEYTAG, value); }
    }
}