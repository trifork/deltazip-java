package com.trifork.deltazip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    //START_OF_YEAR_2000_IN_GREGORIAN_SECONDS

    public static int name_to_keytag(String name) {
        return NAME_TO_KEYTAG.get(name);
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
        int start_pos = src.position();
        byte[] packed_metadata_bytes = DZUtil.readBytestring(src);
        ByteBuffer packed_metadata = ByteBuffer.wrap(packed_metadata_bytes);

        byte _chksum_extra = src.get();
        int end_pos = src.position();
        int chksum = computeMod255Checksum(src, start_pos, end_pos);
        if (chksum != 0) throw new IOException("Checksum failed - was "+chksum+", not zero");

        List<Item> items = new ArrayList<Item>();
        while (packed_metadata.hasRemaining()) {
            items.add(unpack_item(packed_metadata));
        }
        return items;
    }

    private static Item unpack_item(ByteBuffer packed_metadata) throws IOException {
        int keytag = DZUtil.varlen_decode(packed_metadata);
        byte[] value = DZUtil.readBytestring(packed_metadata);
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

        public int getNumericKeytag() {
            return keytag;
        }

        public byte[] getValue() {
            return value;
        }
    }
}
