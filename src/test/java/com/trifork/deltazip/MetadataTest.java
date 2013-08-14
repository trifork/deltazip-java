package com.trifork.deltazip;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class MetadataTest {
    Random rnd = new Random();

    @Test
    public void emptyTest() throws IOException {
        testEncodeDecode(Collections.EMPTY_LIST);
    }

    @Test
    public void opaqueFieldsTest() throws IOException {
        for (int i=1; i<=300; i++) {
            int fieldCount = 1 + rnd.nextInt(i);
            List<Metadata.Item> items = new ArrayList(fieldCount);
            for (int j=0; j<fieldCount; j++) {
                int keytag = 1 + rnd.nextInt(Integer.MAX_VALUE);
                byte[] blob = randomBlob(10*i);
                items.add(new Metadata.Item(keytag, blob));
            }
            testEncodeDecode(items);
        }
    }


    private byte[] randomBlob(int maxSize) {
        byte[] blob = new byte[rnd.nextInt(maxSize+1)];
        rnd.nextBytes(blob);
        return blob;
    }

    private void testEncodeDecode(List<Metadata.Item> items) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Metadata.pack(items, os);
        ByteBuffer src = ByteBuffer.wrap(os.toByteArray());
        List<Metadata.Item> items2 = Metadata.unpack(src);

        assertEquals(items.size(), items2.size());
        for (int i=0; i<items.size(); i++) {
            Metadata.Item a = items.get(i);
            Metadata.Item b = items2.get(i);
            assertEquals(a.getNumericKeytag(), b.getNumericKeytag());
            //assertArrayEquals(a.getValue(), b.getValue()); // Much too slow
            assertTrue(Arrays.equals(a.getValue(), b.getValue()));
        }
    }
}
