package com.trifork.deltazip;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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

    @Test
    public void timestampsTest() throws IOException {
        for (int i=1; i<=30; i++) {
            long seconds = System.currentTimeMillis()/1000 + rnd.nextInt(1 << i);
            Date ts = new Date(seconds * 1000);
            testEncodeDecodeDate(ts);
        }

        for (int y=2001; y<=2130; y++) {
            Calendar c = Calendar.getInstance();
            c.set(Calendar.MILLISECOND, 0);
            c.set(Calendar.YEAR, y);
            testEncodeDecodeDate(c.getTime());
        }
    }

    private void testEncodeDecodeDate(Date ts) throws IOException {
        Metadata.Item item = new Metadata.Timestamp(ts);

        List<Metadata.Item> outItems = testEncodeDecode(Collections.singletonList(item));
        Metadata.Item out = outItems.get(0);
        assertEquals(Metadata.Timestamp.class, out.getClass());
        assertEquals(ts, ((Metadata.Timestamp)out).getDate());
    }

    private byte[] randomBlob(int maxSize) {
        byte[] blob = new byte[rnd.nextInt(maxSize+1)];
        rnd.nextBytes(blob);
        return blob;
    }

    private List<Metadata.Item> testEncodeDecode(List<Metadata.Item> items) throws IOException {
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

        return items2;
    }
}
