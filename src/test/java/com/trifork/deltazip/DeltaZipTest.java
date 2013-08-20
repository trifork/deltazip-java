package com.trifork.deltazip;

import java.nio.ByteBuffer;
import java.io.IOException;
import com.trifork.deltazip.DeltaZip.AppendSpecification;
import com.trifork.deltazip.DZUtil.ByteArrayAccess;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import static junit.framework.Assert.*;

public class DeltaZipTest {
    private static final String LATIN1 = "ISO-8859-1";

    @Test
	public void test_read_known() throws Exception {
		/* With zlib headers:
		byte[] file = {32,0,0,17,0,0,14,120,
					   (byte)249,31,(byte)158,4,106,(byte)243,0,113,
					   0,5,(byte)140,1,(byte)245,32,0,0,
					   17,0,0,0,13,72,101,108,
					   108,111,44,32,87,111,114,108,
					   100,33,0,0,0,13};
		*/

		/** Chunked-deflate, no prefix/suffix. */
		byte[] two_revs1 = {
			(byte)0xCE, (byte)0xB4, 0x7A, 0x10,
			64,0,0,7, 0x05,(byte)0x8C,0x01,(byte)0xF5,
			0,0,4,(byte)243, 0,113,0,
			64,0,0,7,
			0,0,0,13, 0x1F,(byte)0x9E,0x04,0x6A,
			72,101,108,108,111, 44,32,87,111,114,108,100,33,
			0,0,0,13};

		/** Chunked-deflate, using prefix. */
		byte[] two_revs2 = {
			(byte)0xCE, (byte)0xB4, 0x7A, 0x10,
			64,0,0,5, 0x05,(byte)0x8C,0x01,(byte)0xF5,
			8,0,2,0, 4,
			64,0,0,5,
			0,0,0,13, 0x1F,(byte)0x9E,0x04,0x6A,
			72,101,108,108,111,44,32,87,111,114,108,100,33,
			0,0,0,13};

		ByteBuffer exp_rev1 = ByteBuffer.wrap("Hello, World!".getBytes("ISO-8859-1"));
		ByteBuffer exp_rev2 = ByteBuffer.wrap("Hello".getBytes("ISO-8859-1"));

		System.err.println("two_revs 1...");
		test_two_revs_with(two_revs1, exp_rev1, Collections.EMPTY_LIST, exp_rev2, Collections.EMPTY_LIST);
		System.err.println("two_revs 2...");
		test_two_revs_with(two_revs2, exp_rev1, Collections.EMPTY_LIST, exp_rev2, Collections.EMPTY_LIST);
	}

    @Test
    public void test_read_known_metadata() throws Exception {
        // Created with:
        // ./deltazip create test.dz -mtimestamp='2013-08-19 14:37:30' -m'version_id=xyz' a -mtimestamp='2013-08-19 14:37:31' -m'ancestor=www' b
        byte[] two_revs = {
                (byte)0xce, (byte)0xb4, 0x7a, 0x11, 0x58, 0x00, 0x00, 0x18,
                0x0f, 0x37, 0x02, (byte)0xf4, 0x0b, 0x01, 0x04, 0x19,
                (byte)0xa4, (byte)0xea, 0x2a, 0x02, 0x03, 0x78, 0x79, 0x7a,
                (byte)0xab, 0x02, 0x01, 0x00, 0x00, 0x06, 0x53, 0x28,
                0x01, 0x12, (byte)0xa9, 0x00, 0x58, 0x00, 0x00, 0x18,
                0x18, 0x00, 0x00, 0x16, 0x0a, 0x35, 0x02, 0x62,
                0x0b, 0x01, 0x04, 0x19, (byte)0xa4, (byte)0xea, 0x2b, 0x03,
                0x03, 0x77, 0x77, 0x77, (byte)0xaf, (byte)0xf3, (byte)0xc8, 0x4c,
                (byte)0xcf, 0x48, 0x2d, (byte)0xe2, 0x02, 0x00, 0x18, 0x00,
                0x00, 0x16};
        SimpleDateFormat dfmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

        ByteBuffer exp_rev1 = ByteBuffer.wrap("Hi there\n".getBytes(LATIN1));
        List<Metadata.Item> exp_md1 = Arrays.asList(
                new Metadata.Timestamp(dfmt.parse("2013-08-19 14:37:30 UTC")),
                new Metadata.VersionID("xyz".getBytes(LATIN1)));
        ByteBuffer exp_rev2 = ByteBuffer.wrap("Higher\n".getBytes(LATIN1));
        List<Metadata.Item> exp_md2 = Arrays.asList(
                new Metadata.Timestamp(dfmt.parse("2013-08-19 14:37:31 UTC")),
                new Metadata.Ancestor("www".getBytes(LATIN1)));

        test_two_revs_with(two_revs, exp_rev2, exp_md2, exp_rev1, exp_md1);

    }

    public void test_two_revs_with(byte[] file,
                                   ByteBuffer exp_rev1, List<Metadata.Item> exp_md1,
                                   ByteBuffer exp_rev2, List<Metadata.Item> exp_md2)
            throws IOException {
		DeltaZip dz = new DeltaZip(new ByteArrayAccess(file));
		ByteBuffer actual_rev1 = dz.get();
		assertEquals(exp_rev1, actual_rev1);
        assertEquals(dz.getMetadata(), exp_md1);

        dz.previous();
		ByteBuffer actual_rev2  = dz.get();
		assertEquals(exp_rev2, actual_rev2);
        assertEquals(dz.getMetadata(), exp_md2);

        try {
			dz.previous();
			throw new RuntimeException("Assertion failed");
		} catch (Exception e) {}
	}



	@Test
	public void test_add_get() throws Exception {
		System.err.println("test_add_get...");
		Version rev1  = new Version(ByteBuffer.wrap("Hello".getBytes(LATIN1)));
		Version rev1a = new Version(ByteBuffer.wrap("World!".getBytes(LATIN1)));
		Version rev1b = new Version(ByteBuffer.wrap("Held!".getBytes(LATIN1)));
		Version rev2  = new Version(ByteBuffer.wrap("Hello, World!".getBytes(LATIN1)));

        Version rev3  = new Version(ByteBuffer.wrap("Hello, World with timestamp!".getBytes(LATIN1)),
                Metadata.items(new Metadata.Timestamp(new Date(1300000000))));
        Version rev4  = new Version(ByteBuffer.wrap("Hello, World with many metadata".getBytes(LATIN1)),
                Metadata.items(
                        new Metadata.Timestamp(new Date(1400000000)),
                        new Metadata.VersionID("Revision 4"),
                        new Metadata.Ancestor("Revision 3")));

        byte[] file0 = new byte[] {};

		test_add_get_with(file0, rev1,rev2); // Would use 'prefix-copy' chunk.
		test_add_get_with(file0, rev1a,rev2); // Would use 'offset-copy' chunk.
		test_add_get_with(file0, rev1b,rev2); // Would use 'prefix-copy' and 'offset-copy'.
		test_add_get_with(file0, rev2,rev1); // Would use 'deflate' chunk.

		test_add_get_with(file0, rev2,rev3); // With and without metadata.
		test_add_get_with(file0, rev3,rev2); // With and without metadata.
		test_add_get_with(file0, rev3,rev4); // With different metadata.
		test_add_get_with(file0, rev4,rev3); // With different metadata.
	}
	
	public void test_add_get_with(byte[] file0, Version rev1, Version rev2) throws IOException {
		ByteArrayAccess access0 = new ByteArrayAccess(file0);
		DeltaZip dz0 = new DeltaZip(access0);
		AppendSpecification app1 = dz0.add(rev1);
		byte[] file1 = access0.applyAppendSpec(app1);
		dump("file1=", file1);

		ByteArrayAccess access1 = new ByteArrayAccess(file1);
		DeltaZip dz1 = new DeltaZip(access1);
		AppendSpecification app2 = dz1.add(rev2);
		byte[] file2 = access1.applyAppendSpec(app2);
		dump("file2=", file2);

		ByteArrayAccess access2 = new ByteArrayAccess(file2);
		DeltaZip dz2 = new DeltaZip(access2);

		//==== Tests:
		// ...of get():
		assertEquals(dz1.getVersion(), rev1);

		// ...of previous():
		assertEquals(dz2.getVersion(), rev2);
		dz2.previous();
		assertEquals(dz2.getVersion(), rev1);

		// ...of resetCursor():
		dz2.resetCursor();
		assertEquals(dz2.getVersion(), rev2);
		dz2.resetCursor();
		assertEquals(dz2.getVersion(), rev2);

		// ...of clone():
		DeltaZip dz2c = dz2.clone();
		assertEquals(dz2c.getVersion(), rev2);
		dz2c.previous();
		assertEquals(dz2c.getVersion(), rev1);
		// Now, dz2 should still be at the end:
		assertEquals(dz2.getVersion(), rev2);
		// And vice versa:
		dz2c.resetCursor();
		dz2.previous();
		assertEquals(dz2c.getVersion(), rev2);

		try {
			dz1.previous();
			throw new RuntimeException("Assertion failed");
		} catch (Exception e) {}
		try {
			dz2.previous();
			throw new RuntimeException("Assertion failed");
		} catch (Exception e) {}
	}

	@Test
	public void totally_random_test() throws IOException {
		final Random rnd = new Random();

		{ // 100 x 1K.
			ByteBuffer[] versions = new ByteBuffer[100];
			for (int i=0; i<versions.length; i++)
				versions[i] = createRandomBinary(1000, rnd);
			
			series_test_with(versions);
		}

		{ // 40 x 100K.
			ByteBuffer[] versions = new ByteBuffer[40];
			for (int i=0; i<versions.length; i++)
				versions[i] = createRandomBinary(100000, rnd);
			
			series_test_with(versions);
		}
	}

	@Test
	public void very_related_test() throws IOException {
		final Random rnd = new Random();

		{ // 40 x 100K.
			ByteBuffer[] versions = new ByteBuffer[40];
			versions[0] = createRandomBinary(100000, rnd);
			for (int i=1; i<versions.length; i++) {
				byte[] tmp = DZUtil.allToByteArray(versions[i-1]);
				int nMutations = rnd.nextInt(20);
				for (int k=0; k<nMutations; k++)
					tmp[rnd.nextInt(tmp.length)] = (byte) rnd.nextInt(256);
				versions[i] = ByteBuffer.wrap(tmp);
			}
			
			series_test_with(versions);
		}
	}

	@Test
	public void somewhat_related_test() throws IOException {
		final Random rnd = new Random();

		{ // 40 x 100K.
			ByteBuffer[] versions = new ByteBuffer[40];
			versions[0] = createRandomBinary(100000, rnd);
			for (int i=1; i<versions.length; i++) {
				byte[] tmp = DZUtil.allToByteArray(versions[i-1]);

				// Single-byte mutations:
				int nMutations = rnd.nextInt(20);
				for (int j=0; j<nMutations; j++)
					tmp[rnd.nextInt(tmp.length)] = (byte) rnd.nextInt(256);
				versions[i] = ByteBuffer.wrap(tmp);

				// Random runs:
				int nRuns = rnd.nextInt(10);
				for (int j=0; j<nRuns; j++) {
					int start = 0, end = tmp.length;
					int iters = rnd.nextInt(10);
					for (int k=0; k<iters && start<end; k++) { // Select part.
						int mid = start + rnd.nextInt(end - start);
						if (rnd.nextBoolean()) start=mid; else end=mid;
					}

					for (int k=start; k<end; k++) tmp[k] = (byte)rnd.nextInt(256);
				}
				

				// Block swaps:
				int nSwaps = rnd.nextInt(10);
				for (int j=0; j<nSwaps; j++) {
					//TODO
				}

				versions[i] = ByteBuffer.wrap(tmp);
			}
			
			series_test_with(versions);
		}
	}


	public void series_test_with(ByteBuffer[] versions) throws IOException {
		byte[] file = new byte[0];

		System.err.print("<");
		// Add versions:
		for (int i=0; i<versions.length; i++) {
			System.err.print(".");
			ByteArrayAccess access = new ByteArrayAccess(file);
			DeltaZip dz = new DeltaZip(access);
			AppendSpecification app_spec =
				dz.add(versions[i]);
			file = access.applyAppendSpec(app_spec);
		}

		{// Verify contents:
			ByteArrayAccess access = new ByteArrayAccess(file);
			DeltaZip dz = new DeltaZip(access);
			for (int i=versions.length-1; i>=0; i--) {
				System.err.print(",");
				assertEquals(dz.get(), versions[i]);
				if (i>0) {
					assert(dz.hasPrevious());
					dz.previous();
				}
			}
			assert(! dz.hasPrevious());
		}
		System.err.println(">");
	}

	//======================================================================

    @Test
    public void totally_random_test_with_metadata() throws IOException {
        final Random rnd = new Random();

        { // Few and small.
            Version[] versions = new Version[100];
            for (int i=0; i<versions.length; i++)
                versions[i] = createRandomVersion(100, rnd, 3, 10, 10);

            series_test_with(versions);
        }

        { // Many and large.
            Version[] versions = new Version[40];
            for (int i=0; i<versions.length; i++)
                versions[i] = createRandomVersion(1000, rnd, 10, 1000000, 10000);

            series_test_with(versions);
        }
    }

    public void series_test_with(Version[] versions) throws IOException {
        byte[] file = new byte[0];

        System.err.print("<");
        // Add versions:
        for (int i=0; i<versions.length; i++) {
            System.err.print(".");
            ByteArrayAccess access = new ByteArrayAccess(file);
            DeltaZip dz = new DeltaZip(access);
            AppendSpecification app_spec =
                    dz.add(versions[i]);
            file = access.applyAppendSpec(app_spec);
        }

        {// Verify contents:
            ByteArrayAccess access = new ByteArrayAccess(file);
            DeltaZip dz = new DeltaZip(access);
            for (int i=versions.length-1; i>=0; i--) {
                System.err.print(",");
                assertEquals(dz.getVersion(), versions[i]);
                if (i>0) {
                    assert(dz.hasPrevious());
                    dz.previous();
                }
            }
            assert(! dz.hasPrevious());
        }
        System.err.println(">");

    }

    //======================================================================

    public static String toString(ByteBuffer buf) {
		String r = new String(DZUtil.allToByteArray(buf.duplicate()));
		return r;
	}

	public static void dump(String s, ByteBuffer buf) {
		dump(s, DZUtil.allToByteArray(buf.duplicate()));
	}

	public static void dump(String s, byte[] buf) {
		System.err.print(s);
		System.err.print("<<");
		for (int i=0; i<buf.length; i++) {
			if (i>0) System.err.print(",");
			System.err.print(buf[i] & 0xff);
		}
		System.err.println(">>");
	}

    public static Version createRandomVersion(int content_length, Random rnd,
                                              int avg_metadata_cnt, int max_metadata_keytag,
                                              int max_metadata_len)
    {
        List<Metadata.Item> metadata = new ArrayList<Metadata.Item>();
        while (rnd.nextInt(avg_metadata_cnt) > 0) {
            metadata.add(new Metadata.Item(1+rnd.nextInt(max_metadata_keytag),
                    createRandomBlob(rnd.nextInt(max_metadata_len), rnd)));
        }
        return new Version(createRandomBinary(content_length, rnd), metadata);
    }

    public static ByteBuffer createRandomBinary(int length, Random rnd) {
        byte[] buf = createRandomBlob(length, rnd);
		return ByteBuffer.wrap(buf).asReadOnlyBuffer();
	}

    private static byte[] createRandomBlob(int length, Random rnd) {
        byte[] buf = new byte[length];
        rnd.nextBytes(buf);
        return buf;
    }
}