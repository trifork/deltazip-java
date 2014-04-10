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
import static org.junit.Assert.*;

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

        { // Test with full backwards iterator:
            DeltaZip.VersionIterator iter = dz.backwardIterator();

            assertTrue(iter.hasNext());
            Version actual1 = iter.next();
            assertEquals(exp_rev1, actual1.getContents());
            assertEquals(exp_md1, actual1.getMetadata());

            assertTrue(iter.hasNext());
            Version actual2 = iter.next();
            assertEquals(exp_rev2, actual2.getContents());
            assertEquals(exp_md2, actual2.getMetadata());

            assertFalse(iter.hasNext());
        }

        { // Test backwards metadata-only iterator:
            DeltaZip.MetadataIterator iter = dz.backwardMetadataIterator();

            assertTrue(iter.hasNext());
            List<Metadata.Item> actual_md1 = iter.next();
            assertEquals(exp_md1, actual_md1);

            assertTrue(iter.hasNext());
            List<Metadata.Item> actual_md2 = iter.next();
            assertEquals(exp_md2, actual_md2);

            assertFalse(iter.hasNext());
        }

        { // Test forward metadata-only iterator:
            DeltaZip.MetadataIterator iter = dz.forwardMetadataIterator();

            assertTrue(iter.hasNext());
            List<Metadata.Item> actual_md2 = iter.next();
            assertEquals(exp_md2, actual_md2);

            assertTrue(iter.hasNext());
            List<Metadata.Item> actual_md1 = iter.next();
            assertEquals(exp_md1, actual_md1);

            assertFalse(iter.hasNext());
        }
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

    private void test_add_get_with(byte[] file0, Version rev1, Version rev2) throws IOException {
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
        { // ...of iterator (0 version - tests emptiness case):
            DeltaZip.VersionIterator iter0 = dz0.backwardIterator();
            assertFalse(iter0.hasNext());

            DeltaZip.MetadataIterator mditer0 = dz0.backwardMetadataIterator();
            assertFalse(mditer0.hasNext());
            DeltaZip.MetadataIterator mditer0b = dz0.backwardMetadataIterator();
            assertFalse(mditer0b.hasNext());

            DeltaZip.MetadataIterator mditer0f = dz0.forwardMetadataIterator();
            assertFalse(mditer0f.hasNext());
        }

        { // ...of iterator (1 version):
            DeltaZip.VersionIterator iter1 = dz1.backwardIterator();
            assertTrue(iter1.hasNext());
            assertEquals(rev1, iter1.next());
            assertFalse(iter1.hasNext());
        }

        { // ...of iteration (2 versions - tests iteration order):
            DeltaZip.VersionIterator iter2 = dz2.backwardIterator();
            assertTrue(iter2.hasNext());
            assertEquals(rev2, iter2.next()); // Get rev2 first!
            assertTrue(iter2.hasNext());
            assertEquals(rev1, iter2.next());
            assertFalse(iter2.hasNext());
        }

        { // Of independence of iterators:
            DeltaZip.VersionIterator iter2a = dz2.backwardIterator();
            DeltaZip.VersionIterator iter2b = dz2.backwardIterator();
            assertTrue(iter2a.hasNext());
            assertTrue(iter2b.hasNext());

            // First read iter2a:
            assertEquals(iter2a.next(), rev2);
            assertEquals(iter2a.next(), rev1);

            assertFalse(iter2a.hasNext());
            assertTrue(iter2b.hasNext());

            // Then read iter2b:
            assertEquals(iter2b.next(), rev2);
            assertEquals(iter2b.next(), rev1);
        }
        { // Test latestVersion():
            assertEquals(null, dz0.latestVersion());
            assertEquals(rev1, dz1.latestVersion());
            assertEquals(rev2, dz2.latestVersion());
        }
	}

    @Test
    public void test_add_empty() throws Exception {
        byte[] file0 = new byte[] {};
        ByteArrayAccess access0 = new ByteArrayAccess(file0);
        DeltaZip dz0 = new DeltaZip(access0);
        assertFalse(dz0.backwardIterator().hasNext());

        // Add zero versions to the empty archive (without header):
        AppendSpecification app1 = dz0.add(Collections.<Version>emptyList().iterator());
        byte[] file1 = access0.applyAppendSpec(app1);
        assertEquals("Only header", 4, file1.length);

        // Add zero versions to the empty archive (with header):
        ByteArrayAccess access1 = new ByteArrayAccess(file1);
        DeltaZip dz1 = new DeltaZip(access1);
        AppendSpecification app2 = dz1.add(Collections.<Version>emptyList().iterator());
        byte[] file2 = access1.applyAppendSpec(app2);
        assertEquals("Still only header", 4, file2.length);
        assertArrayEquals("Unchanged", file1, file2);

        // Add something:
        ByteArrayAccess access2 = new ByteArrayAccess(file2);
        DeltaZip dz2 = new DeltaZip(access2);
        byte[] rev_in = {0, 1, 2, 3, -2, -1};
        AppendSpecification app3 = dz2.add(Collections.singletonList(new Version(rev_in)));
        byte[] file3 = access2.applyAppendSpec(app3);

        // Verify addition:
        ByteArrayAccess access3 = new ByteArrayAccess(file3);
        DeltaZip dz3 = new DeltaZip(access3);
        DeltaZip.VersionIterator iter3 = dz3.backwardIterator();
        ByteBuffer rev_out = iter3.next().getContents();
        assertArrayEquals("in==out", rev_in, DZUtil.allToByteArray(rev_out));
        assertFalse(iter3.hasNext());

        // Add zero versions to a non-empty archive:
        AppendSpecification app4 = dz3.add(Collections.<Version>emptyList().iterator());
        byte[] file4 = access3.applyAppendSpec(app4);
        assertArrayEquals("Unchanged", file3, file4);
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

				int nMutations = rnd.nextInt(20);
				int nRuns = rnd.nextInt(10);
				int nSwaps = rnd.nextInt(10);
                mutate(tmp, nMutations, nRuns, nSwaps, rnd);

                versions[i] = ByteBuffer.wrap(tmp);
			}
			
			series_test_with(versions);
		}
	}

    @Test
    public void somewhat_related_branch_test() throws IOException {
        final Random rnd = new Random();
        final int maxMetadataItems = 20;

        for (int test_nr=0; test_nr<200; test_nr++) {
            int nVersions1 = rnd.nextInt(10); // [0;9]
            int nVersions2 = rnd.nextInt(10); // [0;9]
            int prefix_length = rnd.nextInt(nVersions1+1); // [0;nVersions1]
            int versionSize = rnd.nextInt(1000); // Up to 1K.

            Version seed_version = new Version(createRandomBinary(versionSize, rnd), Collections.<Metadata.Item>emptyList());
            List<Version> versions1 = createRelatedVersionSequence(seed_version, nVersions1, maxMetadataItems, rnd);
            Version branch_version = prefix_length > 0 ? versions1.get(prefix_length-1) : seed_version;
            List<Version> versions2 = createRelatedVersionSequence(branch_version, nVersions2, maxMetadataItems, rnd);

            branch_test_with(versions1, prefix_length, versions2);
        }
    }

    private List<Version> createRelatedVersionSequence(Version prev_version, int version_count, int maxMetadataItemCount, Random rnd) {
        ArrayList<Version> versions = new ArrayList<Version>(version_count);
        for (int i=0; i<version_count; i++) {
            byte[] tmp = DZUtil.allToByteArray(prev_version.getContents());

            int nMutations = rnd.nextInt(20);
            int nRuns = rnd.nextInt(10);
            int nSwaps = rnd.nextInt(10);
            mutate(tmp, nMutations, nRuns, nSwaps, rnd);

            versions.add(new Version(tmp, MetadataTest.randomMetadata(maxMetadataItemCount, 20, rnd)));
        }
        return versions;
    }

    private void mutate(byte[] data, int nMutations, int nRuns, int nSwaps, Random rnd) {
        if (data.length==0) return;

        // Single-byte mutations:
        for (int j=0; j<nMutations; j++)
            data[rnd.nextInt(data.length)] = (byte) rnd.nextInt(256);

        // Random runs:
        for (int j=0; j<nRuns; j++) {
            int start = 0, end = data.length;
            int iters = rnd.nextInt(10);
            for (int k=0; k<iters && start<end; k++) { // Select part.
                int mid = start + rnd.nextInt(end - start);
                if (rnd.nextBoolean()) start=mid; else end=mid;
            }

            for (int k=start; k<end; k++) data[k] = (byte)rnd.nextInt(256);
        }

        // Block swaps:
        for (int j=0; j<nSwaps; j++) {
            int pos2 = rnd.nextInt(data.length+1);
            int pos1 = rnd.nextInt(pos2+1);
            int pos3 = rnd.nextInt(data.length-pos2+1)+pos2;
            byte[] tmp = new byte[pos3-pos1];
            System.arraycopy(data,pos1, tmp,0, pos3-pos1);
            System.arraycopy(tmp,pos2-pos1, data,pos1, pos3-pos2);
            System.arraycopy(tmp,0, data,pos1+(pos3-pos2), pos2-pos1);
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

        assertArchiveContentsEquals(versions, file);
        System.err.println(">");
	}

    private void branch_test_with(List<Version> versions1, int prefix_length, List<Version> versions2) throws IOException {
        byte[] file0 = new byte[0];
        final byte[] file1;
        { // Build origin archive:
            ByteArrayAccess access = new ByteArrayAccess(file0);
            DeltaZip dz = new DeltaZip(access);

            AppendSpecification app_spec = dz.add(versions1);
            file1 = access.applyAppendSpec(app_spec);
        }
        final byte[] file2;
        { // Build branched archive:
            ByteArrayAccess access = new ByteArrayAccess(file1);
            DeltaZip dz = new DeltaZip(access);
            DeltaZip.MetadataIterator iter = dz.forwardMetadataIterator();
            for (int i=0; i<prefix_length; i++) {
                iter.next();
            }
            DeltaZip.Position branchPosition = iter.getVersionEndPosition();
            AppendSpecification app_spec = dz.add(branchPosition, versions2);
            file2 = access.applyAppendSpec(app_spec);
        }
        { // Verify contents:
            Version[] expectedContents = new Version[prefix_length + versions2.size()];
            {
                int pos=0;
                for (int i=0; i<prefix_length; i++) {
                    expectedContents[pos++] = versions1.get(i);
                }
                for (int i=0; i<versions2.size(); i++) {
                    expectedContents[pos++] = versions2.get(i);
                }
            }

            ByteArrayAccess access = new ByteArrayAccess(file2);
            DeltaZip dz = new DeltaZip(access);
            //System.err.println("Versions1 ("+versions1.size()+"): "+versions1);
            //System.err.println("Versions2 ("+versions2.size()+"): "+versions2);
            //System.err.println("Prefix length: "+prefix_length);
            ArrayList<Version> result = new ArrayList<Version>();
            for (Version version : dz.backwardIterable()) {
                result.add(version);
            }

            //System.err.println("Resulting archive: "+result);
            assertArchiveContentsEquals(expectedContents, dz);
        }
    }

    private void assertArchiveContentsEquals(ByteBuffer[] versions, byte[] file) throws IOException {
        // Verify contents:
        ByteArrayAccess access = new ByteArrayAccess(file);
        DeltaZip dz = new DeltaZip(access);
        int i=versions.length-1;
        for (Version version : dz.backwardIterable()) {
            assertEquals(version.getContents(), versions[i]);
            i--;
        }
        assertEquals("There are only the expected number of versions", i, -1);
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

        ByteArrayAccess access = new ByteArrayAccess(file);
        DeltaZip dz = new DeltaZip(access);
        assertArchiveContentsEquals(versions, dz);

        { // Verify metadata backwards:
            int i=versions.length-1;
            for (List<Metadata.Item> metadata : dz.backwardMetadataIterable()) {
                assertEquals(metadata, versions[i].getMetadata());
                i--;
            }
            assertEquals("There are only the expected number of versions", i, -1);
        }
        { // Verify metadata forwards:
            int i=0;
            for (List<Metadata.Item> metadata : dz.forwardMetadataIterable()) {
                assertEquals(metadata, versions[i].getMetadata());
                i++;
            }
            assertEquals("There are only the expected number of versions", i, versions.length);
        }
        System.err.println(">");

    }

    private void assertArchiveContentsEquals(Version[] versions, DeltaZip dz) {
        // Verify contents:
        int i=versions.length-1;
        for (Version version : dz.backwardIterable()) {
            assertEquals(version, versions[i]);
            i--;
        }
        assertEquals("There are only the expected number of versions", i, -1);
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