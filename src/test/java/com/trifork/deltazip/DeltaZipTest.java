package com.trifork.deltazip;

import java.nio.ByteBuffer;
import java.io.IOException;
import com.trifork.deltazip.DeltaZip.AppendSpecification;
import com.trifork.deltazip.DZUtil.ByteArrayAccess;
import java.util.Random;

import org.junit.Test;
import static junit.framework.Assert.*;

public class DeltaZipTest {

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
			64,0,0,7,0,0,4,(byte)243,
			0,113,0,64,0,0,7,0,
			0,0,13,72,101,108,108,111,
			44,32,87,111,114,108,100,33,
			0,0,0,13};

		/** Chunked-deflate, using prefix. */
		byte[] two_revs2 = {
			(byte)0xCE, (byte)0xB4, 0x7A, 0x10,
			64,0,0,5,8,0,2,0,
			4,64,0,0,5,0,0,0,
			13,72,101,108,108,111,44,32,
			87,111,114,108,100,33,0,0,
			0,13};

		ByteBuffer exp_rev1 = ByteBuffer.wrap("Hello, World!".getBytes("ISO-8859-1"));
		ByteBuffer exp_rev2 = ByteBuffer.wrap("Hello".getBytes("ISO-8859-1"));

		test_two_revs_with(two_revs1, exp_rev1, exp_rev2);
		test_two_revs_with(two_revs2, exp_rev1, exp_rev2);
	}

	public void test_two_revs_with(byte[] file, ByteBuffer exp_rev1, ByteBuffer exp_rev2) throws IOException {
		DeltaZip dz = new DeltaZip(new ByteArrayAccess(file));
		ByteBuffer actual_rev1 = dz.get();
		assertEquals(exp_rev1, actual_rev1);
		dz.previous();
		ByteBuffer actual_rev2  = dz.get();
		assertEquals(exp_rev2, actual_rev2);
		try {
			dz.previous();
			throw new RuntimeException("Assertion failed");
		} catch (Exception e) {}
	}



	@Test
	public void test_add_get() throws Exception {
		ByteBuffer rev1 = ByteBuffer.wrap("Hello".getBytes("ISO-8859-1"));
		ByteBuffer rev1a = ByteBuffer.wrap("World!".getBytes("ISO-8859-1"));
		ByteBuffer rev1b = ByteBuffer.wrap("Held!".getBytes("ISO-8859-1"));
		ByteBuffer rev2 = ByteBuffer.wrap("Hello, World!".getBytes("ISO-8859-1"));

		byte[] file0 = new byte[] {};

		test_add_get_with(file0, rev1,rev2); // Would use 'prefix-copy' chunk.
		test_add_get_with(file0, rev1a,rev2); // Would use 'offset-copy' chunk.
		test_add_get_with(file0, rev1b,rev2); // Would use 'prefix-copy' and 'offset-copy'.
		test_add_get_with(file0, rev2,rev1); // Would use 'deflate' chunk.
	}
	
	public void test_add_get_with(byte[] file0, ByteBuffer rev1, ByteBuffer rev2) throws IOException {
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

		// Tests:
		assertEquals(dz1.get(), rev1);

		assertEquals(dz2.get(), rev2);
		dz2.previous();
		assertEquals(dz2.get(), rev1);

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
				byte[] tmp = DeltaZip.allToByteArray(versions[i-1]);
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
				byte[] tmp = DeltaZip.allToByteArray(versions[i-1]);

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

	public static String toString(ByteBuffer buf) {
		String r = new String(DeltaZip.allToByteArray(buf.duplicate()));
		return r;
	}

	public static void dump(String s, ByteBuffer buf) {
		dump(s, DeltaZip.allToByteArray(buf.duplicate()));
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

	public static ByteBuffer createRandomBinary(int length, Random rnd) {
		byte[] buf = new byte[length];
		rnd.nextBytes(buf);
		return ByteBuffer.wrap(buf).asReadOnlyBuffer();
	}
}