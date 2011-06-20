package com.trifork.deltazip;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;
import com.trifork.deltazip.DeltaZip.AppendSpecification;

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
			32,0,0,7,0,0,4,(byte)243,
			0,113,0,32,0,0,7,0,
			0,0,13,72,101,108,108,111,
			44,32,87,111,114,108,100,33,
			0,0,0,13};

		/** Chunked-deflate, using prefix. */
		byte[] two_revs2 = {
			32,0,0,5,8,0,2,0,
			4,32,0,0,5,0,0,0,
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
		//test_add_get_with(file0, rev2,rev1); // Would use 'deflate' chunk.
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
		byte[] file2 = access0.applyAppendSpec(app2);
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


	//======================================================================

	public static class ByteArrayAccess implements DeltaZip.Access {
		final byte[] data;
		public ByteArrayAccess(byte[] data) {this.data = data;}

		public long getSize() {return data.length;}
		public ByteBuffer pread(long pos, long len) {
			System.err.println("DB| pread("+pos+","+len+") of "+data.length);
			return ByteBuffer.wrap(data, (int)pos, (int)len).slice().asReadOnlyBuffer();
		}
	
		public byte[] applyAppendSpec(DeltaZip.AppendSpecification spec) {
			try {
				int length = (int) (spec.prefix_size + spec.new_tail.remaining());
				ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
				baos.write(data);
				WritableByteChannel channel = Channels.newChannel(baos);
				channel.write(spec.new_tail);
				channel.close();
				return baos.toByteArray();
			} catch (IOException ioe) {throw new RuntimeException(ioe);}
		}
	}

	public static String toString(ByteBuffer buf) {
		int save_pos = buf.position();
		String r = new String(DeltaZip.toByteArray(buf));
		buf.position(save_pos);
		return r;
	}

	public static void dump(String s, ByteBuffer buf) {
		dump(s, DeltaZip.toByteArray(buf));
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
}