package com.trifork.deltazip;

import java.nio.ByteBuffer;

import org.junit.Test;
import static junit.framework.Assert.*;

public class DeltaZipTest {

	@Test
	public void test1() throws Exception {
		byte[] file = {32,0,0,7,0,0,4,(byte)243,
					   0,113,0,32,0,0,7,0,
					   0,0,13,72,101,108,108,111,
					   44,32,87,111,114,108,100,33,
					   0,0,0,13};
		/* With zlib headers:
		byte[] file = {32,0,0,17,0,0,14,120,
					   (byte)249,31,(byte)158,4,106,(byte)243,0,113,
					   0,5,(byte)140,1,(byte)245,32,0,0,
					   17,0,0,0,13,72,101,108,
					   108,111,44,32,87,111,114,108,
					   100,33,0,0,0,13};
		*/

		ByteBuffer exp_rev1 = ByteBuffer.wrap("Hello, World!".getBytes("ISO-8859-1"));
		ByteBuffer exp_rev2 = ByteBuffer.wrap("Hello".getBytes("ISO-8859-1"));

		DeltaZip dz = new DeltaZip(new ByteArrayAccess(file));
		ByteBuffer actual_rev1 = dz.get();
		System.err.println("DB| rev1 = "+toString(actual_rev1));
		assertEquals(exp_rev1, actual_rev1);
		dz.previous();
		ByteBuffer actual_rev2  = dz.get();
		System.err.println("DB| rev2 = "+toString(actual_rev2));
		assertEquals(exp_rev2, actual_rev2);
		try {
			dz.previous();
			throw new RuntimeException("Assertion failed");
		} catch (Exception e) {}
	}

	public static class ByteArrayAccess implements DeltaZip.Access {
		final byte[] data;
		public ByteArrayAccess(byte[] data) {this.data = data;}

		public long getSize() {return data.length;}
		public ByteBuffer pread(long pos, long len) {
			System.err.println("DB| pread("+pos+","+len+")");
			return ByteBuffer.wrap(data, (int)pos, (int)len).slice().asReadOnlyBuffer();
		}
		
	}

	public static String toString(ByteBuffer buf) {
		int save_pos = buf.position();
		String r = new String(DeltaZip.toByteArray(buf));
		buf.position(save_pos);
		return r;
	}
}