package com.trifork.deltazip;

import java.nio.ByteBuffer;
import java.io.IOException;
import com.trifork.deltazip.DeltaZip.AppendSpecification;
import com.trifork.deltazip.DZUtil.ByteArrayAccess;

import java.util.Iterator;
import java.util.LinkedList;

import org.junit.Test;
import static junit.framework.Assert.*;

public class DataArchiveTest {
	
	final ByteBuffer rev1 = ByteBuffer.wrap("Hello, World!".getBytes("ISO-8859-1")).asReadOnlyBuffer();
	final ByteBuffer rev2 = ByteBuffer.wrap("Hello".getBytes("ISO-8859-1")).asReadOnlyBuffer();

	final ByteBuffer rev3 = ByteBuffer.wrap("Hi, hello and welcome".getBytes("ISO-8859-1")).asReadOnlyBuffer();

	public DataArchiveTest() throws Exception {}

	/** Chunked-deflate, using prefix. */
	final byte[] two_revs = {
		(byte)0xCE, (byte)0xB4, 0x7A, 0x10,
		64,0,0,5, 0x05,(byte)0x8C,0x01,(byte)0xF5,
		8,0,2,0, 4,
		64,0,0,5,
		0,0,0,13, 0x1F,(byte)0x9E,0x04,0x6A,
		72,101,108,108,111,44,32,87,111,114,108,100,33,
		0,0,0,13};

	@Test
	public void read_tests() throws Exception {
		DeltaZipDataArchive arch = new DeltaZipDataArchive(two_revs);
		LinkedList<ByteBuffer> expectedContent = new LinkedList<ByteBuffer>();
		expectedContent.add(rev1);
		expectedContent.add(rev2);

		assertEquals(rev1, arch.getLatest());
		verify_iterables(arch, expectedContent);
		// Check that iterator() use hasn't affected cursor:
		assertEquals(rev1, arch.getLatest());
	}

	void verify_iterables(Iterable<ByteBuffer> c1, Iterable<ByteBuffer> c2) {
		Iterator<ByteBuffer> it1 = c1.iterator();
		Iterator<ByteBuffer> it2 = c2.iterator();

		while (true) {
			assertEquals(it1.hasNext(), it2.hasNext());
			if (!it1.hasNext()) break;
			ByteBuffer a = it1.next(), b = it2.next();
			//DeltaZipTest.dump("a=",a);
			//DeltaZipTest.dump("b=",b);
			assertEquals(a, b);
		}
	}

	@Test
	public void write_tests() throws Exception {
		LinkedList<ByteBuffer> expectedContent = new LinkedList<ByteBuffer>();
		expectedContent.add(rev1);
		expectedContent.add(rev2);
		DeltaZipDataArchive arch = new DeltaZipDataArchive(two_revs);
		
		verify_iterables(arch, expectedContent);
		arch.addVersion(rev3);
		expectedContent.addFirst(rev3);
		verify_iterables(arch, expectedContent);

		// Check that getRawData() reflects the added versions:
		ByteBuffer data2 = arch.getRawData();
		DeltaZipDataArchive arch2 = new DeltaZipDataArchive(DZUtil.allToByteArray(data2));
		verify_iterables(arch2, expectedContent);

		// Check that adding two versions also work:
		arch2.addVersion(rev1);
		arch2.addVersion(rev2);
		expectedContent.addFirst(rev1);
		expectedContent.addFirst(rev2);
		ByteBuffer data3 = arch2.getRawData();
		DeltaZipDataArchive arch3 = new DeltaZipDataArchive(DZUtil.allToByteArray(data3));
		verify_iterables(arch3, expectedContent);
	}

}