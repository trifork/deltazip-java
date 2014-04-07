package com.trifork.deltazip;

import java.util.Iterator;
import java.io.IOException;
import java.nio.ByteBuffer;
	
public class DeltaZipDataArchive implements DataArchive {
	private DeltaZip dz;
	private DZUtil.ByteArrayAccess access;
		
	public DeltaZipDataArchive(byte[] archive_data) throws IOException {
		this.access = new DZUtil.ByteArrayAccess(archive_data);
		this.dz = new DeltaZip(access);
	}
		
	public ByteBuffer getLatest() {
		return dz.backwardsIterator().next().getContents();
	}
		
	public void addVersion(ByteBuffer new_version) throws IOException {
		byte[] new_archive_data = access.applyAppendSpec(dz.add(new_version));
		access = new DZUtil.ByteArrayAccess(new_archive_data);
		dz = new DeltaZip(access);
	}

	public ByteBuffer getRawData() {return access.getRawData();}

	public Iterator<ByteBuffer> iterator() {
        final DeltaZip.VersionIterator versionIterator = dz.backwardsIterator();
        return new Iterator<ByteBuffer>() {
            @Override
            public boolean hasNext() { return versionIterator.hasNext(); }

            @Override
            public ByteBuffer next() {
                return versionIterator.next().getContents();
            }

            @Override
            public void remove() { versionIterator.remove(); }
        };
	}

	//======================================================================
    /*
	private static class MyIterator implements Iterator<ByteBuffer> {
		final DeltaZip dz;
		ByteBuffer next;
		public MyIterator(DeltaZip org_dz) throws IOException {
			this.dz = org_dz.clone();
			dz.resetCursor();
			next = dz.get();
		}
		
		@Override
		public boolean hasNext() {return (next!=null);}
		
		@Override
		public ByteBuffer next() {
			ByteBuffer tmp = next;
			if (dz.hasPrevious()) {
				try {
					dz.previous();
				} catch (IOException ioe) {throw new RuntimeException(ioe);}
				next = dz.get();
			} else next=null;
			return tmp;
		}

		@Override
		public void remove() {throw new UnsupportedOperationException();}
	}
	*/

}
	
