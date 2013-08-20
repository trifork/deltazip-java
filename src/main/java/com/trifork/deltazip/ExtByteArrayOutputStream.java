package com.trifork.deltazip;

import java.io.ByteArrayOutputStream;

/** ByteArrayOutputStream with support for "blanks to be filled in later".
 */
class ExtByteArrayOutputStream extends ByteArrayOutputStream {
    public Gap insertGap(int len) {
        int pos = count;
        for (int i=0; i<len; i++) write(0);
        return new Gap(this, pos, len);
    }

    public void writeBigEndianInteger(int value, int len) {
        for (int i=len-1; i>=0; i--) {
            write(value >> (8*i));
        }
    }

    public static class Gap {
        private final ExtByteArrayOutputStream str;
        private final int pos, size;

        private Gap(ExtByteArrayOutputStream str, int pos, int size) {
            this.str = str;
            this.pos = pos;
            this.size = size;
        }

        public void fillIn(byte[] data) {
            if (data.length != size) {
                throw new IllegalArgumentException("Can't fill gap of size "+size+" with blob of size "+data.length);
            }

            for (int i=0; i<data.length; i++) {
                str.buf[pos+i] = data[i];
            }
        }

        public void fillWithBigEndianInteger(int value, int len) {
            if (size != 4) {
                throw new IllegalArgumentException("Can't fill gap of size "+size+" with blob of size "+len);
            }

            if (len>4) throw new IllegalArgumentException("length is > 4");
            int curpos = pos;
            for (int i=len-1; i>=0; i--) {
                str.buf[curpos++] = (byte) (value >> (8*i));
            }
        }

    }

}
