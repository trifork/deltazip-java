package com.trifork.deltazip;

import java.io.ByteArrayOutputStream;

/** ByteArrayOutputStream with support for "blanks to be filled in later".
 */
class ExtByteArrayOutputStream extends ByteArrayOutputStream {
    public int insertBlank(int len) {
        int pos = count;
        for (int i=0; i<len; i++) write(0);
        return pos;
    }

    public void fillBlank(int pos, byte[] data) {
        for (int i=0; i<data.length; i++) {
            buf[pos+i] = data[i];
        }
    }

    public void fillBlankWithBigEndianInteger(int pos, int value, int len) {
        if (len>4) throw new IllegalArgumentException("length is > 4");
        for (int i=len-1; i>=0; i--) {
            buf[pos++] = (byte) (value >> (8*i));
        }
    }

    public void writeBigEndianInteger(int value, int len) {
        for (int i=len-1; i>=0; i--) {
            write(value >> (8*i));
        }
    }
}
