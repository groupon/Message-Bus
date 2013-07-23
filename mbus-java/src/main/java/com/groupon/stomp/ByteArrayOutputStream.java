package com.groupon.stomp;

import java.io.OutputStream;



/**
 * Very similar to the java.io.ByteArrayOutputStream but this version 
 * is not thread safe and the resulting data is returned in a ByteSequence
 * to avoid an extra byte[] allocation.
 * 
 * Copyright (c) 2013, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
public class ByteArrayOutputStream extends OutputStream {

    byte buffer[];
    int size;

    public ByteArrayOutputStream() {
        this(1028);
    }
    public ByteArrayOutputStream(int capacity) {
        buffer = new byte[capacity];
    }

    public void write(int b) {
        int newsize = size + 1;
        checkCapacity(newsize);
        buffer[size] = (byte) b;
        size = newsize;
    }

    public void write(byte b[], int off, int len) {
        int newsize = size + len;
        checkCapacity(newsize);
        System.arraycopy(b, off, buffer, size, len);
        size = newsize;
    }
    
    /**
     * Ensures the the buffer has at least the minimumCapacity specified. 
     * @param i
     */
    private void checkCapacity(int minimumCapacity) {
        if (minimumCapacity > buffer.length) {
            byte b[] = new byte[Math.max(buffer.length << 1, minimumCapacity)];
            System.arraycopy(buffer, 0, b, 0, size);
            buffer = b;
        }
    }

    public void reset() {
        size = 0;
    }

    public ByteSequence toByteSequence() {
        return new ByteSequence(buffer, 0, size);
    }
    
    public byte[] toByteArray() {
        byte rc[] = new byte[size];
        System.arraycopy(buffer, 0, rc, 0, size);
        return rc;
    }
    
    public int size() {
        return size;
    }
}
