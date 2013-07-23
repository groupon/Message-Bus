package com.groupon.messagebus.client;
/*
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
import java.io.UnsupportedEncodingException;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.thrift.api.MessageInternal;

public class Utils{

    private static int MESSAGE_LOG_MAX_LENGTH = 100;
    private static TDeserializer deserializer = new TDeserializer();
    private static TSerializer serializer = new TSerializer();
    private static Logger log = Logger.getLogger(Utils.class);
    private final static String charset = "US-ASCII";

    public static final String NULL_STRING = "null";

    
    static String encode( String clearText){
        String ret = "";
        try {
            byte[] bytes = clearText.getBytes(charset);
            ret = new String(Base64.encodeBase64(bytes), charset);
        } catch (UnsupportedEncodingException e) {
            log.error("Failed to encode message. Non alphanumeric char?");
            ret = clearText;
        }
        
        return ret;
    }
    
    static String decode( String codedText){
        String ret = "";
        try {
            byte[] bytes = codedText.getBytes(charset);
            
            ret = new String(Base64.decodeBase64(bytes), charset);
        } catch (UnsupportedEncodingException e) {
            log.error("Failed to encode message. Non alphanumeric char?");
            ret = codedText;
        }
        
        return ret;
    }
    public static Message getMessageFromBytes(byte[] bytes) {
        try {
            MessageInternal messageInternal = new MessageInternal();
            bytes = Base64.decodeBase64(bytes);
            synchronized(deserializer){
                deserializer.deserialize(messageInternal, bytes);
            }
            return new Message(messageInternal);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read thrift message correctly.", e);
        }
    }

    
    public static byte[] getThriftDataAsBytes(Message message) throws TException, UnsupportedEncodingException {
        byte[] bytes = null;
        synchronized(serializer){
            bytes = serializer.serialize(message.getMessageInternal());
        }
        
        bytes = Base64.encodeBase64(bytes);
        return bytes;
        
    }


    public static String getMessageInternalForLogging(MessageInternal messageInternal) {
        String str = messageInternal.toString();
        if (str.length() > MESSAGE_LOG_MAX_LENGTH)
            str = str.substring(0, MESSAGE_LOG_MAX_LENGTH -1);
        
        return str;
    }
    
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            log.error("Error occurred while resting...", ie);
        }
    }
    
}
