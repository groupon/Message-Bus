package com.groupon.messagebus.client;

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
