package com.groupon.messagebus.api;
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
import java.util.Map;
import java.util.UUID;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.gson.GsonBuilder;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.thrift.api.MessageInternal;
import com.groupon.messagebus.thrift.api.MessagePayload;
import com.groupon.messagebus.thrift.api.MessagePayloadType;

public class Message {

    private final MessageInternal messageInternal;
    // Used to encode host address and message-id. Only used by consumer.
    private String ackId;

    public String getAckId() {
        return ackId;
    }

    public void setAckId(String ackId) {
        this.ackId = ackId;
    }

    private Logger log = Logger.getLogger(Message.class);
    
    private static String getSaltedMessageId(){
        String ret = "";
        try{
            String seed = UUID.randomUUID().toString();
            byte[] bytes =MessageDigest.getInstance("MD5").digest( seed.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            ret = sb.toString();
        }
        catch(NoSuchAlgorithmException e){
            ret = UUID.randomUUID().toString();
        }
        return ret;
    }
    
    public static Message createStringMessage(String messagePayload) {
        return createStringMessage(getSaltedMessageId(), messagePayload);
    }

    public static Message createStringMessage(String messageId,
            String messagePayload) {
        // Create a MessagePayload with string payload type.
        MessagePayload payload = new MessagePayload(MessagePayloadType.STRING);
        payload.setStringPayload(messagePayload);

        return new Message(new MessageInternal(messageId, payload));
    }

    public static Message createBinaryMessage(byte[] binaryPayload) {
        
        return createBinaryMessage(getSaltedMessageId(), binaryPayload);
    }
    
    public static Message createBinaryMessage(String messageId,
            byte[] binaryPayload) {
        // Create a MessagePayload with binary payload type.
        MessagePayload payload = new MessagePayload(MessagePayloadType.BINARY);
        payload.setBinaryPayload(binaryPayload);

        return new Message(new MessageInternal(messageId, payload));
    }

    public static Message createJsonMessage(Object objectPayload){
        return createJsonMessage(getSaltedMessageId(), objectPayload);
    }
            
    
    public static Message createJsonMessage(String messageId,
            Object objectPayload) {
        // convert the passed object to its json representation using
        // google-gson library.
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .create();
        String jsonData = gson.toJson(objectPayload);

        // Create a MessagePayload with json string payload type.
        MessagePayload payload = new MessagePayload(MessagePayloadType.JSON);
        payload.setStringPayload(jsonData);

        return new Message(new MessageInternal(messageId, payload));
    }
    
    public static Message createJsonStringMessage(String jsonString){
        return createJsonStringMessage(getSaltedMessageId(), jsonString);
    }

    public static Message createJsonStringMessage(String messageId,
            String jsonString) {
        // Create a MessagePayload with json string payload type.
        MessagePayload payload = new MessagePayload(MessagePayloadType.JSON);
        payload.setStringPayload(jsonString);

        return new Message(new MessageInternal(messageId, payload));
    }

    public Message(MessageInternal messageInternal) {
        this.messageInternal = messageInternal;
    }

    public String getMessageId() {
        return messageInternal.getMessageId();
    }

    public MessagePayloadType getMessagePayloadType() {
        return messageInternal.getPayload().getMessageFormat();
    }

    public String getJSONStringPayload() throws MessageBusException {
        MessagePayloadType payloadFormat = messageInternal.getPayload()
                .getMessageFormat();
        if (!MessagePayloadType.JSON.equals(payloadFormat)) {
            throw new MessageBusException("Cannot get KeyValue payload from "
                    + payloadFormat + " payload type message.");
        }

        return messageInternal.getPayload().getStringPayload();
    }

    public String getStringPayload() throws MessageBusException {
        MessagePayloadType payloadFormat = messageInternal.getPayload()
                .getMessageFormat();
        if (!MessagePayloadType.STRING.equals(payloadFormat)) {
            throw new MessageBusException("Cannot get String payload from "
                    + payloadFormat + " payload type message.");
        }

        return messageInternal.getPayload().getStringPayload();
    }

    public byte[] getBinaryPayload() throws MessageBusException {
        MessagePayloadType payloadFormat = messageInternal.getPayload()
                .getMessageFormat();
        if (!MessagePayloadType.BINARY.equals(payloadFormat)) {
            throw new MessageBusException("Cannot get Binary payload from "
                    + payloadFormat + " payload type message.");
        }

        return messageInternal.getPayload().getBinaryPayload();
    }

    public void setMessageProperties(Map<String, String> properties) {
        messageInternal.setProperties(properties);
    }

    public Map<String, String> getMessageProperties() {
        return messageInternal.getProperties();
    }

    public MessageInternal getMessageInternal() {
        return messageInternal;
    }
}
