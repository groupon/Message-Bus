package com.groupon.stomp;
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
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

import com.groupon.messagebus.api.exceptions.InvalidDestinationException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.api.exceptions.SendFailedException;
import com.groupon.messagebus.client.Utils;

public class StompConnection {

    // Set this to appropriate value for now set to infinity
    public static final long RECEIVE_TIMEOUT = 30000;

    private Logger log = Logger.getLogger(StompConnection.class);
    private boolean connected = false;
    private Socket stompSocket;
    private ByteArrayOutputStream inputBuffer = new ByteArrayOutputStream();

    public void open(String host, int port) throws IOException,
            UnknownHostException {
        Socket sock = new Socket(host, port);
        sock.setTcpNoDelay(true);
        open(sock);
    }

    public void open(Socket socket) {
        stompSocket = socket;
        connected = true;

    }

    public void close() throws IOException {
        if (stompSocket != null) {
            if (!stompSocket.isClosed())
                stompSocket.close();
            stompSocket = null;
            connected = false;
        }
    }

    public void sendFrame(String data) throws IOException {
        byte[] bytes = data.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        outputStream.write(bytes);
        outputStream.write(0);
        outputStream.flush();
    }

    public void sendFrame(String frame, byte[] data) throws IOException {
        byte[] bytes = frame.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        outputStream.write(bytes);
        outputStream.write(data);
        outputStream.write(0);
        outputStream.flush();
    }

    public StompFrame receive() throws IOException {
        return receive(RECEIVE_TIMEOUT);
    }

    public StompFrame receive(long timeOut) throws IOException {
        stompSocket.setSoTimeout((int) timeOut);
        InputStream is = stompSocket.getInputStream();
        StompWireFormat wf = new StompWireFormat();
        DataInputStream dis = new DataInputStream(is);
        return (StompFrame) wf.unmarshal(dis);
    }

    public String receiveFrame() throws IOException {
        return receiveFrame(RECEIVE_TIMEOUT);
    }

    public String receiveFrame(long timeOut) throws IOException {
        stompSocket.setSoTimeout((int) timeOut);
        InputStream is = stompSocket.getInputStream();
        int c = 0;
        for (;;) {
            c = is.read();
            if (c < 0) {
                throw new IOException("socket closed.");
            } else if (c == 0) {
                c = is.read();
                if (c == '\n') {
                    // end of frame
                    return stringFromBuffer(inputBuffer);
                } else {
                    inputBuffer.write(0);
                    inputBuffer.write(c);
                }
            } else {
                inputBuffer.write(c);
            }
        }
    }

    public boolean isConnected() {
        if (stompSocket == null || !stompSocket.isConnected()) {
            connected = false;
        }

        return connected;
    }

    private String stringFromBuffer(ByteArrayOutputStream inputBuffer)
            throws IOException {
        byte[] ba = inputBuffer.toByteArray();
        inputBuffer.reset();
        return new String(ba, "UTF-8");
    }

    public Socket getStompSocket() {
        return stompSocket;
    }

    public void setStompSocket(Socket stompSocket) {
        this.stompSocket = stompSocket;
    }

    public void connect(String username, String password) throws IOException,
            MessageBusException {
        connect(username, password, null);
    }

    public void connect(String username, String password, String client)
            throws IOException, MessageBusException {
        Map<String, String> headers = new HashMap();
        headers.put("login", username);
        headers.put("passcode", password);
        if (client != null) {
            headers.put("client-id", client);
        }
        StompFrame frame = new StompFrame("CONNECT", headers);
        sendFrame(frame.format());

        StompFrame connect = receive();
        if (!connect.getAction().equals(Stomp.Responses.CONNECTED)) {
            throw new MessageBusException("Not connected to server: "
                    + connect.getBody());
        }
    }

    public void disconnect() throws IOException {

        StompFrame frame = new StompFrame("DISCONNECT");
        sendFrame(frame.format());
    }

    public void sendSafe(String destination, String message,
            Map<String, String> headers) throws IOException,
            SendFailedException {
        sendSafe(destination, message.getBytes(), headers);
    }

    public void sendSafe(String destination, byte[] message,
            Map<String, String> headers) throws IOException,
            SendFailedException {
        // Add receipt header to the headers.
        if (headers == null) {
            headers = new HashMap<String, String>();
        }
        headers.put("receipt", "1");

        // send the message.
        send(destination, message, headers);

        // Wait for the
        StompFrame connect = receive();
        if (!connect.getAction().equals(Stomp.Responses.RECEIPT)) {
            throw new SendFailedException("Failed to receive RECEIPT: "
                    + connect.getBody());
        }
        log.info("Message sent receipt acknowledged for destination:"
                + destination);
    }

    public void send(String destination, String message,
            Map<String, String> headers) throws IOException {
        send(destination, message.getBytes(), null, headers);
    }

    public void send(String destination, byte[] message,
            Map<String, String> headers) throws IOException {
        send(destination, message, null, headers);
    }

    public void send(String destination, byte[] message, String transaction,
            Map<String, String> headers) throws IOException {
        if (headers == null) {
            headers = new HashMap<String, String>();
        }
        headers.put("destination", destination);
        headers.put("persistent", "true");
        if (transaction != null) {
            headers.put("transaction", transaction);
        }

        StompFrame frame = new StompFrame("SEND", headers, message);
        sendFrame(frame.format());
    }

    public void subscribe(String destination) throws IOException {
        subscribe(destination, null, null);
    }

    public void subscribe(String destination, String ack) throws IOException {
        subscribe(destination, ack, new HashMap<String, String>());
    }

    public void subscribe(String destination, String ack,
            Map<String, String> headers) throws IOException{
        if (headers == null) {
            headers = new HashMap<String, String>();
        }
        headers.put("destination", destination);

        if (ack != null) {
            headers.put("ack", ack);
        }
        StompFrame frame = new StompFrame("SUBSCRIBE", headers);
        sendFrame(frame.format());
    }

    public void unsubscribe(String destination) throws IOException {
        unsubscribe(destination, null);
    }

    public void unsubscribe(String destination, Map<String, String> headers)
            throws IOException {
        if (headers == null) {
            headers = new HashMap<String, String>();
        }
        headers.put("destination", destination);

        StompFrame frame = new StompFrame("UNSUBSCRIBE", headers);
        sendFrame(frame.format());
    }

    public void begin(String transaction) throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("transaction", transaction);
        StompFrame frame = new StompFrame("BEGIN", headers);
        sendFrame(frame.format());
    }

    public void abort(String transaction) throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("transaction", transaction);
        StompFrame frame = new StompFrame("ABORT", headers);
        sendFrame(frame.format());
    }

    public void commit(String transaction) throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("transaction", transaction);
        StompFrame frame = new StompFrame("COMMIT", headers);
        sendFrame(frame.format());
    }

    public void ack(String messageId) throws IOException {
        ack(messageId, null, null, null, null);
    }

    public void ack(String messageId, String receiptId) throws IOException {
        ack(messageId, null, null, null, receiptId);
    }

    public void ack(String messageId, String transaction,
            String subscriptionId, String connectionId, String receiptId)
            throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("message-id", messageId);
        log.debug("acking message-id: " + messageId);
        if (transaction != null)
            headers.put("transaction", transaction);
        if (subscriptionId != null && !subscriptionId.equals(""))
            headers.put("subscription", subscriptionId);

        if (connectionId != null && !connectionId.equals(Utils.NULL_STRING))
            headers.put("connection-id", connectionId);

        if (receiptId != null && !receiptId.equals(Utils.NULL_STRING))
            headers.put("receipt", receiptId);

        StompFrame frame = new StompFrame("ACK", headers);
        sendFrame(frame.format());
    }

    public void keepAlive() throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        StompFrame frame = new StompFrame("KEEPALIVE", headers);
        sendFrame(frame.format());
    }

    public void credit(StompFrame frame) throws IOException {
        credit(frame.getHeaders().get("message-id"));
    }

    public void credit(String messageId) throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("message-id", messageId);
        StompFrame frame = new StompFrame("CREDIT", headers);
        sendFrame(frame.format());
    }

    protected String appendHeaders(HashMap<String, Object> headers) {
        StringBuffer result = new StringBuffer();
        for (String key : headers.keySet()) {
            result.append(key + ":" + headers.get(key) + "\n");
        }
        result.append("\n");
        return result.toString();
    }

    public void nack(String messageId, String subscriptionId)
            throws IOException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("message-id", messageId);
        if (subscriptionId != null && !subscriptionId.equals(""))
            headers.put("subscription", subscriptionId);

        StompFrame frame = new StompFrame("NACK", headers);
        sendFrame(frame.format());
    }

}
