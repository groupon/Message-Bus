/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompFrame implements Command {

    public static final byte[] NO_DATA = new byte[] {};

    private String action;
    private Map<String, String> headers = new HashMap<String, String>();
    private byte[] content = NO_DATA;

    public StompFrame(String command) {
        this(command, null, null);
    }

    public StompFrame(String command, Map<String, String> headers) {
        this(command, headers, null);
    }

    public StompFrame(String command, Map<String, String> headers, byte[] data) {
        this.action = command;
        if (headers != null)
            this.headers = headers;
        if (data != null)
            this.content = data;
    }

    public StompFrame() {
    }

    public String getAction() {
        return action;
    }

    public void setAction(String command) {
        this.action = command;
    }

    public byte[] getContent() {
        return content;
    }

    public String getBody() {
        try {
            return new String(content, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return new String(content);
        }
    }

    public void setContent(byte[] data) {
        this.content = data;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    //
    // Methods in the Command interface
    //
    public int getCommandId() {
        return 0;
    }

    public Endpoint getFrom() {
        return null;
    }

    public Endpoint getTo() {
        return null;
    }

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isMessage() {
        return false;
    }

    public boolean isMessageAck() {
        return false;
    }

    public boolean isMessageDispatch() {
        return false;
    }

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isResponse() {
        return false;
    }

    public boolean isResponseRequired() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }

    public boolean isConnectionControl() {
        return false;
    }

    public boolean isWireFormatInfo() {
        return false;
    }

    public void setCommandId(int value) {
    }

    public void setFrom(Endpoint from) {
    }

    public void setResponseRequired(boolean responseRequired) {
    }

    public void setTo(Endpoint to) {
    }

    /*
    public Response visit(CommandVisitor visitor) throws Exception {
        return null;
    }
    */

    public byte getDataStructureType() {
        return 0;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public String toString() {
        return format(true);
    }

    public String format() {
        return format(false);
    }

    public String format(boolean forLogging) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(getAction());
        buffer.append("\n");
        Map headers = getHeaders();
        for (Iterator iter = headers.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            buffer.append(entry.getKey());
            buffer.append(":");
            if (forLogging && entry.getKey().toString().toLowerCase().contains(Stomp.Headers.Connect.PASSCODE)) {
                buffer.append("*****");
            } else {
                buffer.append(entry.getValue());
            }
            buffer.append("\n");
        }
        buffer.append("\n");
        if (getContent() != null) {
            try {
                String contentString = new String(getContent(), "UTF-8");
                if (forLogging) {
                    contentString = MarshallingSupport.truncate64(contentString);
                }
                buffer.append(contentString);
            } catch (Throwable e) {
                buffer.append(Arrays.toString(getContent()));
            }
        }
        return buffer.toString();
    }
}
