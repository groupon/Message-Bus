package com.groupon.messagebus.util.test;
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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.util.DynamicServerListGetter;

public class DynamicServerListGetterTest extends TestCase{

    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test
    public void test0_noServersFound() {

        String dataStr = "";

        Set<HostParams> hosts = DynamicServerListGetter.parseAndReturnHosts(dataStr);
        assertEquals(0, hosts.size());
    }

    public void testHex() throws DecoderException {
        byte[] bytes = "dfdfgfddfgfdgdgg".getBytes();
        
        byte[] hex = new Hex().encode(bytes);
        System.out.println(new String(hex));
        assertEquals(new String(bytes), new String(new Hex().decode(hex)));
    }

    @Test
    public void test2_ServersFound() {

        String dataStr = "hornetq1:61613,hornetq2:61613";

        Set<HostParams> hosts = DynamicServerListGetter.parseAndReturnHosts(dataStr);


        int idx = 0;
        for(com.groupon.messagebus.api.HostParams host : hosts){
            if(idx == 0){
                System.out.println(host);
                assertEquals(host.getHost(), "hornetq2");
                assertEquals(host.getPort(), 61613);
            }
            if(idx == 1){
                System.out.println(host);
                assertEquals(host.getHost(), "hornetq1");
                assertEquals(host.getPort(), 61613);
            }
            idx++;
        }
        assertEquals(2, hosts.size());
    }
    
    @Test
    public void test3_BuildServersURL() throws URISyntaxException, MalformedURLException {

        String host = "dummy";
        int port = 8081;
        String url = DynamicServerListGetter.buildDynamicServersURL(host, port);
        String reference = "http://dummy:8081/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer+ListOfBrokers";
        assert(new URL(url).equals( new URL(reference)));
    }
}
