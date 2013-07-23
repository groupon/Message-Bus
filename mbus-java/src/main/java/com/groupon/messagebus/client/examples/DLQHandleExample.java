package com.groupon.messagebus.client.examples;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;

import com.groupon.messagebus.api.Consumer;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Log;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.client.ConsumerImpl;

public class DLQHandleExample {

    /**
     * @param args
     * @throws InterruptedException 
     * @throws InvalidStatusException 
     */
    public static void main(String[] args) throws InterruptedException, InvalidStatusException {
        // TODO Auto-generated method stub
        ConsumerConfig config = new ConsumerConfig();
        config.setConnectionLifetime(300000);

        // default host to connect to
        HostParams host = new HostParams("localhost", 6661);
        Set<HostParams> hostsList = new HashSet<HostParams>();
        hostsList.add(host);

        //config.setDynamicServerListFetchURL("http://mbus-consumers-list.tm:8081/jmx?command=get_attribute&args=org.hornetq%3Amodule%3DCore%2Ctype%3DServer%20ListOfBrokers");
        config.setDestinationType(DestinationType.QUEUE);
        config.setHostParams(hostsList);
        config.setDestinationName("jms.queue.DLQ");
        config.setAckType(ConsumerAckType.CLIENT_ACK);
        config.setConnectionLifetime(300000);
        config.setSubscriptionId("client-xxx");

        BasicConfigurator.configure();

        Consumer con = new ConsumerImpl();
        try {
            con.start(config);
        } catch (InvalidConfigException e) {
            e.printStackTrace();
        }
        Thread.sleep(500);
        Log.log("connected");
        long startTime = System.currentTimeMillis();
        ArrayList<String> ackIDs = new ArrayList<String>();
        for (int i = 0; i<10; i++) {

            try{
                Message tmp = con.receive();
                Thread.sleep(1);
                if(tmp == null) {
                    Log.log("received null.");
                    continue;
                }
                else
                    Log.log("received something."+tmp.getMessagePayloadType());
                
                if(i%2 == 0)
                ackIDs.add(tmp.getAckId());
                 // con.ack();
                switch(tmp.getMessagePayloadType()){
                case STRING:
                    Log.log(tmp.getStringPayload());
                    break;
                case JSON:
                    Log.log(tmp.getJSONStringPayload());
                    break;
                case BINARY: 
                    Log.log(new String(tmp.getBinaryPayload()));
                    break;
                }
                
            }catch( Exception e)
            {
                Log.log( "receive expired");
            }
            System.out.println();
            // if not in auto ack mode
        }
    }

}
