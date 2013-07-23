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

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;

import com.groupon.messagebus.api.Consumer;
import com.groupon.messagebus.api.ConsumerAckType;
import com.groupon.messagebus.api.ConsumerConfig;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Log;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.client.ConsumerImpl;
import com.groupon.messagebus.api.Message;

public class SimpleClientIntegration {

    /**
     * @param args
     * @throws InterruptedException
     * @throws MessageBusException 
     */
    public static void main(String[] args) throws InterruptedException, MessageBusException  {
        ConsumerConfig config = new ConsumerConfig();
        config.setConnectionLifetime(300000);
        HostParams host = new HostParams("localhost", 61613);
        Set<HostParams> hostsList = new HashSet<HostParams>();
        hostsList.add(host);
        
        config.setDestinationType(DestinationType.TOPIC);
        //config.setDestinationType(DestinationType.QUEUE);
        config.setDestinationName("jms.topic.testTopic2");
        //config.setDestinationName("jms.queue.testQueue1");        
                        
        config.setHostParams(hostsList);        
        config.setSubscriptionId("client-1");
        config.setAckType(ConsumerAckType.CLIENT_ACK);
        config.setConnectionLifetime(40000);

        BasicConfigurator.configure();

        Consumer con = new ConsumerImpl();
        try {
            con.start(config);
        } catch (InvalidConfigException e) {
            e.printStackTrace();
        }



        Log.log("connected");
        long startTime = System.currentTimeMillis();
        for(int i =0 ; i< 100; i++){
            Message tmp = con.receive();
            con.ackSafe();
            //con.nack() - discards last sent message and adds it back to the queue
            //con.nack(message.getAckId()) - discards 'message' and adds it back to the queue
            Log.log(tmp.getMessageId());

            if((i%200)==0){
                //Log.log(new String(tmp));
                double difference = System.currentTimeMillis() - startTime;
                startTime = System.currentTimeMillis();
                if(difference != 0){
                    double took = 200 * 1000/ (difference);
                    Log.log("Consumed 200 messages in "+ difference+ " ms at "+ took + "messages/second Total Consumed="+i);
                }
            }
        }

        con.stop();  
        System.exit(0);

    }

}
