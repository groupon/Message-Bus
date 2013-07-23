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
 * 
 */
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.ProducerConfig;
import com.groupon.messagebus.api.exceptions.BrokerConnectionCloseFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.MessageBusException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;
import com.groupon.messagebus.client.ProducerImpl;

public class ProducerBenchmark {
    public static void main(String[] args) throws InvalidConfigException, MessageBusException{
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
        
        ProducerConfig config = new ProducerConfig();
        int msgCount = 1000;
        if(args.length >0 )
            msgCount = Integer.parseInt(args[0]);
        // 61613 is default port for connecting over stomp. Here we either
        // provide direct broker name as host, or DNS name space

        HostParams host = new HostParams("localhost", 6662);

        config.setBroker(host);
        config.setConnectionLifetime(300000);

        // chose between topic (one to many) or queue (one to one)
        config.setDestinationType(DestinationType.TOPIC);
        config.setDestinationName("jms.topic.testTopic2");

        Producer producer = new ProducerImpl();

        producer.start(config);


        StringBuffer messageStr = new StringBuffer("Hello this is an awesome message!");

        for( int i =0; i < 100; ++i)
            messageStr.append("Hello this is an awesome message!");
        Message message = Message.createStringMessage("id-", messageStr.toString());
        
        System.out.println( messageStr.length());
        long start = System.currentTimeMillis();
        for (int i = 0; i < msgCount; i++) {

            try {
                producer.sendSafe(message, null);
            } catch (Exception e) {            
                e.printStackTrace();
            }
                                    
            //System.out.println("Sent:" + message.getStringPayload());
        }
        
        long end = System.currentTimeMillis();
        
        System.out.println("Time used in mili: " + (end-start));
        producer.stop();
    }
}
