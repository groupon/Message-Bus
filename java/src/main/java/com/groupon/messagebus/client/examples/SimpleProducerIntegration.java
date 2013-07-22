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
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.ProducerConfig;
import com.groupon.messagebus.api.exceptions.BrokerConnectionCloseFailedException;
import com.groupon.messagebus.api.exceptions.BrokerConnectionFailedException;
import com.groupon.messagebus.api.exceptions.InvalidConfigException;
import com.groupon.messagebus.api.exceptions.InvalidStatusException;
import com.groupon.messagebus.api.exceptions.SendFailedException;
import com.groupon.messagebus.api.exceptions.TooManyConnectionRetryAttemptsException;
import com.groupon.messagebus.client.ProducerImpl;
import com.groupon.messagebus.api.Message;

public class SimpleProducerIntegration {


    public static void main(String[] args) throws TooManyConnectionRetryAttemptsException, InvalidConfigException, BrokerConnectionCloseFailedException, BrokerConnectionFailedException, SendFailedException, InvalidStatusException{       

        ProducerConfig config = new ProducerConfig();
        HostParams host = new HostParams("localhost", 61613);


        config.setBroker(host);
        config.setConnectionLifetime(10000);
        config.setDestinationType(DestinationType.TOPIC);
        //config.setDestinationType(DestinationType.QUEUE);
        config.setDestinationName("jms.topic.grouponTestTopic2");
        //config.setDestinationName("jms.queue.grouponTestQueue1");

        Producer producer = new ProducerImpl();


        producer.start(config);
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i<1000; i++)
                sb.append(Math.random());
        for(int i = 0 ; i < 5000 ; i ++ ){

            String messageStr = sb.toString() + i;
            Message message =  Message.createBinaryMessage("id-" + i, messageStr.getBytes());


            producer.sendSafe(message, null);
            System.out.println("Sent:"+ message.getMessageId());
        }

        producer.stop();
    }
}
