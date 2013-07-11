package com.groupon.messagebus.client.examples;

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
