package com.groupon.messagebus.client.examples;

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
        config.setDestinationName("jms.topic.grouponTestTopic2");

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
