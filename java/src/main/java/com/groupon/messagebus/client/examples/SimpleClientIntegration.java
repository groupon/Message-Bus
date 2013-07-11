package com.groupon.messagebus.client.examples;


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
        config.setDestinationName("jms.topic.grouponTestTopic2");
        //config.setDestinationName("jms.queue.grouponTestQueue1");        
                        
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
