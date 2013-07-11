package com.groupon.stomp.examples;

import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.client.Utils;
import com.groupon.stomp.Stomp;
import com.groupon.stomp.Stomp.Headers.Subscribe;
import com.groupon.stomp.StompConnection;
import com.groupon.stomp.StompFrame;

public class StompConsumer {
    public static void main(String []args) throws Exception{
        String queueName = "jms.queue.grouponTestQueue1";
        
        StompConnection connection = new StompConnection();
        connection.open("localhost", 61613);
        connection.connect("rocketman", "rocketman");        
        System.out.println(System.currentTimeMillis());         
        connection.subscribe(queueName, Subscribe.AckModeValues.CLIENT);
        try{
        for(int i=0; i< 100000; i++){
            StompFrame frame = connection.receive();
            System.out.println("Message  consumed" + frame.getBody());
            Message message = Utils.getMessageFromBytes(frame.getBody().getBytes());
            System.out.println(message.getMessageInternal().toString());
            connection.ack(frame.getHeaders().get(Stomp.Headers.Message.MESSAGE_ID), null, null, null, null);
        }   
        }catch (Exception e){
            
            System.out.println(System.currentTimeMillis());
            System.out.println(e.getMessage());
        }

        connection.disconnect();
    }
}
