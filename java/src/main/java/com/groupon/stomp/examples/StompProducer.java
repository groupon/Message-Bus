package com.groupon.stomp.examples;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.client.Utils;
import com.groupon.stomp.StompConnection;

public class StompProducer {
    public static void main(String []args) throws Exception{
        String queueName = "jms.queue.grouponTestQueue1";

        StompConnection connection = new StompConnection();
        connection.open("localhost", 61613);        
        connection.connect("rocketman", "rocketman");

        double log_interval  = 1000;
        double startTime = System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            
            Message message = Message.createStringMessage("id-" + i , "hello world-" + i);
            connection.sendSafe(queueName, new String(Utils.getThriftDataAsBytes(message)), null);
            
            if((i%log_interval)==0){
                double took = log_interval / ((System.currentTimeMillis() - startTime)/1000);
                System.out.println("Counter:" + i +" Messages per second = "+took);
                startTime = System.currentTimeMillis();
            }

        }

        connection.disconnect();
    }
}
