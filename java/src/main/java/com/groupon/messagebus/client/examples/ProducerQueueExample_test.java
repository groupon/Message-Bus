package com.groupon.messagebus.client.examples;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.groupon.messagebus.client.*;
import com.groupon.messagebus.api.DestinationType;
import com.groupon.messagebus.api.HostParams;
import com.groupon.messagebus.api.Message;
import com.groupon.messagebus.api.Producer;
import com.groupon.messagebus.api.ProducerConfig;


public class ProducerQueueExample_test {

        public static int maxMessageCount = 250000;
        public static int maxBatchSize = 1; //If no batching set this as 1
        public static int maxMessageSize = 1024; //Size in bytes
        public static int maxThreads = 4;

        private String message(){
                String msg = new String("");

                //Creating the initial msg
                StringBuilder sb = new StringBuilder("");
                for(int i=0;i<maxMessageSize;i++){
                        sb.append('a');
                }

                //Final msg to send
                msg = sb.toString();

                return msg;
        }
        private class ProducerWrapper implements Runnable{
                private CountDownLatch cl;
                Producer producer;

                public ProducerWrapper(CountDownLatch c, ProducerConfig config) throws Exception{
                        cl = c; 
                        producer = new ProducerImpl();
                        producer.start(config);
                }

                public void run(){
                        String msg = message();         

                        try {
                                StringBuilder sb = new StringBuilder("");
                                int i=1;
                                for (; i <= (maxMessageCount); i++) {
                                        Message message = Message.createBinaryMessage("id-" + i, msg.getBytes());
                                        producer.sendSafe(message, null);
                                        
                                }

                                producer.stop();
                        } catch (Exception e) {
                                e.printStackTrace();
                        } 

                        cl.countDown();
                }
        }

        public void startThreads() throws Exception{
                
                //Set the configurations
                BasicConfigurator.configure();
                Logger.getRootLogger().setLevel(Level.ERROR);
                HostParams host = new HostParams("localhost", 6661);
                ProducerConfig config = new ProducerConfig();
                config.setBroker(host);
                config.setConnectionLifetime(300000);           
//              config.setDestinationType(DestinationType.QUEUE); // chose between topic (one to many) or queue (one to one)
//              config.setDestinationName("jms.queue.grouponTestQueue2");
                config.setDestinationType(DestinationType.TOPIC); // chose between topic (one to many) or queue (one to one)
                config.setDestinationName("jms.topic.grouponTestTopic2");

                //Start the thread pool
                Executor exec = Executors.newFixedThreadPool(maxThreads);
                CountDownLatch cl = new CountDownLatch(maxThreads);

                
                long start = System.currentTimeMillis();
                
                for(int i=0;i<maxThreads;i++){
                        exec.execute(new ProducerWrapper(cl, config));
                }
                cl.await();

                long end = System.currentTimeMillis();
                double time = ((end - start)*1.0)/1000.0;
                double data = ((maxMessageCount *maxMessageSize*maxThreads)*1.0)/(1024.0*1024.0);
                double speed = data/time;
                System.out.println("Threads = "+maxThreads);
                System.out.println("Messages = "+maxMessageCount*maxThreads);
                System.out.println("Batch size = "+maxBatchSize);
                System.out.println("Data = "+data+"MB");
                System.out.println("Time = "+ time+"sec");
                System.out.println("Speed = "+speed+" MB/sec");
                
                System.exit(1);

        }
        public static void main(String[] args) throws Exception{
                new ProducerQueueExample_test().startThreads();
        }

}
