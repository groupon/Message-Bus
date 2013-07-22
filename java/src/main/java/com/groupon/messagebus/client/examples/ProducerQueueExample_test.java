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
