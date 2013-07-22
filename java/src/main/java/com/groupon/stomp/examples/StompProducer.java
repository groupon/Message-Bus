package com.groupon.stomp.examples;
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
