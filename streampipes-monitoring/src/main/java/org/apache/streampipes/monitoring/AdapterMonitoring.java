/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.monitoring;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.streampipes.messaging.kafka.SpKafkaProducer;
import org.apache.streampipes.serializers.json.JacksonSerializer;

import java.util.concurrent.ConcurrentHashMap;

public class AdapterMonitoring {

    private ConcurrentHashMap<String, AdapterStatus> adapterStatus;
    private Thread monitoringThread;
    // TODO  should it only work with Kafka?
    // To make it generic, reuse SendToBrokerAdapter sink in connect
    private SpKafkaProducer spKafkaProducer;

    public AdapterMonitoring() {
        // Should I start the thread here?
        this.adapterStatus = new ConcurrentHashMap<>();
        this.spKafkaProducer = new SpKafkaProducer("localhost:9094", "adapterstatus");
    }

    public ConcurrentHashMap<String, AdapterStatus> getAdapterStatus() {
        return adapterStatus;
    }

    public void put(String id, AdapterStatus adapterStatus) {
        this.adapterStatus.put(id, adapterStatus);
    }

    public void remove(String id) {
        this.adapterStatus.remove(id);
    }

    public void init() {
        Runnable runnable = () -> {
            while(true) {
                try {
//                    System.out.println("Currently: " + getAdapterStatus().size() + " adapters are running.");
                    long currentTimestamp = System.currentTimeMillis();
                    getAdapterStatus().forEach((s, adapterStatus) -> {
                        adapterStatus.setTimestamp(currentTimestamp);

                        try {
                            String serializedAdapterStatus = JacksonSerializer.getObjectMapper().writeValueAsString(adapterStatus);
                            this.spKafkaProducer.publish(serializedAdapterStatus);
                            System.out.println(serializedAdapterStatus);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }


                        adapterStatus.reset();
                    });
                    // TODO Scheduled task instead of Thread, see StreamPipesBackendApplication
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        this.monitoringThread = new Thread(runnable);
        this.monitoringThread.start();
    }

}
