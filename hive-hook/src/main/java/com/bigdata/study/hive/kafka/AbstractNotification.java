/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bigdata.study.hive.kafka;

import com.alibaba.fastjson.JSON;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract notification interface implementation.
 */
public abstract class AbstractNotification implements NotificationInterface {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractNotification.class);


    public static final int MAX_BYTES_PER_CHAR = 4;  // each char can encode upto 4 bytes in UTF-8

    /**
     * IP address of the host in which this process has started
     */
    private static String localHostAddress = "";

    /**
     *
     */
    private static String currentUser = "";

    // ----- Constructors ----------------------------------------------------

    public AbstractNotification(Configuration applicationProperties) {
    }

    @VisibleForTesting
    protected AbstractNotification() {
    }

    // ----- NotificationInterface -------------------------------------------

    @Override
    public <T> void send(String type, List<T> messages) {
        //List<String> strMessages = new ArrayList<>(messages.size());
        String strMessages ="";
        for (int index = 0; index < messages.size(); index++) {
            strMessages= createNotificationMessages(messages);
            sendInternal(type, strMessages);
        }
    }

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the array of messages to send
     *
     */
    protected abstract void sendInternal(String type, String messages);



    public static String createNotificationMessages(Object message) {
            return JSON.toJSONString(message);//通过fastjson转换成字符串，在消费端再转换成对象
    }


}
