package com.bigdata.study.hive.kafka;

import java.util.List;

public interface NotificationInterface {


    <T> void send(String type, List<T> messages);

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();

}
