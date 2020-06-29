package com.bigdata.study.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.bigdata.study.hive.kafka.HookNotificationEntity;
import com.bigdata.study.hive.kafka.NotificationInterface;
import com.bigdata.study.hive.kafka.NotificationProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHook {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHook.class);
    public static final String NOTIFICATION_ASYNCHRONOUS                    = "notification.hook.asynchronous";
    public static final String NOTIFICATION_ASYNCHRONOUS_MIN_THREADS        = "notification.hook.asynchronous.minThreads";
    public static final String NOTIFICATION_ASYNCHRONOUS_MAX_THREADS        = "notification.hook.asynchronous.maxThreads";
    public static final String NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS = "notification.hook.asynchronous.keepAliveTimeMs";
    public static final String NOTIFICATION_ASYNCHRONOUS_QUEUE_SIZE         = "notification.hook.asynchronous.queueSize";
    public static final String NOTIFICATION_MAX_RETRIES                     = "notification.hook.retry.maxRetries";
    public static final String NOTIFICATION_RETRY_INTERVAL                  = "notification.hook.retry.interval";
    private static final int               SHUTDOWN_HOOK_WAIT_TIME_MS = 3000;
    protected static NotificationInterface notificationInterface;
    protected static Configuration         hookProperties;
    private static ExecutorService executor = null;
    private static final int notificationMaxRetries;
    private static final int notificationRetryInterval;


    static{
        try {
            hookProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        //后面配置读取配置文件
        //最大重试次数
        notificationMaxRetries = hookProperties.getInt(NOTIFICATION_MAX_RETRIES,3);
        //最大重试间隔
        notificationRetryInterval = hookProperties.getInt(NOTIFICATION_RETRY_INTERVAL,1000);

        notificationInterface = NotificationProvider.get();

        //是否异步
        boolean isAsync = hookProperties.getBoolean(NOTIFICATION_ASYNCHRONOUS, Boolean.TRUE);
        if (isAsync) {
            int  minThreads      = hookProperties.getInt(NOTIFICATION_ASYNCHRONOUS_MIN_THREADS, 1);
            int  maxThreads      = hookProperties.getInt(NOTIFICATION_ASYNCHRONOUS_MAX_THREADS, 1);
            long keepAliveTimeMs = hookProperties.getLong(NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS, 10000);
            int  queueSize       = hookProperties.getInt(NOTIFICATION_ASYNCHRONOUS_QUEUE_SIZE, 10000);

            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTimeMs, TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<>(queueSize),
                    new ThreadFactoryBuilder().setNameFormat("Notifier %d").setDaemon(true).build());

            ShutdownHookManager.get().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        LOG.info("==> Shutdown of Hook");

                        executor.shutdown();
                        executor.awaitTermination(SHUTDOWN_HOOK_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                        executor = null;
                    } catch (InterruptedException excp) {
                        LOG.info("Interrupt received in shutdown.", excp);
                    } finally {
                        LOG.info("<== Shutdown of  Hook");
                    }
                }
            }, 30);
        }
        LOG.info("Created Hook");
    }



    protected void notifyEntities(List<HookNotificationEntity> messages, UserGroupInformation ugi){
        notifyEntities(messages, ugi, notificationMaxRetries);
    }

    public static void notifyEntities(List<HookNotificationEntity> messages, UserGroupInformation ugi, int maxRetries) {
        if (executor == null) { // 发送同步数据
            notifyEntitiesInternal(messages,maxRetries,ugi,notificationInterface);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    notifyEntitiesInternal(messages,maxRetries,ugi,notificationInterface);
                }
            });
        }
    }


    static void notifyEntitiesInternal(List<HookNotificationEntity> messages, int maxRetries, UserGroupInformation ugi,
                                       NotificationInterface notificationInterface) {

        if (null == messages || messages.isEmpty()){
            return;
        }

        final int    maxAttempts         = maxRetries < 1 ? 1 : maxRetries;

        for (int numAttempt = 1; numAttempt <= maxAttempts; numAttempt++) {
            if (numAttempt > 1) { // retry attempt
                try {
                    LOG.debug("Sleeping for {} ms before retry", notificationRetryInterval);

                    Thread.sleep(notificationRetryInterval);
                } catch (InterruptedException ie) {
                    LOG.error("Notification hook thread sleep interrupted");

                    break;
                }
            }

            try {
                if (ugi == null) {
                    //todo: 具体向kafka 发送message的方法
                    notificationInterface.send("HIVE_HOOK", messages);
                } else {
                    PrivilegedExceptionAction<Object> privilegedNotify = new PrivilegedExceptionAction<Object>() {
                        @Override
                        public Object run() throws Exception {
                            notificationInterface.send("HIVE_HOOK", messages);
                            return messages;
                        }
                    };

                    ugi.doAs(privilegedNotify);
                }

                break;
            } catch (Exception e) {
                LOG.error("Failed to send notification - attempt #{}; error={}", numAttempt, e.getMessage());
            }
        }


    }



}
