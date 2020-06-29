/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.study.hive.events;

import com.bigdata.study.hive.ApplicationProperties;
import com.bigdata.study.hive.HiveHookContext;
import com.bigdata.study.hive.kafka.HookNotificationEntity;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public abstract class BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHiveEvent.class);


    public static final Map<Integer, String> OWNER_TYPE_TO_ENUM_VALUE = new HashMap<Integer, String>();

    //+++
    protected static Configuration hookProperties;
    public static final Long connection_id;

    static {
        OWNER_TYPE_TO_ENUM_VALUE.put(1, "USER");
        OWNER_TYPE_TO_ENUM_VALUE.put(2, "ROLE");
        OWNER_TYPE_TO_ENUM_VALUE.put(3, "GROUP");

        try {
            hookProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }
        //获取配置文件里写死的，数据连接的id
        connection_id = hookProperties.getLong("table.connection_info.id");
    }

    protected final HiveHookContext context;


    protected BaseHiveEvent(HiveHookContext context) {
        this.context = context;
    }

    public HiveHookContext getContext() {
        return context;
    }

    /**
     * 从钩子里面获取sql的所有输入，输出的类型和名称，方便了解其原理和调试
     *
     * @return
     * @throws Exception
     */
    public HookNotificationEntity setAllInputsAndOutputs(HookNotificationEntity entity) throws Exception{
            if(null==entity){//如果前面的处理没有产生对象，则这里生成一个基本对象，用来把必要信息带到消息当中
                entity =new HookNotificationEntity();
                entity.setOperationName(getHiveContext().getOperationName()); //hive.OperationName
                entity.setConnectionId(connection_id);
                entity.setOperator(getHiveContext().getUserName()); //hive.getUserName
                entity.setOperatTime(getCurrentTime()); //currentTime
                entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr()); //hiveContext.getQueryPlan().getQueryStr()

            }
            HookContext hiveContext = getHiveContext();  //hiveContext
            if (hiveContext.getInputs() != null) {
                List<Map<String,Object>> allInputs =new ArrayList<>();
                for (ReadEntity input : hiveContext.getInputs()) {
                    Map<String,Object> inputMap=new HashMap<>();
                    //System.out.println("input.getType()="+input.getType()+"input.getName()="+input.getName());
                    inputMap.put("type",input.getType());
                    inputMap.put("name",input.getName());
                    inputMap.put("isDirect",input.isDirect());
                    allInputs.add(inputMap);
                }
                if (!allInputs.isEmpty()) {
                    entity.setAllInputs(allInputs);
                }
            }
            if (hiveContext.getOutputs() != null) {
                List<Map<String,Object>> allOutputs =new ArrayList<>();
                for (WriteEntity output : hiveContext.getOutputs()) {
                    Map<String,Object> outputMap=new HashMap<>();
                    //System.out.println("output.getType()="+output.getType()+"output.getName()="+output.getName());
                    outputMap.put("type",output.getType());
                    outputMap.put("name",output.getName());
                    allOutputs.add(outputMap);
                }
                if (!allOutputs.isEmpty()) {
                    entity.setAllOutputs(allOutputs);
                }
            }
                return entity;
    }

    protected Date getCurrentTime(){
        return new Date();
    }

    protected HookContext getHiveContext() {
        return context.getHiveContext();
    }

    /**
     * 调用元数据项目api，保存采集到的hive的元数据和血缘
     *
     * @return
     * @throws Exception
     */
    public HookNotificationEntity getMetaAndLineage() throws Exception{
        HookNotificationEntity entity= saveMetaAndLineage();
        return setAllInputsAndOutputs(entity);
    }

    /**
     * 调用元数据项目api，保存采集到的hive的元数据和血缘
     *
     * @return
     * @throws Exception
     */
    public abstract HookNotificationEntity saveMetaAndLineage() throws Exception;

    protected Hive getHive() {
        return context.getHive();
    }

    protected String getUserName() {
        String ret = getHiveContext().getUserName();

        if (StringUtils.isEmpty(ret)) {
            UserGroupInformation ugi = getHiveContext().getUgi();

            if (ugi != null) {
                ret = ugi.getShortUserName();
            }

            if (StringUtils.isEmpty(ret)) {
                try {
                    ret = UserGroupInformation.getCurrentUser().getShortUserName();
                } catch (IOException e) {
                    LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
                    ret = System.getProperty("user.name");
                }
            }
        }


        return ret;
    }

}
