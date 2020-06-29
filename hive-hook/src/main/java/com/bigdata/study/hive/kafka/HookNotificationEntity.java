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
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Base type of hook message.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class HookNotificationEntity implements Serializable {
    private static final long serialVersionUID = 1L;

    //public static final String UNKNOW_USER = "UNKNOWN";

    ///**
    // * Type of the hook message.
    // */
    //public enum HookNotificationType {
    //    TYPE_CREATE, TYPE_UPDATE, ENTITY_CREATE, ENTITY_PARTIAL_UPDATE, ENTITY_FULL_UPDATE, ENTITY_DELETE,
    //    ENTITY_CREATE_V2, ENTITY_PARTIAL_UPDATE_V2, ENTITY_FULL_UPDATE_V2, ENTITY_DELETE_V2
    //}

    private String operationName;//hive的操作类型==钩子拿到的sql的操作类型
    //protected String    user; //操作用户，执行sql的用户
    /**
     * 父级:数据连接表id
     */
    //@Column(name = "connection_id")
    private Long connectionId;

    /**
     * 数据库名
     */
    private String databaseName;

    ///**
    // * 模式名，三层数据库这个字段才有值
    // */
    //@Column(name = "schema_name")
    //private String schemaName;

    /**
     * 表名
     */
    //@Column(name = "table_name")
    private String tableName;

    ///**
    // * 表的类型:1表、2视图
    // */
    ////@Column(name = "table_type")
    //private Integer tableType;

    /**
     * 操作用户，执行sql的用户
     */
    private String operator;

    /**
     * 操作时间：（以解析到的元数据变化时间为准。不能以本数据自动产生的实际为准）
     */
    private Date operatTime;

    /**
     * 重命名，重命名时，新的表名或者库名
     */
    private String newName;

    /**
     *  是否是外部表：：清空内部表，hive才会清除数据，即影响血缘
     */
    private Boolean isExternal;

    ///**
    // * 是否是：临时表
    // */
    //private Boolean temporary;
    //===============================
    /**
     * 执行的sql语句
     */
    private String sqlStr;
    /**
     * 输入列表::根据操作类型，可能是表名、也可能是库名
     */
    private List<HookNotificationEntity> inputs;
    /**
     * 输出列表::根据操作类型，可能是表名、也可能是库名
     */
    private List<HookNotificationEntity> outputs;

    /**
     * sql解析出来的所有的inputs的基础内容，方便调试
     */
    private List<Map<String,Object>> allInputs;
    /**
     * sql解析出来的所有的outputs的基础内容，方便调试
     */
    private List<Map<String,Object>> allOutputs;
    //===============================================

    public HookNotificationEntity() {
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public Long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Long connectionId) {
        this.connectionId = connectionId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    //public Integer getTableType() {
    //    return tableType;
    //}
    //
    //public void setTableType(Integer tableType) {
    //    this.tableType = tableType;
    //}

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Date getOperatTime() {
        return operatTime;
    }

    public void setOperatTime(Date operatTime) {
        this.operatTime = operatTime;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public String getSqlStr() {
        return sqlStr;
    }

    public void setSqlStr(String sqlStr) {
        this.sqlStr = sqlStr;
    }

    public List<HookNotificationEntity> getInputs() {
        return inputs;
    }

    public void setInputs(List<HookNotificationEntity> inputs) {
        this.inputs = inputs;
    }

    public List<HookNotificationEntity> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<HookNotificationEntity> outputs) {
        this.outputs = outputs;
    }

    public void setExternal(Boolean external) {
        isExternal = external;
    }

    public Boolean getExternal() {
        return isExternal;
    }

    public List<Map<String, Object>> getAllInputs() {
        return allInputs;
    }

    public void setAllInputs(List<Map<String, Object>> allInputs) {
        this.allInputs = allInputs;
    }

    public List<Map<String, Object>> getAllOutputs() {
        return allOutputs;
    }

    public void setAllOutputs(List<Map<String, Object>> allOutputs) {
        this.allOutputs = allOutputs;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }
        sb.append("HookNotificationEntity{");
        sb.append("operationName=").append(operationName);
        sb.append(", connectionId=").append(connectionId);
        sb.append(", databaseName=").append(databaseName);
        sb.append(", tableName=").append(tableName);
        //sb.append(", tableType=").append(tableType);
        sb.append(", operator=").append(operator);
        sb.append(", operatTime=").append(operatTime);
        sb.append(", newName=").append(newName);
        sb.append(", isExternal=").append(isExternal);
        sb.append(", sqlStr=").append(sqlStr);
        //if(null!=inputs&&inputs.size()>0){
        //    sb.append(", inputs=").append(inputs);
        //}else {
        sb.append(", inputs=").append(inputs);
        //}
        //if(null!=inputs&&inputs.size()>0){
        //    sb.append(", outputs=").append(outputs);
        //}else {
        sb.append(", outputs=").append(outputs);
        //}
        sb.append(", allInputs=").append(allInputs);
        sb.append(", allOutputs=").append(allOutputs);
        sb.append("}");
        return sb;
    }

    //@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    //@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    //@JsonIgnoreProperties(ignoreUnknown=true)
    //@XmlRootElement
    //@XmlAccessorType(XmlAccessType.PROPERTY)
    //public static class EntityCreateRequestV2 extends HookNotification implements Serializable {
    //    private AtlasEntitiesWithExtInfo entities;
    //
    //    private EntityCreateRequestV2() {
    //    }
    //
    //    public EntityCreateRequestV2(String user, AtlasEntitiesWithExtInfo entities) {
    //        super(HookNotificationType.ENTITY_CREATE_V2, user);
    //
    //        this.entities = entities;
    //    }
    //
    //    public AtlasEntitiesWithExtInfo getEntities() {
    //        return entities;
    //    }
    //
    //    @Override
    //    public String toString() {
    //        return entities == null ? "null" : entities.toString();
    //    }
    //}
    //
    //@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    //@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    //@JsonIgnoreProperties(ignoreUnknown=true)
    //@XmlRootElement
    //@XmlAccessorType(XmlAccessType.PROPERTY)
    //public static class EntityUpdateRequestV2 extends HookNotification implements Serializable {
    //    private AtlasEntitiesWithExtInfo entities;
    //
    //    private EntityUpdateRequestV2() {
    //    }
    //
    //    public EntityUpdateRequestV2(String user, AtlasEntitiesWithExtInfo entities) {
    //        super(HookNotificationType.ENTITY_FULL_UPDATE_V2, user);
    //
    //        this.entities = entities;
    //    }
    //
    //    public AtlasEntitiesWithExtInfo getEntities() {
    //        return entities;
    //    }
    //
    //    @Override
    //    public String toString() {
    //        return entities == null ? "null" : entities.toString();
    //    }
    //}
    //
    //@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    //@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    //@JsonIgnoreProperties(ignoreUnknown=true)
    //@XmlRootElement
    //@XmlAccessorType(XmlAccessType.PROPERTY)
    //public static class EntityPartialUpdateRequestV2 extends HookNotification implements Serializable {
    //    private AtlasObjectId          entityId;
    //    private AtlasEntityWithExtInfo entity;
    //
    //    private EntityPartialUpdateRequestV2() {
    //    }
    //
    //    public EntityPartialUpdateRequestV2(String user, AtlasObjectId entityId, AtlasEntityWithExtInfo entity) {
    //        super(HookNotificationType.ENTITY_PARTIAL_UPDATE_V2, user);
    //
    //        this.entityId = entityId;
    //        this.entity   = entity;
    //    }
    //
    //    public AtlasObjectId getEntityId() {
    //        return entityId;
    //    }
    //
    //    public AtlasEntityWithExtInfo getEntity() {
    //        return entity;
    //    }
    //
    //    @Override
    //    public String toString() {
    //        return "entityId=" + entityId + "; entity=" + entity;
    //    }
    //}
    //
    //@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    //@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
    //@JsonIgnoreProperties(ignoreUnknown=true)
    //@XmlRootElement
    //@XmlAccessorType(XmlAccessType.PROPERTY)
    //public static class EntityDeleteRequestV2 extends HookNotification implements Serializable {
    //    private List<AtlasObjectId> entities;
    //
    //    private EntityDeleteRequestV2() {
    //    }
    //
    //    public EntityDeleteRequestV2(String user, List<AtlasObjectId> entities) {
    //        super(HookNotificationType.ENTITY_DELETE_V2, user);
    //
    //        this.entities = entities;
    //    }
    //
    //    public List<AtlasObjectId> getEntities() {
    //        return entities;
    //    }
    //
    //    @Override
    //    public String toString() {
    //        return entities == null ? "null" : entities.toString();
    //    }
    //}
}
