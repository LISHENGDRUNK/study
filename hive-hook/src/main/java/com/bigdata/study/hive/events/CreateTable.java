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

import com.bigdata.study.hive.HiveHookContext;
import com.bigdata.study.hive.kafka.HookNotificationEntity;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.metadata.Table;

public class CreateTable extends BaseHiveEvent {
    private final boolean skipTempTables;

    public CreateTable(HiveHookContext context, boolean skipTempTables) {
        super(context);

        this.skipTempTables = skipTempTables;
    }

    @Override
    public HookNotificationEntity saveMetaAndLineage() throws Exception {
        //boolean ret = false;
        //AtlasEntitiesWithExtInfo entities =


        //if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
        //    //ret = Collections.singletonList(new EntityCreateRequestV2(getUserName(), entities));
        //}

        return getEntities();
    }

    public HookNotificationEntity getEntities() throws Exception {
        //AtlasEntitiesWithExtInfo ret   = new AtlasEntitiesWithExtInfo();
        Database                 db    = null;
        Table                    table = null;

        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                table = entity.getTable();
                if (table != null) {
                    //db    = getHive().getDatabase(table.getDbName());
                    table = getHive().getTable(table.getDbName(), table.getTableName());
                    if (table != null) {
                        // If its an external table, even though the temp table skip flag is on, we create the table since we need the HDFS path to temp table lineage.
                        if (skipTempTables && table.isTemporary()) {// && !TableType.EXTERNAL_TABLE.equals(table.getTableType())
                            table = null;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        if (table != null) {
            //TableOperationLogUnusual tableOperationLogUnusual = getTLogUnusual(getUserName(), getQueryStr(), table.getDbName(), getCurrentTime());
            //try{
            //    TableOperationLog tableOperationLog = new TableOperationLog();
            //    //AtlasEntity tblEntity = toTableEntity(table, ret);
            //    ConnectionInfo connInfo=  getConnInfo(connection_id);
            //    commonService.saveCreateOneTableMeta(connInfo.getId(),table.getDbName(),null,table.getTableName(),false);
            //    Map<String, Object> tableMetaMap = getTaleMeta(connInfo, table.getDbName(), table.getTableName(), 1);
            //    Double size = (Double) tableMetaMap.get("size");
            //    Long record = (Long) tableMetaMap.get("record");
            //    Date createTime = (Date) tableMetaMap.get("createTime");
            //    String owner = tableMetaMap.get("owner").toString();
            //    Date ddlTime = (Date) tableMetaMap.get("ddlTime");
            //    TableMeta tableMeta = new TableMeta();
            //    if (record != 0L){
            //        tableMeta.setDataUpdateTime(createTime);
            //    }
            //    tableMeta.setTableSize(size);
            //    tableMeta.setRecords(record);
            //    tableMeta.setOwner(owner);
            //    tableMeta.setDdlCreateTime(createTime);
            //    tableMeta.setDdlUpdateTime(ddlTime);
            //    tableMeta.setOperator(getUserName());
            //    Long dbId = getDbId(connInfo, table.getDbName());
            //    tableMeta.setDatabaseId(dbId);
            //    Long tableId = getTableId(connInfo, dbId, null, table.getTableName());
            //    //更新元数据
            //    updateTableMeta(tableId, tableMeta);
            //    //保存正常日志
            //    saveTableLog(getUserName(), tableId, dbId, "Entity Created", getQueryStr(), connection_id);
            //}catch (Exception e){
            //    // 保存异常日志
            //    tableOperationLogUnusualService.addTableOperationLogUnusual(tableOperationLogUnusual);
            //    e.printStackTrace();
            //}

            //if (isHBaseStore(table)) {
            //    // This create lineage to HBase table in case of Hive on HBase
            //    AtlasEntity hbaseTableEntity = toReferencedHBaseTable(table, ret);
            //    if (hbaseTableEntity != null) {
            //        final AtlasEntity processEntity;
            //        //if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
            //        //不管是不是外部表关联hbase，血缘都改成从hbase到hive,方便理解
            //            processEntity = getHiveProcessEntity(Collections.singletonList(hbaseTableEntity), Collections.singletonList(tblEntity));
            //        //} else {
            //        //    processEntity = getHiveProcessEntity(Collections.singletonList(tblEntity), Collections.singletonList(hbaseTableEntity));
            //        //}
            //        ret.addEntity(processEntity);
            //    }
            //} else {
            //    if (TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
            //        AtlasEntity hdfsPathEntity = getPathEntity(table.getDataLocation(), ret);
            //        AtlasEntity processEntity  = getHiveProcessEntity(Collections.singletonList(hdfsPathEntity), Collections.singletonList(tblEntity));
            //        ret.addEntity(processEntity);
            //        ret.addReferredEntity(hdfsPathEntity);
            //    }
            //}
            HookNotificationEntity entity =new HookNotificationEntity();
            entity.setOperationName(getHiveContext().getOperationName());
            entity.setConnectionId(connection_id);
            entity.setDatabaseName(table.getDbName());
            //entity.setTableType(1);
            if(TableType.EXTERNAL_TABLE.equals(table.getTableType())) {
                entity.setExternal(true);
            }else {
                entity.setExternal(false);
            }
            entity.setTableName(table.getTableName());
            entity.setOperator(getUserName());
            entity.setOperatTime(getCurrentTime());
            entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
            //entity.setInputs();
            //entity.setOutputs();
            //entity.setNewName();
            return  entity;
        }

        //addProcessedEntities(ret);

        return null;
    }
}
