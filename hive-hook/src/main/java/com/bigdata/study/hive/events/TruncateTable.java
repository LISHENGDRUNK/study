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
import org.apache.hadoop.hive.ql.hooks.Entity;

public class TruncateTable extends BaseHiveEvent {
    public TruncateTable(HiveHookContext context) {
        super(context);
    }

    @Override
    public HookNotificationEntity saveMetaAndLineage() throws Exception {
        //boolean ret = false;
        //List<AtlasObjectId>    entities =


        //if (CollectionUtils.isNotEmpty(entities)) {
        //    //ret = new ArrayList<>(entities.size());
        //
        //    for (AtlasObjectId entity : entities) {
        //        //ret.add(new EntityDeleteRequestV2(getUserName(), Collections.singletonList(entity)));
        //    }
        //}

        return getEntities();
    }

    public HookNotificationEntity getEntities() throws Exception {
        //List<AtlasObjectId> ret = new ArrayList<>();
        //todo 异常日志
        for (Entity entity1 : getHiveContext().getOutputs()) {
            if (entity1.getType() == Entity.Type.TABLE) {
                //String        tblQName = getQualifiedName(entity.getTable());
                //AtlasObjectId dbId     = new AtlasObjectId(HIVE_TYPE_TABLE, ATTRIBUTE_QUALIFIED_NAME, tblQName);
                //context.removeFromKnownTable(tblQName);
                //ret.add(dbId);

                ////1、(内部表)删除血缘:外部表不会清空数据，所以不用删除血缘
                //Table table=entity.getTable();
                //ConnectionInfo connInfo=  getConnInfo(connection_id);
                //if(TableType.EXTERNAL_TABLE.equals(table.getTableType())){
                //    TableMetaVo tableMeta =new TableMetaVo();
                //    tableMeta.setConnectionId(connInfo.getId());
                //    tableMeta.setDatabaseName(table.getDbName());
                //    //tableMeta.setSchemaName("");//hive没有模式
                //    tableMeta.setTableName(table.getTableName());
                //    tableMetaService.findAndDeleteLineageOfTableMeta(tableMeta);
                //}
                //Map<String, Object> tableMetaMap = getTaleMeta(connInfo, table.getDbName(), table.getTableName(), 0);
                //Double size = 0.0;
                //long record = 0;
                //Date ddlTime = (Date) tableMetaMap.get("ddlTime");
                //TableMeta tableMeta = new TableMeta();
                //tableMeta.setTableSize(size);
                //tableMeta.setRecords(record);
                //tableMeta.setDataUpdateTime(ddlTime);
                //tableMeta.setDdlUpdateTime(ddlTime);
                //tableMeta.setOperator(getUserName());
                //Long dbId = getDbId(connInfo, table.getDbName());
                //tableMeta.setDatabaseId(dbId);
                //Long tableId = getTableId(connInfo, dbId, null, table.getTableName());
                //updateTableMeta(tableId, tableMeta);
                //
                //// 保存日志
                //saveTableLog(getUserName(), tableId, dbId, "Entity Updated", getQueryStr(), connection_id);

                HookNotificationEntity entity = new HookNotificationEntity();
                entity.setOperationName(getHiveContext().getOperationName());
                entity.setConnectionId(connection_id);
                entity.setDatabaseName(entity1.getTable().getDbName());
                //entity.setTableType(1);
                if (TableType.EXTERNAL_TABLE.equals(entity1.getTable().getTableType())) {
                    entity.setExternal(true);
                } else {
                    entity.setExternal(false);
                }
                entity.setTableName(entity1.getTable().getTableName());
                entity.setOperator(getUserName());
                entity.setOperatTime(getCurrentTime());
                if (TableType.EXTERNAL_TABLE.equals(entity1.getTable().getTableType())) {
                    entity.setExternal(true);
                } else {
                    entity.setExternal(false);
                }
                entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
                //entity.setInputs();
                //entity.setOutputs();
                //entity.setNewName();
                return entity;

            }
        }

        return null;
    }
}
