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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterTableRename extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(AlterTableRename.class);

    public AlterTableRename(HiveHookContext context) {
        super(context);
    }

    @Override
    public HookNotificationEntity saveMetaAndLineage() throws Exception {
        //boolean ret = false;
        if (CollectionUtils.isEmpty(getHiveContext().getInputs())) {
            LOG.error("AlterTableRename: old-table not found in inputs list");
            return null;
        }
        Table oldTable = getHiveContext().getInputs().iterator().next().getTable();
        Table newTable = null;
        if (CollectionUtils.isNotEmpty(getHiveContext().getOutputs())) {
            for (WriteEntity entity : getHiveContext().getOutputs()) {
                if (entity.getType() == Entity.Type.TABLE) {
                    newTable = entity.getTable();
                    //Hive sends with both old and new table names in the outputs which is weird. So skipping that with the below check
                    if (StringUtils.equalsIgnoreCase(newTable.getDbName(), oldTable.getDbName()) && StringUtils.equalsIgnoreCase(newTable.getTableName(), oldTable.getTableName())) {
                        newTable = null;
                        continue;
                    }
                    newTable = getHive().getTable(newTable.getDbName(), newTable.getTableName());
                    break;
                }
            }
        }
        if (newTable == null) {
            LOG.error("AlterTableRename: renamed table not found in outputs list");
            return null;
        }
//        ConnectionInfo connInfo=  getConnInfo(connection_id);
//        TableMetaVo tableMeta =new TableMetaVo();
//        tableMeta.setConnectionId(connInfo.getId());
//        tableMeta.setDatabaseName(oldTable.getDbName());
//        //tableMeta.setSchemaName("");
//        tableMeta.setTableName(oldTable.getTableName());
//        tableMetaService.findAndRenameTableMeta(tableMeta,newTable.getTableName());//如何获取新旧表名
////        Map<String, Object> tableMetaMap = getTaleMeta(connInfo, table.getDbName(), table.getTableName(), 0);
////        Date ddlTime = (Date) tableMetaMap.get("ddlTime");
//        Date ddlTime = getCurrentTime();
//        TableMeta tableMeta1 = new TableMeta();
//        tableMeta1.setDdlUpdateTime(ddlTime);
//        tableMeta1.setOperator(getUserName());
//        Long dbId = getDbId(connInfo, oldTable.getDbName());
//        tableMeta1.setDatabaseId(dbId);
//        Long tableId = getTableId(connInfo, dbId, null, newTable.getTableName());
//        updateTableMeta(tableId, tableMeta1);
//        // 保存日志
//        saveTableLog(getUserName(), tableId, dbId, "Entity Updated", getQueryStr(), connection_id);

        HookNotificationEntity entity = new HookNotificationEntity();
        entity.setOperationName(getHiveContext().getOperationName());
        entity.setConnectionId(connection_id);
        entity.setDatabaseName(oldTable.getDbName());
        //entity.setTableType(1);
        if (TableType.EXTERNAL_TABLE.equals(oldTable.getTableType())) {
            entity.setExternal(true);
        } else {
            entity.setExternal(false);
        }
        entity.setTableName(oldTable.getTableName());
        entity.setOperator(getUserName());
        entity.setOperatTime(getCurrentTime());
        entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
        //entity.setInputs();
        //entity.setOutputs();
        entity.setNewName(newTable.getTableName());
        return entity;

        //
        //AtlasEntityWithExtInfo oldTableEntity = toTableEntity(oldTable);
        //
        //// first update with oldTable info, so that the table will be created if it is not present in Atlas
        ////ret.add(new EntityUpdateRequestV2(getUserName(), new AtlasEntitiesWithExtInfo(oldTableEntity)));
        //
        //AtlasEntityWithExtInfo renamedTableEntity = toTableEntity(newTable);
        //
        //// update qualifiedName for all columns, partitionKeys, storageDesc
        //String renamedTableQualifiedName = (String) renamedTableEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME);
        //
        ////renameColumns((List<AtlasObjectId>) oldTableEntity.getEntity().getAttribute(ATTRIBUTE_COLUMNS), oldTableEntity, renamedTableQualifiedName, ret);
        ////renameColumns((List<AtlasObjectId>) oldTableEntity.getEntity().getAttribute(ATTRIBUTE_PARTITION_KEYS), oldTableEntity, renamedTableQualifiedName, ret);
        ////renameStorageDesc(oldTableEntity, renamedTableEntity, ret);
        //
        //// remove columns, partitionKeys and storageDesc - as they have already been updated above
        //removeAttribute(renamedTableEntity, ATTRIBUTE_COLUMNS);
        //removeAttribute(renamedTableEntity, ATTRIBUTE_PARTITION_KEYS);
        //removeAttribute(renamedTableEntity, ATTRIBUTE_STORAGEDESC);
        //
        //// set previous name as the alias
        //renamedTableEntity.getEntity().setAttribute(ATTRIBUTE_ALIASES, Collections.singletonList(oldTable.getTableName()));
        //
        //AtlasObjectId oldTableId = new AtlasObjectId(oldTableEntity.getEntity().getTypeName(), ATTRIBUTE_QUALIFIED_NAME, oldTableEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        //
        //// update qualifiedName and other attributes (like params - which include lastModifiedTime, lastModifiedBy) of the table
        ////ret.add(new EntityPartialUpdateRequestV2(getUserName(), oldTableId, renamedTableEntity));
        //
        ////context.removeFromKnownTable((String) oldTableEntity.getEntity().getAttribute(ATTRIBUTE_QUALIFIED_NAME));

        //return ret;
    }

    //private void renameColumns(List<AtlasObjectId> columns, AtlasEntityExtInfo oldEntityExtInfo, String newTableQualifiedName, boolean notifications) {
    //    if (CollectionUtils.isNotEmpty(columns)) {
    //        for (AtlasObjectId columnId : columns) {
    //            AtlasEntity   oldColumn   = oldEntityExtInfo.getEntity(columnId.getGuid());
    //            AtlasObjectId oldColumnId = new AtlasObjectId(oldColumn.getTypeName(), ATTRIBUTE_QUALIFIED_NAME, oldColumn.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
    //            AtlasEntity   newColumn   = new AtlasEntity(oldColumn.getTypeName(), ATTRIBUTE_QUALIFIED_NAME, getColumnQualifiedName(newTableQualifiedName, (String) oldColumn.getAttribute(ATTRIBUTE_NAME)));
    //
    //            notifications.add(new EntityPartialUpdateRequestV2(getUserName(), oldColumnId, new AtlasEntityWithExtInfo(newColumn)));
    //        }
    //    }
    //}
    //
    //private void renameStorageDesc(AtlasEntityWithExtInfo oldEntityExtInfo, AtlasEntityWithExtInfo newEntityExtInfo, boolean notifications) {
    //    AtlasEntity oldSd = getStorageDescEntity(oldEntityExtInfo);
    //    AtlasEntity newSd = getStorageDescEntity(newEntityExtInfo);
    //
    //    if (oldSd != null && newSd != null) {
    //        AtlasObjectId oldSdId = new AtlasObjectId(oldSd.getTypeName(), ATTRIBUTE_QUALIFIED_NAME, oldSd.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
    //
    //        newSd.removeAttribute(ATTRIBUTE_TABLE);
    //
    //        notifications.add(new EntityPartialUpdateRequestV2(getUserName(), oldSdId, new AtlasEntityWithExtInfo(newSd)));
    //    }
    //}

    //private void removeAttribute(AtlasEntityWithExtInfo entity, String attributeName) {
    //    Object attributeValue = entity.getEntity().getAttribute(attributeName);
    //
    //    entity.getEntity().getAttributes().remove(attributeName);
    //
    //    if (attributeValue instanceof AtlasObjectId) {
    //        AtlasObjectId objectId = (AtlasObjectId) attributeValue;
    //
    //        entity.removeReferredEntity(objectId.getGuid());
    //    } else if (attributeValue instanceof Collection) {
    //        for (Object item : (Collection) attributeValue)
    //            if (item instanceof AtlasObjectId) {
    //                AtlasObjectId objectId = (AtlasObjectId) item;
    //
    //                entity.removeReferredEntity(objectId.getGuid());
    //            }
    //    }
    //}

    //private AtlasEntity getStorageDescEntity(AtlasEntityWithExtInfo tableEntity) {
    //    AtlasEntity ret = null;
    //
    //    if (tableEntity != null && tableEntity.getEntity() != null) {
    //        Object attrSdId = tableEntity.getEntity().getAttribute(ATTRIBUTE_STORAGEDESC);
    //
    //        if (attrSdId instanceof AtlasObjectId) {
    //            ret = tableEntity.getReferredEntity(((AtlasObjectId) attrSdId).getGuid());
    //        }
    //    }
    //
    //    return ret;
    //}
}
