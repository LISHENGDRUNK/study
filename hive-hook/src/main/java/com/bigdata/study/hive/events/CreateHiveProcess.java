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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class CreateHiveProcess extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreateHiveProcess.class);

    public CreateHiveProcess(HiveHookContext context) {
        super(context);
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
        if (!skipProcess()) {
            List<Table> inputs         = new ArrayList<Table>();
            List<Table> outputs        = new ArrayList<Table>();
            HookContext       hiveContext    = getHiveContext();
            Set<String>       processedNames = new HashSet<String>();

            if (hiveContext.getInputs() != null) {
                for (ReadEntity input : hiveContext.getInputs()) {
                    if (!(input.getType()== Entity.Type.TABLE||input.getType()== Entity.Type.PARTITION)) {//!input.isDirect()||
                        continue;
                    }
                    if(!input.isDirect()){//非直接血缘，则跳过
                        continue;
                    }
                    //System.out.println("input.getType()="+input.getType()+"input.getName()="+input.getName());
                    String qualifiedName = input.getName();
                    if (qualifiedName == null || !processedNames.add(qualifiedName)) {
                        continue;
                    }
                    Table table = getHive().getTable(input.getTable().getDbName(), input.getTable().getTableName());
                    if (null!=table  ) {
                        inputs.add(table);
                    }
                }
            }

            if (hiveContext.getOutputs() != null) {
                for (WriteEntity output : hiveContext.getOutputs()) {
                    if (!(output.getType()== Entity.Type.TABLE||output.getType()== Entity.Type.PARTITION)) {
                        continue;
                    }
                    //System.out.println("output.getType()="+output.getType()+"output.getName()="+output.getName());
                    String qualifiedName = output.getName();
                    if (qualifiedName == null || !processedNames.add(qualifiedName)) {
                        continue;
                    }
                    Table table = getHive().getTable(output.getTable().getDbName(), output.getTable().getTableName());
                    if (null != table) {
                        outputs.add(table);
                    }
                }
            }
            if (!inputs.isEmpty() || !outputs.isEmpty()) {
                //String      queryStr    = getHiveContext().getQueryPlan().getQueryStr();
                //新增血缘节点的时候，需要注意，这个节点可能已经存在，可能是其他操作已经建立了该节点
                //建立血缘过程的时候，也需要判断是否已经存在该过程了，可能是分区操作或者其他操作，已经建立了该过程；已经存在的就不用再建立了
                //List<Long> fromIds=new ArrayList<Long>();
                //List<Long> toId=new ArrayList<Long>();//目前一个过程只有一个输出
                //ConnectionInfo connInfo=  getConnInfo(connection_id);
                //for(Table from :inputs){
                //    lineageService.insertLineNoteForOneTable(connInfo, from.getDbName(),from.getTableName(), fromIds);
                //}
                //for(Table to :outputs){
                //    lineageService.insertLineNoteForOneTable(connInfo, to.getDbName(),to.getTableName(), toId);
                //}
                //// 输出过程的sql语句；即本次执行的sql
                //lineageService.insertLineprocessForTable(fromIds, toId, queryStr);

                List<HookNotificationEntity> inputsEntity=new ArrayList();
                for(Table from :inputs){
                    HookNotificationEntity entityInput =new HookNotificationEntity();
                    entityInput.setOperationName(getHiveContext().getOperationName());
                    entityInput.setConnectionId(connection_id);
                    entityInput.setDatabaseName(from.getDbName());
                    //entityInput.setTableType(1);
                    if(TableType.EXTERNAL_TABLE.equals(from.getTableType())) {
                        entityInput.setExternal(true);
                    }else {
                        entityInput.setExternal(false);
                    }
                    entityInput.setTableName(from.getTableName());
                    entityInput.setOperator(getUserName());
                    entityInput.setOperatTime(getCurrentTime());
                    inputsEntity.add(entityInput);
                }
                List<HookNotificationEntity> outputsEntity=new ArrayList();
                for(Table to :outputs){
                    HookNotificationEntity entityInput =new HookNotificationEntity();
                    entityInput.setOperationName(getHiveContext().getOperationName());
                    entityInput.setConnectionId(connection_id);
                    entityInput.setDatabaseName(to.getDbName());
                    //entityInput.setTableType(1);
                    entityInput.setTableName(to.getTableName());
                    if(TableType.EXTERNAL_TABLE.equals(to.getTableType())) {
                        entityInput.setExternal(true);
                    }else {
                        entityInput.setExternal(false);
                    }
                    entityInput.setOperator(getUserName());
                    entityInput.setOperatTime(getCurrentTime());
                    outputsEntity.add(entityInput);
                }
                //目前用第一个输出表，作为主表返回
                //HookNotificationEntity entityOutput= outputsEntity.get(0);
                HookNotificationEntity entity =new HookNotificationEntity();
                entity.setOperationName(getHiveContext().getOperationName());
                entity.setConnectionId(connection_id);
                //entity.setDatabaseName(entityOutput.getDatabaseName());
                ////entity.setTableType(1);
                //entity.setTableName(entityOutput.getTableName());
                entity.setOperator(getUserName());
                entity.setOperatTime(getCurrentTime());
                entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
                entity.setInputs(inputsEntity);
                entity.setOutputs(outputsEntity);
                //entity.setExternal(entityOutput.isExternal());
                //entity.setNewName();
                return  entity;
            }
        }


        //if (!skipProcess()) {
        //    List<AtlasEntity> inputs         = new ArrayList<>();
        //    List<AtlasEntity> outputs        = new ArrayList<>();
        //    HookContext       hiveContext    = getHiveContext();
        //    Set<String>       processedNames = new HashSet<>();

            //ret = new AtlasEntitiesWithExtInfo();

            //if (hiveContext.getInputs() != null) {
            //    for (ReadEntity input : hiveContext.getInputs()) {
            //        String qualifiedName = getQualifiedName(input);
            //        if (qualifiedName == null || !processedNames.add(qualifiedName)) {
            //            continue;
            //        }
            //        AtlasEntity entity = getInputOutputEntity(input, ret);
            //        //Table table = getHive().getTable(entity.getTable().getDbName(), entity.getTable().getTableName());
            //        if (!input.isDirect()) {
            //            continue;
            //        }
            //        if (entity != null) {
            //            inputs.add(entity);
            //        }
            //    }
            //}
            //
            //if (hiveContext.getOutputs() != null) {
            //    for (WriteEntity output : hiveContext.getOutputs()) {
            //        String qualifiedName = getQualifiedName(output);
            //        if (qualifiedName == null || !processedNames.add(qualifiedName)) {
            //            continue;
            //        }
            //        AtlasEntity entity = getInputOutputEntity(output, ret);
            //        if (entity != null) {
            //            outputs.add(entity);
            //        }
            //    }
            //}
            //if (!inputs.isEmpty() || !outputs.isEmpty()) {
            //    AtlasEntity process = getHiveProcessEntity(inputs, outputs);
            //    ret.addEntity(process);
            //    //processColumnLineage(process, ret);
            //    addProcessedEntities(ret);
            //}
            //else {
            //    ret = null;
            //}
        //}

        return null;
    }

    //private void processColumnLineage(AtlasEntity hiveProcess, AtlasEntitiesWithExtInfo entities) {
    //    LineageInfo lineageInfo = getHiveContext().getLinfo();
    //
    //    if (lineageInfo == null || CollectionUtils.isEmpty(lineageInfo.entrySet())) {
    //        return;
    //    }
    //
    //    for (Map.Entry<DependencyKey, Dependency> entry : lineageInfo.entrySet()) {
    //        String      outputColName = getQualifiedName(entry.getKey());
    //        AtlasEntity outputColumn  = context.getEntity(outputColName);
    //
    //        if (outputColumn == null) {
    //            LOG.warn("column-lineage: non-existing output-column {}", outputColName);
    //
    //            continue;
    //        }
    //
    //        List<AtlasEntity> inputColumns = new ArrayList<>();
    //
    //        for (BaseColumnInfo baseColumn : getBaseCols(entry.getValue())) {
    //            String      inputColName = getQualifiedName(baseColumn);
    //            AtlasEntity inputColumn  = context.getEntity(inputColName);
    //
    //            if (inputColumn == null) {
    //                LOG.warn("column-lineage: non-existing input-column {} for output-column={}", inputColName, outputColName);
    //
    //                continue;
    //            }
    //
    //            inputColumns.add(inputColumn);
    //        }
    //
    //        if (inputColumns.isEmpty()) {
    //            continue;
    //        }
    //
    //        AtlasEntity columnLineageProcess = new AtlasEntity(HIVE_TYPE_COLUMN_LINEAGE);
    //
    //        columnLineageProcess.setAttribute(ATTRIBUTE_NAME, hiveProcess.getAttribute(ATTRIBUTE_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
    //        columnLineageProcess.setAttribute(ATTRIBUTE_QUALIFIED_NAME, hiveProcess.getAttribute(ATTRIBUTE_QUALIFIED_NAME) + ":" + outputColumn.getAttribute(ATTRIBUTE_NAME));
    //        columnLineageProcess.setAttribute(ATTRIBUTE_INPUTS, getObjectIds(inputColumns));
    //        columnLineageProcess.setAttribute(ATTRIBUTE_OUTPUTS, Collections.singletonList(getObjectId(outputColumn)));
    //        columnLineageProcess.setAttribute(ATTRIBUTE_QUERY, getObjectId(hiveProcess));
    //        columnLineageProcess.setAttribute(ATTRIBUTE_DEPENDENCY_TYPE, entry.getValue().getType());
    //        columnLineageProcess.setAttribute(ATTRIBUTE_EXPRESSION, entry.getValue().getExpr());
    //
    //        entities.addEntity(columnLineageProcess);
    //    }
    //}

    //private Collection<BaseColumnInfo> getBaseCols(Dependency lInfoDep) {
    //    Collection<BaseColumnInfo> ret = Collections.emptyList();
    //
    //    if (lInfoDep != null) {
    //        try {
    //            Method getBaseColsMethod = lInfoDep.getClass().getMethod("getBaseCols");
    //
    //            Object retGetBaseCols = getBaseColsMethod.invoke(lInfoDep);
    //
    //            if (retGetBaseCols != null) {
    //                if (retGetBaseCols instanceof Collection) {
    //                    ret = (Collection) retGetBaseCols;
    //                } else {
    //                    LOG.warn("{}: unexpected return type from LineageInfo.Dependency.getBaseCols(), expected type {}",
    //                            retGetBaseCols.getClass().getName(), "Collection");
    //                }
    //            }
    //        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
    //            LOG.warn("getBaseCols()", ex);
    //        }
    //    }
    //
    //    return ret;
    //}


    private boolean skipProcess() {
        Set<ReadEntity>  inputs  = getHiveContext().getInputs();
        Set<WriteEntity> outputs = getHiveContext().getOutputs();

        boolean ret = CollectionUtils.isEmpty(inputs) && CollectionUtils.isEmpty(outputs);

        if (!ret) {
            if (getContext().getHiveOperation() == HiveOperation.QUERY) {
                // Select query has only one output
                if (outputs.size() == 1) {
                    WriteEntity output = outputs.iterator().next();

                    if (output.getType() == Entity.Type.DFS_DIR || output.getType() == Entity.Type.LOCAL_DIR) {
                        if (output.getWriteType() == WriteEntity.WriteType.PATH_WRITE && output.isTempURI()) {
                            ret = true;
                        }
                    }

                }
            }
        }

        return ret;
    }
}
