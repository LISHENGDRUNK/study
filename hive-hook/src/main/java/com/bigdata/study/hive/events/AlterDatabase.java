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
import org.apache.hadoop.hive.ql.hooks.Entity;

public class AlterDatabase extends CreateDatabase {
    public AlterDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public HookNotificationEntity saveMetaAndLineage() throws Exception {
        //boolean ret = true;
        //AtlasEntitiesWithExtInfo entities =
        //        getEntities();
        //if (entities != null && CollectionUtils.isNotEmpty(entities.getEntities())) {
        //    //ret = Collections.singletonList(new EntityUpdateRequestV2(getUserName(), entities));
        //}
        for (Entity entity1 : getHiveContext().getOutputs()) {
            if (entity1.getType() == Entity.Type.DATABASE) {
                HookNotificationEntity entity = new HookNotificationEntity();
                entity.setOperationName(getHiveContext().getOperationName());
                entity.setConnectionId(connection_id);
                entity.setDatabaseName(entity1.getDatabase().getName());
                //entity.setTableType(1);
                //entity.setTableName(table.getTableName());
                entity.setOperator(getUserName());
                entity.setOperatTime(getCurrentTime());
                entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
                //entity.setInputs();
                //entity.setOutputs();
                //entity.setNewName();
                return entity;
            }
        }
        return  null;
    }
}
