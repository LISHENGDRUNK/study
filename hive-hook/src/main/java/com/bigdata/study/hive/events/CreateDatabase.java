package com.bigdata.study.hive.events;

import com.bigdata.study.hive.HiveHookContext;
import com.bigdata.study.hive.kafka.HookNotificationEntity;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatabase extends BaseHiveEvent{

    private static final Logger LOG = LoggerFactory.getLogger(CreateDatabase.class);
    public CreateDatabase(HiveHookContext context) {
        super(context);
    }

    @Override
    public HookNotificationEntity saveMetaAndLineage() throws Exception {
        return getEntities();
    }

    public HookNotificationEntity getEntities() throws Exception {

        for (Entity entity1 : getHiveContext().getOutputs()) {
            if (entity1.getType() == Entity.Type.DATABASE) {
                Database db = entity1.getDatabase();
                if (db != null) {
                    db = getHive().getDatabase(db.getName());
                }
                if (db != null) {
                    //ConnectionInfo connInfo=  getConnInfo(connection_id);
                    //Date date = getCurrentTime();
                    //DatabaseMeta databaseMeta = new DatabaseMeta(connInfo.getId(), db.getName(), date, date);//时间请自行传入
                    //iDatabaseMetaService.createDB(databaseMeta);
                    HookNotificationEntity entity =new HookNotificationEntity();
                    entity.setOperationName(getHiveContext().getOperationName());
                    entity.setConnectionId(connection_id);
                    entity.setDatabaseName(db.getName());
                    //entity.setTableType(1);
                    //entity.setTableName(table.getTableName());
                    entity.setOperator(getUserName());
                    entity.setOperatTime(getCurrentTime());
                    entity.setSqlStr(getHiveContext().getQueryPlan().getQueryStr());
                    //entity.setInputs();
                    //entity.setOutputs();
                    //entity.setNewName();
                    return  entity;
                    // TODO 保存日志
                    //AtlasEntity dbEntity = toDbEntity(db);
                    //ret.addEntity(dbEntity);
                } else {
                    LOG.error("CreateDatabase.getEntities(): failed to retrieve db");
                }
            }
        }
        //addProcessedEntities(ret);
        return null;

    }
}
