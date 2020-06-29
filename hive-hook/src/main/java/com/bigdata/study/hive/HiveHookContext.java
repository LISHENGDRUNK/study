package com.bigdata.study.hive;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveHookContext {

    public static final char   QNAME_SEP_CLUSTER_NAME = '@';
    public static final char   QNAME_SEP_ENTITY_NAME  = '.';
    public static final char   QNAME_SEP_PROCESS      = ':';
    public static final String TEMP_TABLE_PREFIX      = "_temp-";

    private final MyHiveHook hook;
    private final HiveOperation hiveOperation;
    private final HookContext hiveContext;
    private final Hive hive;


    public HiveHookContext(MyHiveHook hook, HiveOperation hiveOperation, HookContext hiveContext) throws Exception {
        this.hook = hook;
        this.hiveOperation = hiveOperation;
        this.hiveContext = hiveContext;
        this.hive = Hive.get(hiveContext.getConf());
    }

    public String getClusterName() {
        return hook.getClusterName();
    }

    public HiveOperation getHiveOperation() {
        return hiveOperation;
    }

    public HookContext getHiveContext() {
        return hiveContext;
    }

    public Hive getHive() {
        return hive;
    }

    public MyHiveHook getHook() {
        return hook;
    }

    public String getQualifiedName(Database db) {
        return (db.getName() + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
    }

    public String getQualifiedName(Table table) {
        String tableName = table.getTableName();

        if (table.isTemporary()) {
            if (SessionState.get() != null && SessionState.get().getSessionId() != null) {
                tableName = tableName + TEMP_TABLE_PREFIX + SessionState.get().getSessionId();
            } else {
                tableName = tableName + TEMP_TABLE_PREFIX + RandomStringUtils.random(10);
            }
        }

        return (table.getDbName() + QNAME_SEP_ENTITY_NAME + tableName + QNAME_SEP_CLUSTER_NAME).toLowerCase() + getClusterName();
    }

}
