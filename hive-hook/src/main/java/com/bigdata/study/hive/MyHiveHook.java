package com.bigdata.study.hive;

import com.bigdata.study.hive.events.*;
import com.bigdata.study.hive.kafka.HookNotificationEntity;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyHiveHook extends AbstractHook implements ExecuteWithHookContext {


    private static Logger LOG = LoggerFactory.getLogger(MyHiveHook.class);

    private static final Map<String, HiveOperation> OPERATION_MAP = new HashMap<String, HiveOperation>();
    public static final String  APPLICATION_PROPERTIES     = "hook.properties";
    public static final String CONF_CLUSTER_NAME              = "atlas.cluster.name";
    protected static Configuration hookProperties;
    private static final String   clusterName;
    public static final String DEFAULT_CLUSTER_NAME = "primary";

    static{

        try {
             hookProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        for (HiveOperation hiveOperation : HiveOperation.values()){
            OPERATION_MAP.put(hiveOperation.getOperationName(), hiveOperation);
        }

        clusterName    = hookProperties.getString(CONF_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    }


    public MyHiveHook() {
    }


    public void run(HookContext hookContext) throws Exception {

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.run({})", hookContext.getOperationName());
        }

        try {
            HiveOperation oper = OPERATION_MAP.get(hookContext.getOperationName());
            HiveHookContext context = new HiveHookContext(this, oper, hookContext);
            BaseHiveEvent event = null;
            final UserGroupInformation ugi = hookContext.getUgi() == null ? Utils.getUGI() : hookContext.getUgi();
            switch (oper){

                // 创建库
                case CREATEDATABASE:
                    event = new CreateDatabase(context);
                    break;
                case DROPDATABASE:
                    // 1. 删除源数据库，以及库里的表、表字段
                    // 2. 删除库内所有表血缘
                    event = new DropDatabase(context);
                    break;
                case ALTERDATABASE:
                case ALTERDATABASE_OWNER:
                    // 1. hive没有修改数据库名的语法，所以无表、字段、备注等元数据操作，但是操作人修改人要记录
                    event  = new AlterDatabase(context);
                    break;
                case CREATETABLE:
                    // 1. 创建表、以及字段
                    // 2. 从hdfs 建表的过程记录成血缘会引起冲突，所以不记录
                    event = new CreateTable(context,true);
                    break;
                case DROPTABLE:
                case DROPVIEW:
                    //1. 删除表，以及表字段
                    //2. 删除表血缘关系
                    event = new DropTable(context);
                    break;
                case TRUNCATETABLE:
                    //1. 只针对内部表删除血缘
                    event = new TruncateTable(context);
                    break;
                case CREATETABLE_AS_SELECT: // 新建表、新建血缘
                case CREATEVIEW: // 创建视图 --> 新建元数据、新建血缘
                case ALTERVIEW_AS:
                case QUERY: // 查询 --> insert into as 也是一种血缘
                    event = new CreateHiveProcess(context);
                    break;
                case ALTERTABLE_FILEFORMAT:
                case ALTERTABLE_CLUSTER_SORT:
                case ALTERTABLE_BUCKETNUM:
                case ALTERTABLE_PROPERTIES:
                case ALTERVIEW_PROPERTIES:
                case ALTERTABLE_SERDEPROPERTIES:
                case ALTERTABLE_SERIALIZER:
                case ALTERTABLE_ADDCOLS:
                case ALTERTABLE_REPLACECOLS:
                case ALTERTABLE_PARTCOLTYPE:
                case ALTERTABLE_LOCATION:
                case ALTERTABLE_RENAMECOL:
                    //1. 覆盖表，以及表字段
                    //2. hive修改表 --> 不产生血缘  修改库、表ddl时间 操作者
                    event = new AlterTable(context,true);
                    break;
                case ALTERTABLE_RENAME:
                case ALTERVIEW_RENAME:
                    //1. 修改表名
                    event = new AlterTableRename(context);
                    break;
                default:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("HiveHook.run({}): operation ignored", hookContext.getOperationName());
                    }
                    break;
            }

            if (event != null){
                List<HookNotificationEntity> list=new ArrayList();
                list.add(event.getMetaAndLineage());
                //// 将对应hive 产生的事件信息发送到kafka topic "HIVE_HOOK"   使用父类的方法
                super.notifyEntities(list, ugi);
            }
        } catch (Throwable t) {
            LOG.error("HiveHook.run(): failed to process operation {}", hookContext.getOperationName(), t);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.run({})", hookContext.getOperationName());
        }
    }


    public String getClusterName(){
        return clusterName;
    }

}
