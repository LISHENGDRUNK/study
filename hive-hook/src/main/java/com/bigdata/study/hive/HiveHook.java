package com.bigdata.study.hive;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveHook implements ExecuteWithHookContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveHook.class);
    private static final String HIVE_HOOK_IMPL_CLASSNAME = "com.yc.MyHiveHook";
    private ExecuteWithHookContext hiveHookImpl = null;

    public HiveHook() {
        this.initialize();
    }

    @Override
    public void run(HookContext hookContext) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.run({})", hookContext);
        }

            hiveHookImpl.run(hookContext);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.run({})", hookContext);
        }
    }


    private void initialize() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> HiveHook.initialize()");
        }
        try {
            @SuppressWarnings("unchecked")
            Class<ExecuteWithHookContext> cls =
                    (Class<ExecuteWithHookContext>) Class.forName(HIVE_HOOK_IMPL_CLASSNAME);

            LOG.debug("hiveHookImp cls.newInstance()== "+cls.toString());
            hiveHookImpl = cls.newInstance();
        } catch (Exception excp) {
            LOG.error("Error instantiating Atlas hook implementation", excp);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== HiveHook.initialize()");
        }
    }
}
