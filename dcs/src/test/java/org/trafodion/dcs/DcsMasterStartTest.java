package org.trafodion.dcs;


import com.esgyn.common.LicenseHelper;
import mockit.*;
import org.apache.trafodion.jdbc.t2.T2Driver;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trafodion.dcs.master.DcsMaster;
import org.trafodion.dcs.master.ServerManager;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.util.LicenseHolder;

public class DcsMasterStartTest {
    @BeforeClass
    public static void beforeClass() {
        new Expectations(LicenseHelper.class) {
            {
                // 对IPrivilege的所有实现类录制，假设测试用户有权限
                LicenseHolder.isMultiTenancyEnabled();
                result = false;
            }
        };

        new MockUp<T2Driver>(T2Driver.class) {
            //mock静态代码块
            @Mock
            void $clinit(Invocation invocation) {

            }
        };
        new MockUp<ServerManager.RestartHandler>(ServerManager.RestartHandler.class) {
            //mock静态代码块
            @Mock
            public ScriptContext call(Invocation invocation) {
                return new ScriptContext();
            }
        };
        new MockUp<ScriptManager>(ScriptManager.class) {
            //mock静态代码块
            @Mock
            public void runScript(ScriptContext ctx) {

            }
        };
    }

    @Test
    public void testMaster() throws InterruptedException {
        DcsMaster master = new DcsMaster(new String[]{"1"});
        master.join();
    }

}
