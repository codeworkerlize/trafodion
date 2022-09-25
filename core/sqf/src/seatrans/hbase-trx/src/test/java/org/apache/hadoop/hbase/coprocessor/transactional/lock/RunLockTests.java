package org.apache.hadoop.hbase.coprocessor.transactional.lock;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.apache.log4j.BasicConfigurator;

public class RunLockTests {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Result result = JUnitCore.runClasses(TestAll.class);
        boolean hasFailure = false;
        for (Failure fail : result.getFailures()) {
            if (!hasFailure) {
                hasFailure = true;
            }
            System.out.println(fail.toString());
        }
        if (!hasFailure && result.wasSuccessful()) {
            System.out.println("All tests finished successfully...");
        } else {
            System.out.println("All tests finished with failure");
        }
        System.exit(1);
    }
}
