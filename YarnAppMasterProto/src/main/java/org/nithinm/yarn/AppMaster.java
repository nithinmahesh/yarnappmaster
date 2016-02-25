package org.nithinm.yarn;

import org.apache.hadoop.util.ThreadUtil;

/**
 * Created by nithinm on 2/17/2016.
 */
public class AppMaster {
    private static int processRuntimeMinutes = 10; // 10 minutes

    public static void main(String[] args) {
        AppMasterService service = new AppMasterService();
        service.start();
        // ThreadUtil.sleepAtLeastIgnoreInterrupts(processRuntimeMinutes * 1000 * 60);
        // service.shutdown();
    }
}
