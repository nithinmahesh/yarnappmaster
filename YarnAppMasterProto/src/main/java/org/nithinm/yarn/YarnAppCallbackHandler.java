package org.nithinm.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

/**
 * Created by nithinm on 2/18/2016.
 */
public class YarnAppCallbackHandler implements AMRMClientAsync.CallbackHandler {
    public void onContainersCompleted(List<ContainerStatus> var1) {}

    public void onContainersAllocated(List<Container> var1) {}

    public void onShutdownRequest(){}

    public void onNodesUpdated(List<NodeReport> var1){}

    public float getProgress(){ return (float) 0.5; }

    public void onError(Throwable var1){}
}

