package com.epam.openspaces.persistency.kafka;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider;

import static org.junit.Assert.fail;

public class XapIntegratedTest {
    static ProcessingUnitContainer space;
    static ProcessingUnitContainer mirror;

    @BeforeClass
    public static void init() throws Exception {
        space = createSpace();
        mirror = createMirror();
    }

    @Test
    public void test() throws InterruptedException {
        fail("Not implemented yet");
    }

    private static ProcessingUnitContainer createSpace() throws IOException {
        IntegratedProcessingUnitContainerProvider container = new IntegratedProcessingUnitContainerProvider();
        container.addConfigLocation("space.xml");
        ClusterInfo spaceClusterInfo = new ClusterInfo();
        spaceClusterInfo.setNumberOfInstances(1);
        spaceClusterInfo.setNumberOfBackups(1);
        spaceClusterInfo.setSchema("partitioned-sync2backup");
        container.setClusterInfo(spaceClusterInfo);
        return container.createContainer();
    }

    private static ProcessingUnitContainer createMirror() throws IOException {
        IntegratedProcessingUnitContainerProvider container = new IntegratedProcessingUnitContainerProvider();
        container.addConfigLocation("mirror.xml");
        return container.createContainer();
    }

    @AfterClass
    public static void shutdown() {
        space.close();
        mirror.close();
    }
}
