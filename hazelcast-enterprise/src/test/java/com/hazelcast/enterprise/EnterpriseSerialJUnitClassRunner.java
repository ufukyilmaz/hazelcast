package com.hazelcast.enterprise;

import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseSerialJUnitClassRunner extends HazelcastSerialClassRunner {

    public EnterpriseSerialJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    public EnterpriseSerialJUnitClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        super.runChild(method, notifier);
    }

    @Override
    public void run(RunNotifier notifier) {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        super.run(notifier);
    }
}
