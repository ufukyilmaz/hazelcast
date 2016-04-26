package com.hazelcast.spi.hotrestart.impl.di;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DiContainerTest {

    private final DiContainer di = new DiContainer();

    @Test
    public void when_depInstance_then_getIt() {
        di.dep(new T1());
        assertNotNull(di.get(T1.class));
    }

    @Test
    public void when_depNamedInstance_then_getIt() {
        di.dep("t1", new T1());
        assertNotNull(di.get("t1"));
    }

    @Test
    public void when_depClass_then_getIt() {
        di.dep(T2.class);
        assertNotNull(di.get(T2.class));
    }

    @Test
    public void when_depNamedClass_then_getIt() {
        di.dep("t2", T2.class);
        assertNotNull(di.get("t2"));
    }

    @Test
    public void when_injectIntoCxor_then_getDep() {
        di.dep(new T1())
          .dep(new T2())
          .dep(T5.class);
        final T5 t5 = di.get(T5.class);
        assertNotNull(t5);
        assertNotNull(t5.t1);
        assertNotNull(t5.t2);
    }

    @Test
    public void when_injectIntoCxorByName_then_getDep() {
        di.dep("t1", new T1())
          .dep("t2", new T2())
          .dep(T4.class);
        final T4 t4 = di.get(T4.class);
        assertNotNull(t4);
        assertNotNull(t4.t1);
        assertNotNull(t4.t2);
    }

    @Test
    public void when_injectIntoField_then_getDep() {
        di.dep(new T1())
          .dep(new T2())
          .wireAndInitializeAll();
        final T1 t1 = di.get(T1.class);
        assertNotNull(t1);
        assertNotNull(t1.t2);
    }

    @Test
    public void when_injectIntoFieldByName_then_getDep() {
        di.dep("t2", new T2())
          .dep(new T3())
          .wireAndInitializeAll();
        final T3 t3 = di.get(T3.class);
        assertNotNull(t3);
        assertNotNull(t3.t2);
    }

    @Test
    public void when_injectIntoMethod_then_getDep() {
        di.dep(new T1())
          .dep(new T2())
          .dep(new T6())
          .wireAndInitializeAll();
        final T6 t6 = di.get(T6.class);
        assertNotNull(t6);
        assertNotNull(t6.t1);
        assertNotNull(t6.t2);
    }

    @Test
    public void when_injectIntoMethodByName_then_getDep() {
        final T2 t2a = new T2();
        final T2 t2b = new T2();
        di.dep("t2a", t2a)
          .dep("t2b", t2b)
          .dep(new T7())
          .wireAndInitializeAll();
        final T7 t7 = di.get(T7.class);
        assertNotNull(t7);
        assertSame(t2a, t7.t2a);
        assertSame(t2b, t7.t2b);
    }

    @Test
    public void when_hasInitialize_then_calledInRegistrationOrder() {
        final int depCount = 1000;
        final List<T8> deps = new ArrayList<T8>();
        for (int i = 0; i < depCount; i++) {
            final T8 dep = new T8();
            deps.add(dep);
            di.dep("t8_" + i, dep);
        }
        di.dep(new Cntr()).wireAndInitializeAll();
        int i = 1;
        for (T8 dep : deps) {
            assertEquals(i++, dep.initOrder);
        }
    }

    @Test
    public void when_createSubContainer_then_seeItsContents() {
        di.dep(new T2());
        final DiContainer di2 = new DiContainer(di);
        di2.dep(new T1()).wireAndInitializeAll();
        final T1 t1 = di2.get(T1.class);
        assertNotNull(t1);
        assertEquals(di.get(T2.class), t1.t2);
    }
}

class T1 {
    @Inject T2 t2;
}

class T2 {
    @Inject T2() { }
}

class T3 {
    @Inject @Name("t2") T2 t2;
}

class T4 {
    final T1 t1;
    final T2 t2;

    @Inject T4(@Name("t1") T1 t1, @Name("t2") T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }
}

class T5 {
    final T1 t1;
    final T2 t2;

    @Inject
    private T5(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }
}

class T6 {
    T1 t1;
    T2 t2;

    @Inject
    void inject(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }
}

class T7 {
    T2 t2a;
    T2 t2b;

    @Inject
    void inject(@Name("t2a") T2 t2a, @Name("t2b") T2 t2b) {
        this.t2a = t2a;
        this.t2b = t2b;
    }
}

class T8 {
    int initOrder;

    @Inject Cntr cntr;

    @Initialize
    private void init() {
        initOrder = ++cntr.count;
    }
}

class Cntr {
    int count;
}
