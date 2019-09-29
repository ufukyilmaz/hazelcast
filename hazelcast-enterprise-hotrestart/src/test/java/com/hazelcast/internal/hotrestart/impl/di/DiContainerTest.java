package com.hazelcast.internal.hotrestart.impl.di;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiContainerTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final DiContainer di = new DiContainer();

    @Test
    public void when_depInstance_then_getIt() {
        // Given
        di.dep(new T1());

        // When
        final T1 t1 = di.get(T1.class);

        // Then
        assertSame(t1, di.get(T1.class));
    }

    @Test
    public void when_depNamedInstance_then_getIt() {
        // Given
        di.dep("t1", new T1());

        // When
        final Object t1 = di.get("t1");

        // Then
        assertSame(t1, di.get("t1"));
    }

    @Test
    public void when_depClass_then_getIt() {
        // Given
        di.dep((Object) T2.class); // upcast to exercise a codepath in dep(Object)

        // When
        final T2 t2 = di.get(T2.class);

        // Then
        assertNotNull(t2);
        assertSame(t2, di.get(T2.class));
    }

    @Test
    public void when_depDeclaredClassWithInstanceOfDifferentClass_then_getIt() {
        // Given
        final T2 t2 = new T2();
        di.dep(Object.class, t2);

        // When - Then
        assertSame(t2, di.get(Object.class));
    }

    @Test
    public void when_depDeclaredClassAndActualClass_then_getIt() {
        // Given
        di.dep(T2.class, T2Sub.class);

        // When
        final T2 t2 = di.get(T2.class);

        // Then
        assertNotNull(t2);
        assertSame(t2, di.get(T2.class));
    }

    @Test
    public void when_depNamedClass_then_getIt() {
        // Given
        di.dep("t2", T2.class);

        // When
        final Object t2 = di.get("t2");

        // Then
        assertNotNull(t2);
        assertSame(t2, di.get("t2"));
    }

    @Test
    public void when_depAfterInitializeAll_then_wiredImmediately() {
        // Given
        di.dep(new T2());
        di.wireAndInitializeAll();

        // When
        di.dep(new T1());

        // Then
        assertNotNull(di.get(T1.class).t2);
    }

    @Test
    public void when_depDisposable_then_disposeCalled() {
        // Given
        final DisposableThing dt = new DisposableThing();
        di.dep(dt).disposable();

        // When
        di.dispose();

        // Then
        assertTrue(dt.disposed);
    }

    @Test
    public void when_injectIntoCxor_then_getDep() {
        // Given
        di.dep(new T1())
                .dep(new T2());

        // When
        di.dep(T5.class);

        // Then
        final T5 t5 = di.get(T5.class);
        assertNotNull(t5);
        assertNotNull(t5.t1);
        assertNotNull(t5.t2);
    }

    @Test
    public void when_injectIntoCxorByName_then_getDep() {
        // Given
        di.dep("t1", new T1())
                .dep("t2", new T2());

        // When
        di.dep(T4.class);

        // Then
        final T4 t4 = di.get(T4.class);
        assertNotNull(t4);
        assertNotNull(t4.t1);
        assertNotNull(t4.t2);
    }

    @Test
    public void when_injectIntoField_then_getDep() {
        // Given
        di.dep(new T1())
                .dep(new T2());

        // When
        di.wireAndInitializeAll();

        // Then
        final T1 t1 = di.get(T1.class);
        assertNotNull(t1);
        assertNotNull(t1.t2);
    }

    @Test
    public void when_injectIntoField_butDepMissing_then_exception() {
        // Given
        di.dep(new T1());

        // Then
        exceptionRule.expect(DiException.class);

        // When
        di.wireAndInitializeAll();
    }

    @Test
    public void when_injectIntoFieldByName_then_getDep() {
        // Given
        di.dep("t2", new T2())
                .dep(new T3());

        // When
        di.wireAndInitializeAll();

        // Then
        final T3 t3 = di.get(T3.class);
        assertNotNull(t3);
        assertNotNull(t3.t2);
    }

    @Test
    public void when_injectIntoFieldByName_butNameMissing_then_Exception() {
        // Given
        di.dep("brokent2", new T2())
                .dep(new T3());

        // Then
        exceptionRule.expect(DiException.class);

        // When
        di.wireAndInitializeAll();
    }

    @Test
    public void when_injectIntoMethod_then_getDep() {
        // Given
        di.dep(new T1())
                .dep(new T2())
                .dep(new T6());

        // When
        di.wireAndInitializeAll();

        // Then
        final T6 t6 = di.get(T6.class);
        assertNotNull(t6);
        assertNotNull(t6.t1);
        assertNotNull(t6.t2);
    }

    @Test
    public void when_injectIntoMethod_butMethodFails_then_exception() {
        // Given
        di.dep(new WithDefectiveMethods(0))
                .dep(new T2());

        // Then
        exceptionRule.expect(DiException.class);

        // When
        di.wireAndInitializeAll();
    }

    @Test
    public void when_injectIntoMethodByName_then_getDep() {
        // Given
        final T2 t2a = new T2();
        final T2 t2b = new T2();
        di.dep("t2a", t2a)
                .dep("t2b", t2b)
                .dep(new T7());

        // When
        di.wireAndInitializeAll();

        // Then
        final T7 t7 = di.get(T7.class);
        assertNotNull(t7);
        assertSame(t2a, t7.t2a);
        assertSame(t2b, t7.t2b);
    }

    @Test
    public void when_hasInitialize_then_calledInRegistrationOrder() {
        // Given
        final int depCount = 1000;
        final List<T8> deps = new ArrayList<T8>();
        for (int i = 0; i < depCount; i++) {
            final T8 dep = new T8();
            deps.add(dep);
            di.dep("t8_" + i, dep);
        }
        di.dep(new Cntr());

        // When
        di.wireAndInitializeAll();

        // Then
        int i = 1;
        for (T8 dep : deps) {
            assertEquals(i++, dep.initOrder);
        }
    }

    @Test
    public void when_instantiate_butCxorFails_then_exception() {
        exceptionRule.expect(DiException.class);
        di.instantiate(WithDefectiveMethods.class);
    }

    @Test
    public void when_instantiateWithoutInjectCxor_then_exception() {
        exceptionRule.expect(DiException.class);
        di.instantiate(T1.class);
    }

    @Test
    public void when_instantiateAbstractClass_then_exception() {
        exceptionRule.expect(DiException.class);
        di.instantiate(AbstractClass.class);
    }

    @Test
    public void getByNameAssertingType() {
        // Given
        final T2 t2 = new T2();
        di.dep("t2", t2);

        // When - Then
        assertSame(t2, di.get("t2", T2.class));
    }

    @Test
    public void when_callWireAndInitializeAll_twice_thenFail() {
        di.wireAndInitializeAll();
        exceptionRule.expect(DiException.class);
        di.wireAndInitializeAll();
    }

    @Test
    public void when_createSubContainer_then_seeItsContents() {
        // Given
        final T2 t2ByType = new T2();
        final T2 t2ByName = new T2();
        final String t2Name = "t2";
        di.dep(t2ByType);
        di.dep(t2Name, t2ByName);

        // When
        final DiContainer subContainer = new DiContainer(di);
        final T1 t1 = new T1();
        subContainer.dep(t1).wireAndInitializeAll();

        // Then
        assertSame(t2ByType, t1.t2);
        assertSame(subContainer.get(t2Name), t2ByName);
    }

    @Test
    public void invokeGivenMethod() {
        // Given
        final T2 t2 = new T2();
        di.dep(t2);
        final WithMethodToInvoke wmti = new WithMethodToInvoke();

        // When
        di.invoke(wmti, "invokeMe");

        // Then
        assertSame(t2, wmti.t2);
    }

    @Test
    public void when_invokeGivenMethod_butMethodNotFound_then_exception() {
        // Given
        final T2 t2 = new T2();
        di.dep(t2);
        final WithMethodToInvoke wmti = new WithMethodToInvoke();

        // Then
        exceptionRule.expect(DiException.class);

        // When
        di.invoke(wmti, "dontInvokeMe");
    }
}

class T1 {
    @Inject
    T2 t2;
}

class T2 {
    @Inject
    T2() {
    }
}

class T2Sub extends T2 {
    @Inject
    T2Sub() {
    }
}

class T3 {
    @Inject
    @Name("t2")
    T2 t2;
}

class T4 {
    final T1 t1;
    final T2 t2;

    @Inject
    T4(@Name("t1") T1 t1, @Name("t2") T2 t2) {
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

    @Inject
    Cntr cntr;

    @Initialize
    private void init() {
        initOrder = ++cntr.count;
    }
}

class DisposableThing implements Disposable {
    boolean disposed;

    @Override
    public void dispose() {
        disposed = true;
    }
}

class WithMethodToInvoke {
    T2 t2;

    public void invokeMe(T2 t2) {
        this.t2 = t2;
    }
}

class Cntr {
    int count;
}

class WithDefectiveMethods {
    @Inject
    WithDefectiveMethods() {
        throw new UnsupportedOperationException("I am a defective constructor");
    }

    WithDefectiveMethods(int ignored) {
    }

    @Inject
    void defectiveMethod(T2 t2) {
        throw new UnsupportedOperationException("I am a defective method");
    }
}

abstract class AbstractClass {
    @Inject
    AbstractClass() {
    }
}
