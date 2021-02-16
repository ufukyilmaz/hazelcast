package com.hazelcast.jet.impl.processor;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/**
 * A meta-supplier that will only use the given {@code ProcessorSupplier}
 * on a node with given {@link Address}.
 */
public class SpecificMemberPms implements ProcessorMetaSupplier, IdentifiedDataSerializable {

    private ProcessorSupplier supplier;
    private Address memberAddress;

    @SuppressWarnings("unused")
    SpecificMemberPms() {
    }

    public SpecificMemberPms(ProcessorSupplier supplier, Address memberAddress) {
        this.supplier = supplier;
        this.memberAddress = memberAddress;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        if (context.localParallelism() != 1) {
            throw new IllegalArgumentException(
                    "Local parallelism of " + context.localParallelism() + " was requested for a vertex that "
                            + "supports only total parallelism of 1. Local parallelism must be 1.");
        }
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (!addresses.contains(memberAddress)) {
            throw new JetException("Cluster does not contain the required member: " + memberAddress);
        }
        return addr -> addr.equals(memberAddress) ? supplier : count -> singletonList(new ExpectNothingP());
    }

    @Override
    public int preferredLocalParallelism() {
        return 1;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(supplier);
        out.writeObject(memberAddress);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        supplier = in.readObject();
        memberAddress = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetProcessorDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetProcessorDataSerializerHook.SPECIFIC_MEMBER_PMS;
    }
}