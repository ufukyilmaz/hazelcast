package com.hazelcast.cp.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.Address;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;


/**
 * Persists and restores CP member identity of the local member.
 */
public class CPMemberMetadataStoreImpl implements CPMetadataStore {

    static final String CP_METADATA_FILE_NAME = "cp-metadata";


    private final File dir;

    CPMemberMetadataStoreImpl(File dir) {
        this.dir = dir;
    }

    /**
     * Returns true if this member is marked as AP member on the storage layer.
     * If {@code false} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    @Override
    public boolean isMarkedAPMember() {
        File file = new File(dir, CP_METADATA_FILE_NAME);
        return file.exists() && file.length() == 0;
    }

    /**
     *  Marks this member as AP member on the storage layer.
     */
    @Override
    public void markAPMember() throws IOException {
        File file = new File(dir, CP_METADATA_FILE_NAME);
        boolean created = file.createNewFile();
        assert created;
    }

    /**
     * Persists {@link CPMember} identity of the local member to storage.
     */
    @Override
    public void persistLocalMember(CPMember member) throws IOException {
        File tmp = new File(dir, CP_METADATA_FILE_NAME + ".tmp");
        FileOutputStream fileOutputStream = new FileOutputStream(tmp);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        try {
            writeUUID(out, member.getUuid());
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        IOUtil.rename(tmp, new File(dir, "cp-metadata"));
    }

    /**
     * Reads {@link CPMember} identity of this member from storage.
     * If {@code null} is returned, it means that AP/CP identity of the member
     * is not not known yet CP member discovery will run.
     */
    @Override
    public CPMember readLocalMember(Address address) throws IOException {
        File file = new File(dir, CP_METADATA_FILE_NAME);
        if (!file.exists()) {
            return null;
        }
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        try {
            UUID uuid = readUUID(in);
            return new CPMemberInfo(uuid, address);
        } finally {
            IOUtil.closeResource(in);
        }
    }
}
