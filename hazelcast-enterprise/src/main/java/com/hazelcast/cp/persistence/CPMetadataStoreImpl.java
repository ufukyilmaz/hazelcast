package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.internal.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Persists and restores CP member metadata of the local member.
 */
public class CPMetadataStoreImpl implements CPMetadataStore {

    static final String CP_MEMBER_FILE_NAME = "cp-member";
    private static final String METADATA_GROUP_ID_FILE_NAME_PREFIX = "metadata-group-id-";

    private final File dir;

    CPMetadataStoreImpl(File dir) {
        this.dir = dir;
    }

    @Override
    public boolean isMarkedAPMember() {
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        return file.exists() && file.length() == 0;
    }

    @Override
    public boolean tryMarkAPMember() throws IOException {
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        if (file.exists()) {
            return file.length() == 0;
        }
        return file.createNewFile();
    }

    @Override
    public boolean containsLocalMemberFile() {
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        return file.exists() && file.length() > 0;
    }

    @Override
    public void persistLocalCPMember(CPMember member) throws IOException {
        File tmp = new File(dir, CP_MEMBER_FILE_NAME + ".tmp");
        FileOutputStream fileOutputStream = new FileOutputStream(tmp);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        try {
            writeUUID(out, member.getUuid());
            Address address = member.getAddress();
            out.writeUTF(address.getHost());
            out.writeInt(address.getPort());
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            closeResource(fileOutputStream);
            closeResource(out);
        }
        IOUtil.rename(tmp, new File(dir, CP_MEMBER_FILE_NAME));
    }

    @Override
    public CPMemberInfo readLocalCPMember() throws IOException {
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        if (!file.exists() || file.length() == 0) {
            return null;
        }
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        try {
            UUID uuid = readUUID(in);
            Address address = new Address(in.readUTF(), in.readInt());
            return new CPMemberInfo(uuid, address);
        } finally {
            closeResource(in);
        }
    }

    @Override
    public void persistMetadataGroupId(RaftGroupId groupId) throws IOException {
        String fileName = getMetadataGroupIdFileName(groupId);
        File tmp = new File(dir, "tmp-" + fileName);
        FileOutputStream fileOutputStream = new FileOutputStream(tmp);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
        try {
            out.writeUTF(groupId.getName());
            out.writeLong(groupId.getSeed());
            out.writeLong(groupId.getId());
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            closeResource(fileOutputStream);
            closeResource(out);
        }
        IOUtil.rename(tmp, new File(dir, fileName));
        deleteStaleMetadataGroupIdFiles(groupId);
    }

    private void deleteStaleMetadataGroupIdFiles(RaftGroupId groupId) {
        String latestMetadataGroupIdFileName = getMetadataGroupIdFileName(groupId);
        String[] metadataGroupIdFileNames = getMetadataGroupIdFileNames();

        assert metadataGroupIdFileNames != null && metadataGroupIdFileNames.length > 0;

        Arrays.sort(metadataGroupIdFileNames);
        for (String name : metadataGroupIdFileNames) {
            if (name.equals(latestMetadataGroupIdFileName)) {
                return;
            }

            IOUtil.deleteQuietly(new File(dir, name));
        }
    }

    @Override
    public RaftGroupId readMetadataGroupId() throws IOException {
        String[] metadataGroupIdFileNames = getMetadataGroupIdFileNames();
        if (metadataGroupIdFileNames == null || metadataGroupIdFileNames.length == 0) {
            return null;
        }

        Arrays.sort(metadataGroupIdFileNames);
        String metadataGroupIdFileName = metadataGroupIdFileNames[metadataGroupIdFileNames.length - 1];

        File file = new File(dir, metadataGroupIdFileName);
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        try {
            String name = in.readUTF();
            long seed = in.readLong();
            long commitIndex = in.readLong();
            return new RaftGroupId(name, seed, commitIndex);
        } finally {
            closeResource(in);
        }
    }

    private String[] getMetadataGroupIdFileNames() {
        return dir.list((dir, name) -> name.startsWith(METADATA_GROUP_ID_FILE_NAME_PREFIX));
    }

    private String getMetadataGroupIdFileName(RaftGroupId groupId) {
        return String.format(METADATA_GROUP_ID_FILE_NAME_PREFIX + "%016x", groupId.getSeed());
    }

    static boolean isCPMemberFile(File dir, String fileName) {
        if (!(dir.exists() && dir.isDirectory())) {
            return false;
        }

        File file = new File(dir, fileName);
        return file.exists() && file.isFile() && fileName.equals(CP_MEMBER_FILE_NAME);
    }

    static boolean isMetadataGroupIdFile(File dir, String fileName) {
        File file = new File(dir, fileName);
        return file.exists() && file.isFile() && fileName.startsWith(METADATA_GROUP_ID_FILE_NAME_PREFIX);
    }

    static boolean isCPDirectory(File dir) {
        return isCPMemberFile(dir, CP_MEMBER_FILE_NAME);
    }

}
