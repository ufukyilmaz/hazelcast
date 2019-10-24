package com.hazelcast.cp.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.persistence.CPMetadataStore;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.cp.persistence.FileIOSupport.TMP_SUFFIX;
import static com.hazelcast.cp.persistence.FileIOSupport.readWithChecksum;
import static com.hazelcast.cp.persistence.FileIOSupport.writeWithChecksum;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static java.util.Arrays.sort;

/**
 * Persists and restores CP member metadata of the local member.
 */
public class CPMetadataStoreImpl implements CPMetadataStore {

    static final String CP_MEMBER_FILE_NAME = "cp-member";
    static final String ACTIVE_CP_MEMBERS_FILE_NAME = "active-members";
    private static final String METADATA_GROUP_ID_FILE_NAME_PREFIX = "metadata-group-id-";

    private final File dir;
    private final InternalSerializationService serializationService;

    CPMetadataStoreImpl(File dir, InternalSerializationService serializationService) {
        this.dir = dir;
        this.serializationService = serializationService;
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
        writeWithChecksum(dir, CP_MEMBER_FILE_NAME, serializationService, out -> out.writeObject(member));
    }

    @Override
    public CPMemberInfo readLocalCPMember() throws IOException {
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        if (!file.exists() || file.length() == 0) {
            return null;
        }
        try {
            return readWithChecksum(dir, CP_MEMBER_FILE_NAME, serializationService, ObjectDataInput::readObject);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot read local CP member", e);
        }
    }

    @Override
    public void persistActiveCPMembers(Collection<? extends CPMember> members, long commitIndex) throws IOException {
        writeWithChecksum(dir, ACTIVE_CP_MEMBERS_FILE_NAME, serializationService, out -> {
            out.writeLong(commitIndex);
            out.writeInt(members.size());
            for (CPMember member : members) {
                out.writeObject(member);
            }
        });
    }

    @Override
    public long readActiveCPMembers(Collection<CPMember> members) throws IOException {
        try {
            Long result = readWithChecksum(dir, ACTIVE_CP_MEMBERS_FILE_NAME, serializationService, in -> {
                long commitIndex = in.readLong();
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    CPMember member = in.readObject();
                    members.add(member);
                }
                return commitIndex;
            });
            return result != null ? result : 0L;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot read active CP members", e);
        }
    }

    @Override
    public void persistMetadataGroupId(RaftGroupId groupId) throws IOException {
        String fileName = getMetadataGroupIdFileName(groupId);
        writeWithChecksum(dir, fileName, serializationService, out -> out.writeObject(groupId));
        deleteStaleMetadataGroupIdFiles(groupId);
    }

    private void deleteStaleMetadataGroupIdFiles(RaftGroupId groupId) {
        String latestMetadataGroupIdFileName = getMetadataGroupIdFileName(groupId);
        String[] metadataGroupIdFileNames = getMetadataGroupIdFileNames();

        assert metadataGroupIdFileNames != null && metadataGroupIdFileNames.length > 0;

        for (String name : metadataGroupIdFileNames) {
            if (name.equals(latestMetadataGroupIdFileName)) {
                return;
            }
            deleteQuietly(new File(dir, name));
        }
    }

    @Override
    public RaftGroupId readMetadataGroupId() throws IOException {
        String[] metadataGroupIdFileNames = getMetadataGroupIdFileNames();
        if (metadataGroupIdFileNames == null || metadataGroupIdFileNames.length == 0) {
            return null;
        }

        sort(metadataGroupIdFileNames);
        String fileName = metadataGroupIdFileNames[metadataGroupIdFileNames.length - 1];

        try {
            return readWithChecksum(dir, fileName, serializationService, ObjectDataInput::readObject);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot read metadata group id", e);
        }
    }

    private String[] getMetadataGroupIdFileNames() {
        return dir.list((dir, name) ->
                name.startsWith(METADATA_GROUP_ID_FILE_NAME_PREFIX) && !name.endsWith(TMP_SUFFIX));
    }

    static String getMetadataGroupIdFileName(RaftGroupId groupId) {
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
