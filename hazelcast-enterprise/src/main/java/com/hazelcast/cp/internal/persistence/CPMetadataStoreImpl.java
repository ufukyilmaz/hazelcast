package com.hazelcast.cp.internal.persistence;

import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.cp.internal.persistence.FileIOSupport.TMP_SUFFIX;
import static com.hazelcast.cp.internal.persistence.FileIOSupport.readWithChecksum;
import static com.hazelcast.cp.internal.persistence.FileIOSupport.writeWithChecksum;
import static com.hazelcast.internal.nio.IOUtil.deleteQuietly;
import static java.util.Arrays.sort;

/**
 * Persists and restores CP member metadata of the local member.
 */
public class CPMetadataStoreImpl implements CPMetadataStore {

    static final String CP_MEMBER_FILE_NAME = "cp-member";
    static final String ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX = "active-members-";
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
        String fileName = getActiveCpMembersFileName(commitIndex);
        writeWithChecksum(dir, fileName, serializationService, out -> {
            out.writeLong(commitIndex);
            out.writeInt(members.size());
            for (CPMember member : members) {
                out.writeObject(member);
            }
        });
        deleteStaleFiles(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX);
    }

    @Override
    public long readActiveCPMembers(Collection<CPMember> members) throws IOException {
        String latestFileName = getLatestFileName(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX);
        if (latestFileName == null) {
            return 0L;
        }

        try {
            Long result = readWithChecksum(dir, latestFileName, serializationService, in -> {
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
        String fileName = getMetadataGroupIdFileName(groupId.getSeed());
        writeWithChecksum(dir, fileName, serializationService, out -> out.writeObject(groupId));
        deleteStaleFiles(METADATA_GROUP_ID_FILE_NAME_PREFIX);
    }

    @Override
    public RaftGroupId readMetadataGroupId() throws IOException {
        String latestFileName = getLatestFileName(METADATA_GROUP_ID_FILE_NAME_PREFIX);
        if (latestFileName == null) {
            return null;
        }

        try {
            return readWithChecksum(dir, latestFileName, serializationService, ObjectDataInput::readObject);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot read metadata group id", e);
        }
    }

    private String getLatestFileName(String prefix) {
        String[] fileNames = getFileNamesWithPrefix(prefix);
        if (fileNames == null || fileNames.length == 0) {
            return null;
        }

        sort(fileNames);
        return fileNames[fileNames.length - 1];
    }

    private void deleteStaleFiles(String prefix) {
        String[] allFileNames = getFileNamesWithPrefix(prefix);
        assert allFileNames != null && allFileNames.length > 0;

        sort(allFileNames);
        String latestFileName = allFileNames[allFileNames.length - 1];

        for (String name : allFileNames) {
            if (name.equals(latestFileName)) {
                continue;
            }
            deleteQuietly(new File(dir, name));
        }
    }

    static String getMetadataGroupIdFileName(RaftGroupId groupId) {
        return getFileNameWithPrefixAndSuffix(METADATA_GROUP_ID_FILE_NAME_PREFIX, groupId.getSeed());
    }

    private static String getMetadataGroupIdFileName(long seed) {
        return getFileNameWithPrefixAndSuffix(METADATA_GROUP_ID_FILE_NAME_PREFIX, seed);
    }

    private static String getActiveCpMembersFileName(long commitIndex) {
        return getFileNameWithPrefixAndSuffix(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX, commitIndex);
    }

    private static String getFileNameWithPrefixAndSuffix(String prefix, long suffix) {
        return String.format(prefix + "%016x", suffix);
    }

    private String[] getFileNamesWithPrefix(String prefix) {
        return dir.list((dir, name) -> name.startsWith(prefix) && !name.endsWith(TMP_SUFFIX));
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
