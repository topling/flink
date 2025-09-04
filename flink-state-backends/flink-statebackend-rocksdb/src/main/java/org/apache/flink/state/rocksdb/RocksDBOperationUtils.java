/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.rocksdb;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.math.NumberUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.ImportColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SidePluginRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/** Utils for RocksDB Operations. */
public class RocksDBOperationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBOperationUtils.class);

    public static RocksDB openDB(
            String path,
            List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
            List<ColumnFamilyHandle> stateColumnFamilyHandles,
            ColumnFamilyOptions columnFamilyOptions,
            DBOptions dbOptions)
            throws IOException {
        // System.err.printf("RocksDBOperationUtils.openDB: %s%n", path);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

        RocksDB dbRef;

        try {
            // Ensure that the working directory exists and is a directory to make RocksDB happy
            File pathFile = new File(Preconditions.checkNotNull(path));

            if (!pathFile.exists() && !pathFile.mkdirs() && !pathFile.isDirectory()) {
                throw new IOException(
                        "Could not create working directory for RocksDB instance: " + path);
            }

            dbRef =
                    toplingdbOpen(
                            Preconditions.checkNotNull(dbOptions),
                            path,
                            columnFamilyDescriptors,
                            stateColumnFamilyHandles);
        } catch (Exception e) {
            IOUtils.closeQuietly(columnFamilyOptions);
            columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));

            // improve error reporting on Windows
            throwExceptionIfPathLengthExceededOnWindows(path, e);

            throw new IOException("Error while opening RocksDB instance.", e);
        }

        // requested + default CF
        Preconditions.checkState(
                1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");
        return dbRef;
    }

    private static final int TOPLINGDB_DEBUG_LEVEL =
            NumberUtils.toInt(System.getenv("SidePluginRepo_DebugLevel"), 0);

    private static final String FLINK_TOPLING_CONF = System.getenv("FLINK_TOPLINGDB_CONF");

    static SidePluginRepo loadToplingSidePluginRepo() {
        if (FLINK_TOPLING_CONF == null) {
            return null;
        }
        SidePluginRepo r = null;
        try {
            r = new SidePluginRepo();
            r.importAutoFile(FLINK_TOPLING_CONF);
        } catch (RocksDBException e) {
            throw new RuntimeException(
                    "Failed to load toplingdb conf from " + FLINK_TOPLING_CONF, e);
        }
        return r;
    }

    public static final SidePluginRepo TOPLINGDB_REPO = loadToplingSidePluginRepo();

    public static RocksDB toplingdbOpen(
            final DBOptions dbOptions,
            final String path,
            final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            final List<ColumnFamilyHandle> columnFamilyHandles)
            throws RocksDBException {
        // System.err.printf("RocksDBOperationUtils.toplingdbOpen: %s%n", FLINK_TOPLING_CONF);
        if (TOPLINGDB_REPO == null) {
            return RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);
        }
        String dboName = dboNameOf(path);
        ObjectMapper omapper = new ObjectMapper();
        omapper.disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        omapper.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
        ObjectNode root = omapper.createObjectNode();
        root.putObject("DBOptions").putObject(dboName).put("update_from", "dbo");
        ObjectNode cfoMap = root.putObject("CFOptions");
        ObjectNode dbNode = root.putObject("databases").putObject(path);
        dbNode.put("method", "DB::Open");
        dbNode = dbNode.putObject("params");
        dbNode.put("db_options", dboName);
        dbNode.put("path", path);
        ObjectNode dbcfoNode = dbNode.putObject("column_families");
        for (ColumnFamilyDescriptor cfd : columnFamilyDescriptors) {
            String cfName = cfd.getName() == null ? "default" : new String(cfd.getName());
            String cfoName = cfoNameOf(path, cfName);
            cfoMap.putObject(cfoName).put("update_from", "default");
            dbcfoNode.put(cfName, cfoName);
        }
        String strJson;
        try {
            strJson = omapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize toplingdb json config", e);
        }
        if (TOPLINGDB_DEBUG_LEVEL >= 1) {
            System.err.printf("toplingdb openDB: %s%n", strJson);
        }
        synchronized (TOPLINGDB_REPO) {
            for (ColumnFamilyDescriptor cfdesc : columnFamilyDescriptors) {
                String cfName = new String(cfdesc.getName());
                String cfoName = "cfo-" + path + "-" + cfName;
                TOPLINGDB_REPO.put(cfoName, cfdesc.getOptions());
            }
            TOPLINGDB_REPO.put(dboName, dbOptions);
            TOPLINGDB_REPO.importJson(strJson);
            return TOPLINGDB_REPO.openDB(path, columnFamilyHandles);
        }
    }

    private static String cfoNameOf(String path, String cfName) {
        return "cfo-" + path + "-" + cfName;
    }

    private static String dboNameOf(String path) {
        return "dbo-" + path;
    }

    public static RocksIteratorWrapper getRocksIterator(
            RocksDB db, ColumnFamilyHandle columnFamilyHandle, ReadOptions readOptions) {
        return new RocksIteratorWrapper(db.newIterator(columnFamilyHandle, readOptions));
    }

    public static void registerKvStateInformation(
            Map<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            String columnFamilyName,
            RocksDBKeyedStateBackend.RocksDbKvStateInfo registeredColumn) {

        kvStateInformation.put(columnFamilyName, registeredColumn);
        if (nativeMetricMonitor != null) {
            nativeMetricMonitor.registerColumnFamily(
                    columnFamilyName, registeredColumn.columnFamilyHandle);
        }
    }

    /**
     * Creates a state info from a new meta info to use with a k/v state.
     *
     * <p>Creates the column family for the state. Sets TTL compaction filter if {@code
     * ttlCompactFiltersManager} is not {@code null}.
     *
     * @param importFilesMetaData if not empty, we import the files specified in the metadata to the
     *     column family.
     */
    public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
            RegisteredStateMetaInfoBase metaInfoBase,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity,
            List<ExportImportFilesMetaData> importFilesMetaData,
            ICloseableRegistry cancelStreamRegistryForRestore) {

        ColumnFamilyDescriptor columnFamilyDescriptor =
                createColumnFamilyDescriptor(
                        metaInfoBase,
                        columnFamilyOptionsFactory,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);

        try {
            ColumnFamilyHandle columnFamilyHandle =
                    createColumnFamily(
                            columnFamilyDescriptor,
                            db,
                            importFilesMetaData,
                            cancelStreamRegistryForRestore);
            return new RocksDBKeyedStateBackend.RocksDbKvStateInfo(
                    columnFamilyHandle, metaInfoBase);
        } catch (Exception ex) {
            IOUtils.closeQuietly(columnFamilyDescriptor.getOptions());
            throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", ex);
        }
    }

    /**
     * Create RocksDB-backed KV-state, including RocksDB ColumnFamily.
     *
     * @param cancelStreamRegistryForRestore {@link ICloseableRegistry#close closing} it interrupts
     *     KV state creation
     */
    public static RocksDBKeyedStateBackend.RocksDbKvStateInfo createStateInfo(
            RegisteredStateMetaInfoBase metaInfoBase,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity,
            ICloseableRegistry cancelStreamRegistryForRestore) {
        return createStateInfo(
                metaInfoBase,
                db,
                columnFamilyOptionsFactory,
                ttlCompactFiltersManager,
                writeBufferManagerCapacity,
                Collections.emptyList(),
                cancelStreamRegistryForRestore);
    }

    /**
     * Creates a column descriptor for a state column family.
     *
     * <p>Sets TTL compaction filter if {@code ttlCompactFiltersManager} is not {@code null}.
     */
    public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
            RegisteredStateMetaInfoBase metaInfoBase,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            @Nullable RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nullable Long writeBufferManagerCapacity) {

        byte[] nameBytes = metaInfoBase.getName().getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(
                !Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
                "The chosen state name 'default' collides with the name of the default column family!");

        ColumnFamilyOptions options =
                createColumnFamilyOptions(columnFamilyOptionsFactory, metaInfoBase.getName());

        if (ttlCompactFiltersManager != null) {
            ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(metaInfoBase, options);
        }

        if (writeBufferManagerCapacity != null) {
            // It'd be great to perform the check earlier, e.g. when creating write buffer manager.
            // Unfortunately the check needs write buffer size that was just calculated.
            sanityCheckArenaBlockSize(
                    options.writeBufferSize(),
                    options.arenaBlockSize(),
                    writeBufferManagerCapacity);
        }

        return new ColumnFamilyDescriptor(nameBytes, options);
    }

    /**
     * Logs a warning if the arena block size is too high causing RocksDB to flush constantly.
     * Essentially, the condition <a
     * href="https://github.com/dataArtisans/frocksdb/blob/49bc897d5d768026f1eb816d960c1f2383396ef4/include/rocksdb/write_buffer_manager.h#L47">
     * here</a> will always be true.
     *
     * @param writeBufferSize the size of write buffer (bytes)
     * @param arenaBlockSizeConfigured the manually configured arena block size, zero or less means
     *     not configured
     * @param writeBufferManagerCapacity the size of the write buffer manager (bytes)
     * @return true if sanity check passes, false otherwise
     */
    static boolean sanityCheckArenaBlockSize(
            long writeBufferSize, long arenaBlockSizeConfigured, long writeBufferManagerCapacity) {

        long defaultArenaBlockSize =
                RocksDBMemoryControllerUtils.calculateRocksDBDefaultArenaBlockSize(writeBufferSize);
        long arenaBlockSize =
                arenaBlockSizeConfigured <= 0 ? defaultArenaBlockSize : arenaBlockSizeConfigured;
        long mutableLimit =
                RocksDBMemoryControllerUtils.calculateRocksDBMutableLimit(
                        writeBufferManagerCapacity);
        if (RocksDBMemoryControllerUtils.validateArenaBlockSize(arenaBlockSize, mutableLimit)) {
            return true;
        } else {
            LOG.warn(
                    "RocksDBStateBackend performance will be poor because of the current Flink memory configuration! "
                            + "RocksDB will flush memtable constantly, causing high IO and CPU. "
                            + "Typically the easiest fix is to increase task manager managed memory size. "
                            + "If running locally, see the parameter taskmanager.memory.managed.size. "
                            + "Details: arenaBlockSize {} > mutableLimit {} (writeBufferSize = {}, arenaBlockSizeConfigured = {},"
                            + " defaultArenaBlockSize = {}, writeBufferManagerCapacity = {})",
                    arenaBlockSize,
                    mutableLimit,
                    writeBufferSize,
                    arenaBlockSizeConfigured,
                    defaultArenaBlockSize,
                    writeBufferManagerCapacity);
            return false;
        }
    }

    public static ColumnFamilyOptions createColumnFamilyOptions(
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

        // ensure that we use the right merge operator, because other code relies on this
        return columnFamilyOptionsFactory
                .apply(stateName)
                .setMergeOperatorName(MERGE_OPERATOR_NAME);
    }

    private static ColumnFamilyHandle createColumnFamily(
            ColumnFamilyDescriptor columnDescriptor,
            RocksDB db,
            List<ExportImportFilesMetaData> importFilesMetaData,
            ICloseableRegistry cancelStreamRegistryForRestore)
            throws RocksDBException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            // abort recovery if the task thread was already interrupted
            // e.g. because the task was cancelled
            throw new InterruptedException("The thread was interrupted, aborting recovery");
        } else if (cancelStreamRegistryForRestore.isClosed()) {
            throw new CancelTaskException("The stream was closed, aborting recovery");
        }

        if (TOPLINGDB_REPO != null) {
            String path = db.getName();
            String cfName = new String(columnDescriptor.getName());
            String cfoName = cfoNameOf(path, cfName);
            ObjectMapper omapper = new ObjectMapper();
            ObjectNode root = omapper.createObjectNode();
            root.putObject("CFOptions").putObject(cfoName).put("update_from", "default");
            String strJson;
            try {
                strJson = omapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize toplingdb json config", e);
            }
            synchronized (TOPLINGDB_REPO) {
                if (TOPLINGDB_DEBUG_LEVEL >= 1) {
                    System.err.printf("toplingdb createCF: %s%n", strJson);
                }
                TOPLINGDB_REPO.put(cfoName, columnDescriptor.getOptions());
                TOPLINGDB_REPO.importJson(strJson); // update cfoName
                if (importFilesMetaData.isEmpty()) {
                    return TOPLINGDB_REPO.createCF(db, cfName, cfoName);
                } else {
                    try (ImportColumnFamilyOptions importColumnFamilyOptions =
                            new ImportColumnFamilyOptions().setMoveFiles(true)) {
                        return TOPLINGDB_REPO.createCFWithImport(
                                db,
                                cfName,
                                cfoName,
                                importColumnFamilyOptions,
                                importFilesMetaData);
                    }
                }
            }
        }

        if (importFilesMetaData.isEmpty()) {
            return db.createColumnFamily(columnDescriptor);
        } else {
            try (ImportColumnFamilyOptions importColumnFamilyOptions =
                    new ImportColumnFamilyOptions().setMoveFiles(true)) {
                return db.createColumnFamilyWithImport(
                        columnDescriptor, importColumnFamilyOptions, importFilesMetaData);
            }
        }
    }

    public static void addColumnFamilyOptionsToCloseLater(
            List<ColumnFamilyOptions> columnFamilyOptions, ColumnFamilyHandle columnFamilyHandle) {
        try {
            // IMPORTANT NOTE: Do not call ColumnFamilyHandle#getDescriptor() just to judge if it
            // return null and then call it again when it return is not null. That will cause
            // task manager native memory used by RocksDB can't be released timely after job
            // restart.
            // The problem can find in : https://issues.apache.org/jira/browse/FLINK-21986
            if (columnFamilyHandle != null) {
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyHandle.getDescriptor();
                if (columnFamilyDescriptor != null) {
                    columnFamilyOptions.add(columnFamilyDescriptor.getOptions());
                }
            }
        } catch (RocksDBException e) {
            // ignore
        }
    }

    @Nullable
    public static OpaqueMemoryResource<RocksDBSharedResources> allocateSharedCachesIfConfigured(
            RocksDBMemoryConfiguration jobMemoryConfig,
            Environment env,
            double memoryFraction,
            Logger logger,
            RocksDBMemoryControllerUtils.RocksDBMemoryFactory rocksDBMemoryFactory)
            throws IOException {

        try {
            RocksDBSharedResourcesFactory factory =
                    RocksDBSharedResourcesFactory.from(jobMemoryConfig, env);
            if (factory == null) {
                return null;
            }

            return factory.create(
                    jobMemoryConfig, env, memoryFraction, logger, rocksDBMemoryFactory);

        } catch (Exception e) {
            throw new IOException("Failed to acquire shared cache resource for RocksDB", e);
        }
    }

    private static void throwExceptionIfPathLengthExceededOnWindows(String path, Exception cause)
            throws IOException {
        // max directory path length on Windows is 247.
        // the maximum path length is 260, subtracting one file name length (12 chars) and one NULL
        // terminator.
        final int maxWinDirPathLen = 247;

        if (path.length() > maxWinDirPathLen && OperatingSystem.isWindows()) {
            throw new IOException(
                    String.format(
                            "The directory path length (%d) is longer than the directory path length limit for Windows (%d): %s",
                            path.length(), maxWinDirPathLen, path),
                    cause);
        }
    }
}
