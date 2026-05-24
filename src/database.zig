const std = @import("std");
const rdb = @import("rocksdb");
const lib = @import("lib.zig");

const Allocator = std.mem.Allocator;
const RwLock = std.Io.RwLock;

const Data = lib.Data;
const Iterator = lib.Iterator;
const IteratorDirection = lib.IteratorDirection;
const RawIterator = lib.RawIterator;
const WriteBatch = lib.WriteBatch;

const copy = lib.data.copy;
const copyLen = lib.data.copyLen;


pub const DB = struct {
    db: *rdb.rocksdb_t,
    default_cf: ?ColumnFamilyHandle = null,
    cf_name_to_handle: *CfNameToHandleMap,

    const Self = @This();

    pub fn open(
        allocator: Allocator,
        dir: []const u8,
        db_options: DBOptions,
        maybe_column_families: ?[]const ColumnFamilyDescription,
        for_read_only: bool,
        err_str: *?Data,
    ) (Allocator.Error || error{RocksDBOpen})!struct { Self, []const ColumnFamily } {
        const column_families = if (maybe_column_families) |cfs|
            cfs
        else
            &[1]ColumnFamilyDescription{.{ .name = "default" }};

        const cf_handles = try allocator.alloc(?ColumnFamilyHandle, column_families.len);
        defer allocator.free(cf_handles);

        // open database
        const db = db: {
            const cf_options = try allocator.alloc(?*const rdb.rocksdb_options_t, column_families.len);
            defer allocator.free(cf_options);
            const cf_names = try allocator.alloc([*c]const u8, column_families.len);
            defer allocator.free(cf_names);
            const db_opts = db_options.convert();
            defer rdb.rocksdb_options_destroy(db_opts);
            
            for (column_families, 0..) |cf, i| {
                cf_names[i] = @ptrCast(cf.name.ptr);
                cf_options[i] = cf.options.convert();
            }
            var ch = CallHandler.init(err_str);

            const ret = if (for_read_only)
                rdb.rocksdb_open_for_read_only_column_families(
                    db_opts,
                    dir.ptr,
                    @intCast(cf_names.len),
                    @ptrCast(cf_names.ptr),
                    @ptrCast(cf_options.ptr),
                    @ptrCast(cf_handles.ptr),
                    0,
                    @ptrCast(&ch.err_str_in),
                )
            else
                rdb.rocksdb_open_column_families(
                    db_opts,
                    dir.ptr,
                    @intCast(cf_names.len),
                    @ptrCast(cf_names.ptr),
                    @ptrCast(cf_options.ptr),
                    @ptrCast(cf_handles.ptr),
                    @ptrCast(&ch.err_str_in),
                );

            break :db try ch.handle(ret, error.RocksDBOpen);
        };

        // organize column family metadata
        const cf_list = try allocator.alloc(ColumnFamily, column_families.len);
        errdefer allocator.free(cf_list);
        const cf_map = try CfNameToHandleMap.create(allocator);
        errdefer cf_map.destroy();
        for (cf_list, 0..) |*cf, i| {
            const name = try allocator.dupe(u8, column_families[i].name);
            errdefer allocator.free(name);
            cf.* = .{
                .name = name,
                .handle = cf_handles[i].?,
            };
            try cf_map.map.put(allocator, name, cf_handles[i].?);
        }

        return .{
            Self{ .db = db.?, .cf_name_to_handle = cf_map },
            cf_list,
        };
    }

    pub fn withDefaultColumnFamily(self: Self, column_family: ColumnFamilyHandle) Self {
        return .{
            .db = self.db,
            .cf_name_to_handle = self.cf_name_to_handle,
            .default_cf = column_family,
        };
    }

    /// Closes the database and cleans up this struct's state.
    pub fn deinit(self: Self) void {
        self.cf_name_to_handle.destroy();
        rdb.rocksdb_close(self.db);
    }

    /// Delete the entire database from the filesystem.
    /// Destroying a database after it is closed has undefined behavior.
    pub fn destroy(self: Self) error{Closed}!void {
        rdb.rocksdb_destroy_db(self.db);
    }

    pub fn createColumnFamily(
        self: *Self,
        name: []const u8,
        err_str: *?Data,
    ) !ColumnFamilyHandle {
        const options = rdb.rocksdb_options_create();
        defer rdb.rocksdb_options_destroy(options);
        var ch = CallHandler.init(err_str);
        const handle = (try ch.handle(rdb.rocksdb_create_column_family(
            self.db,
            options,
            @ptrCast(name),
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBCreateColumnFamily)).?;
        self.cf_name_to_handle.put(name, handle);
        return handle;
    }

    pub fn columnFamily(
        self: *const Self,
        cf_name: []const u8,
    ) error{UnknownColumnFamily}!ColumnFamilyHandle {
        return self.cf_name_to_handle.get(cf_name) orelse error.UnknownColumnFamily;
    }

    pub fn put(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        key: []const u8,
        value: []const u8,
        err_str: *?Data,
    ) error{RocksDBPut}!void {
        const options = rdb.rocksdb_writeoptions_create();
        defer rdb.rocksdb_writeoptions_destroy(options);
        var ch = CallHandler.init(err_str);
        try ch.handle(rdb.rocksdb_put_cf(
            self.db,
            options,
            column_family orelse self.default_cf,
            key.ptr,
            key.len,
            value.ptr,
            value.len,
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBPut);
    }

    pub fn get(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        key: []const u8,
        err_str: *?Data,
    ) error{RocksDBGet}!?Data {
        var valueLength: usize = 0;
        const options = rdb.rocksdb_readoptions_create();
        defer rdb.rocksdb_readoptions_destroy(options);
        var ch = CallHandler.init(err_str);
        const value = try ch.handle(rdb.rocksdb_get_cf(
            self.db,
            options,
            column_family orelse self.default_cf,
            key.ptr,
            key.len,
            &valueLength,
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBGet);
        if (value == 0) {
            return null;
        }
        return .{
            .free = rdb.rocksdb_free,
            .data = value[0..valueLength],
        };
    }

    pub fn delete(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        key: []const u8,
        err_str: *?Data,
    ) error{RocksDBDelete}!void {
        const options = rdb.rocksdb_writeoptions_create();
        defer rdb.rocksdb_writeoptions_destroy(options);
        var ch = CallHandler.init(err_str);
        try ch.handle(rdb.rocksdb_delete_cf(
            self.db,
            options,
            column_family orelse self.default_cf,
            key.ptr,
            key.len,
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBDelete);
    }

    pub fn deleteFilesInRange(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        start_key: []const u8,
        limit_key: []const u8,
        err_str: *?Data,
    ) error{RocksDBDeleteFilesInRange}!void {
        var ch = CallHandler.init(err_str);
        try ch.handle(rdb.rocksdb_delete_file_in_range_cf(
            self.db,
            column_family orelse self.default_cf,
            @ptrCast(start_key.ptr),
            start_key.len,
            @ptrCast(limit_key.ptr),
            limit_key.len,
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBDeleteFilesInRange);
    }

    pub fn iterator(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        direction: IteratorDirection,
        start: ?[]const u8,
    ) Iterator {
        const it = self.rawIterator(column_family);
        if (start) |seek_target| switch (direction) {
            .forward => it.seek(seek_target),
            .reverse => it.seekForPrev(seek_target),
        } else switch (direction) {
            .forward => it.seekToFirst(),
            .reverse => it.seekToLast(),
        }
        return .{
            .raw = it,
            .direction = direction,
            .done = false,
        };
    }

    pub fn rawIterator(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
    ) RawIterator {
        const options = rdb.rocksdb_readoptions_create();
        defer rdb.rocksdb_readoptions_destroy(options); // TODO does this need to outlive the iterator?
        const inner_iter = rdb.rocksdb_create_iterator_cf(
            self.db,
            options,
            column_family orelse self.default_cf,
        ).?;
        const ri = RawIterator{ .inner = inner_iter };
        return ri;
    }

    pub fn liveFiles(self: *const Self, allocator: Allocator) Allocator.Error![]const LiveFile {
        const files = rdb.rocksdb_livefiles(self.db).?;
        defer rdb.rocksdb_livefiles_destroy(files);
        const num_files: usize = @intCast(rdb.rocksdb_livefiles_count(files));

        var livefiles: std.ArrayList(LiveFile) = .empty;
        defer livefiles.deinit(allocator);

        var key_size: usize = 0;
        for (0..num_files) |i| {
            const file_num: c_int = @intCast(i);
            try livefiles.append(allocator, .{
                .allocator = allocator,
                .column_family_name = try copy(allocator, rdb.rocksdb_livefiles_column_family_name(files, file_num)),
                .name = try copy(allocator, rdb.rocksdb_livefiles_name(files, file_num)),
                .size = rdb.rocksdb_livefiles_size(files, file_num),
                .level = rdb.rocksdb_livefiles_level(files, file_num),
                .start_key = try copyLen(allocator, rdb.rocksdb_livefiles_smallestkey(files, file_num, &key_size), key_size),
                .end_key = try copyLen(allocator, rdb.rocksdb_livefiles_largestkey(files, file_num, &key_size), key_size),
                .num_entries = rdb.rocksdb_livefiles_entries(files, file_num),
                .num_deletions = rdb.rocksdb_livefiles_deletions(files, file_num),
            });
        }

        return try livefiles.toOwnedSlice(allocator);
    }

    pub fn propertyValueCf(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        propname: []const u8,
    ) Data {
        const value = rdb.rocksdb_property_value_cf(
            self.db,
            column_family orelse self.default_cf,
            @ptrCast(propname.ptr),
        );
        return .{
            .data = std.mem.span(value),
            .free = rdb.rocksdb_free,
        };
    }

    pub fn write(
        self: *const Self,
        batch: WriteBatch,
        err_str: *?Data,
    ) error{RocksDBWrite}!void {
        const options = rdb.rocksdb_writeoptions_create();
        defer rdb.rocksdb_writeoptions_destroy(options);
        var ch = CallHandler.init(err_str);
        try ch.handle(rdb.rocksdb_write(
            self.db,
            options,
            batch.inner,
            @ptrCast(&ch.err_str_in),
        ), error.RocksDBWrite);
    }

    pub fn flush(
        self: *const Self,
        column_family: ?ColumnFamilyHandle,
        err_str: *?Data,
    ) error{RocksDBFlush}!void {
        const options = rdb.rocksdb_flushoptions_create();
        defer rdb.rocksdb_flushoptions_destroy(options);
        var ch = CallHandler.init(err_str);
        const e = error.RocksDBFlush;
        if (column_family) |cf|
            try ch.handle(rdb.rocksdb_flush_cf(self.db, options, cf, @ptrCast(&ch.err_str_in)), e)
        else
            try ch.handle(rdb.rocksdb_flush(self.db, options, @ptrCast(&ch.err_str_in)), e);
    }
};

pub const DBOptions = struct {
    // ------------------------------------------------------------------
    // Basic / setup
    // ------------------------------------------------------------------

    /// If true, the database will be created if it is missing.
    /// Default: false
    create_if_missing: bool = false,

    /// If true, missing column families will be automatically created on open.
    /// Default: false
    create_missing_column_families: bool = false,

    /// If true, an error is raised if the database already exists.
    /// Default: false
    error_if_exists: bool = false,

    /// If true, the implementation will do aggressive checking of the data
    /// it is processing and will stop early if it detects any errors.
    /// Default: true
    paranoid_checks: bool = true,

    // ------------------------------------------------------------------
    // Files & I/O
    // ------------------------------------------------------------------

    /// Number of open files that can be used by the DB. -1 = always keep open.
    /// Default: -1
    max_open_files: i32 = -1,

    /// If non-zero, we will limit total bytes of writes per second to this
    /// value. Default: 0 (disabled)
    bytes_per_sync: u64 = 0,

    /// Same as bytes_per_sync, but for WAL writes.
    /// Default: 0 (use bytes_per_sync)
    wal_bytes_per_sync: u64 = 0,

    /// Maximum total data size for a level. Used to trigger compaction.
    /// Default: 0 (disabled, rocksdb picks based on write_buffer_size)
    db_write_buffer_size: usize = 0,


    /// If non-zero, the DB will write at most this many bytes before slowing
    /// down writes. Useful to avoid compaction IO spikes.
    /// Default: 0
    max_total_wal_size: u64 = 0,

    /// Number of bytes to preallocate (via fallocate) the manifest files.
    /// Default: 4 MiB
    manifest_preallocation_size: usize = 4 * 1024 * 1024,

    /// if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
    /// Default: 600
    stats_dump_period_sec: c_uint = 600,

    /// if not zero, dump rocksdb.stats to LOG every stats_persist_period_sec
    /// Default: 600
    stats_persist_period_sec: c_uint = 600,

    // ------------------------------------------------------------------
    // WAL
    // ------------------------------------------------------------------



    /// Recovery mode on open after an unclean shutdown.
    /// Default: .point_in_time
    wal_recovery_mode: WalRecoveryMode = .point_in_time,

    // ------------------------------------------------------------------
    // Parallelism / threading
    // ------------------------------------------------------------------

    /// Number of background jobs (compaction + flush) the DB can run concurrently.
    /// Prefer calling `rocksdb_options_increase_parallelism` in production
    /// instead of setting these directly; this just sets both to the same value.
    /// Default: 1 (each)
    max_background_jobs: c_int = 2,

    /// Maximum number of concurrent background compaction jobs.
    /// -1 means use max_background_jobs.
    /// Default: -1
    max_background_compactions: c_int = -1,

    /// Maximum number of concurrent background flush jobs.
    /// -1 means use max_background_jobs.
    /// Default: -1
    max_background_flushes: c_int = -1,

    // ------------------------------------------------------------------
    // Logging
    // ------------------------------------------------------------------

    /// Log level for info logs.
    /// Default: .info
    info_log_level: InfoLogLevel = .info,

    /// Maximum log file size. 0 = no limit.
    /// Default: 0
    max_log_file_size: usize = 0,

    /// Time for the info log file to roll (in seconds). 0 = disabled.
    /// Default: 0
    log_file_time_to_roll: usize = 0,

    /// Maximum number of info log files to keep.
    /// Default: 1000
    keep_log_file_num: usize = 1000,

    /// Recycle log files. If non-zero, instead of deleting old log files,
    /// keep this many for future use by overwriting them.
    /// Default: 0
    recycle_log_file_num: usize = 0,

    // ------------------------------------------------------------------
    // Compression (DB-wide default, overridden per CF)
    // ------------------------------------------------------------------
    compression: CompressionType = .no_compression,

    // ------------------------------------------------------------------
    // Misc
    // ------------------------------------------------------------------

    /// If true, then the contents of manifest and data files are not
    /// synced to stable storage. Can be useful if you are debugging or
    /// testing and want faster open/close cycles.
    /// Default: false
    skip_stats_update_on_db_open: bool = false,

    /// Compress rotated WAL files.
    /// Default: false
    allow_mmap_reads: bool = false,

    /// Allow the OS to mmap file for writes.
    /// Default: false
    allow_mmap_writes: bool = false,

    /// Use direct I/O for reads (bypasses OS page cache).
    /// Default: false
    use_direct_reads: bool = false,

    /// Use direct I/O for flush and compaction writes.
    /// Default: false
    use_direct_io_for_flush_and_compaction: bool = false,

    /// If true, allow ingestion of data that have been created by a newer
    /// version of the DB. Used in some migration scenarios.
    /// Default: false
    skip_checking_sst_file_sizes_on_db_open: bool = false,

    /// If true, threads synchronizing with the write batch group leader will
    /// wait for up to write_thread_max_yield_usec before blocking on a mutex.
    /// Default: true
    enable_write_thread_adaptive_yield: bool = true,


    /// If true, allow concurrent memtable writes. Enabling this can
    /// improve write throughput for workloads with many parallel writers.
    /// Default: true
    allow_concurrent_memtable_write: bool = true,

    /// Recovery mode to use when WAL is corrupt.
    /// Default: false (fail on corruption)
    avoid_unnecessary_blocking_io: bool = false,

    // ------------------------------------------------------------------
    // convert()
    // ------------------------------------------------------------------
    fn convert(do: DBOptions) *rdb.struct_rocksdb_options_t {
        const ro = rdb.rocksdb_options_create().?;

        // basic / setup
        rdb.rocksdb_options_set_create_if_missing(ro, @intFromBool(do.create_if_missing));
        rdb.rocksdb_options_set_create_missing_column_families(ro, @intFromBool(do.create_missing_column_families));
        rdb.rocksdb_options_set_error_if_exists(ro, @intFromBool(do.error_if_exists));
        rdb.rocksdb_options_set_paranoid_checks(ro, @intFromBool(do.paranoid_checks));

        // files & I/O
        rdb.rocksdb_options_set_max_open_files(ro, do.max_open_files);
        rdb.rocksdb_options_set_bytes_per_sync(ro, do.bytes_per_sync);
        rdb.rocksdb_options_set_wal_bytes_per_sync(ro, do.wal_bytes_per_sync);
        rdb.rocksdb_options_set_db_write_buffer_size(ro, do.db_write_buffer_size);
        rdb.rocksdb_options_set_max_total_wal_size(ro, do.max_total_wal_size);
        rdb.rocksdb_options_set_manifest_preallocation_size(ro, do.manifest_preallocation_size);
        rdb.rocksdb_options_set_stats_dump_period_sec(ro, do.stats_dump_period_sec);
        rdb.rocksdb_options_set_stats_persist_period_sec(ro, do.stats_persist_period_sec);

        // WAL
        rdb.rocksdb_options_set_wal_recovery_mode(ro, @intFromEnum(do.wal_recovery_mode));

        // parallelism
        rdb.rocksdb_options_set_max_background_jobs(ro, do.max_background_jobs);
        rdb.rocksdb_options_set_max_background_compactions(ro, do.max_background_compactions);
        rdb.rocksdb_options_set_max_background_flushes(ro, do.max_background_flushes);

        // logging
        rdb.rocksdb_options_set_info_log_level(ro, @intFromEnum(do.info_log_level));
        rdb.rocksdb_options_set_max_log_file_size(ro, do.max_log_file_size);
        rdb.rocksdb_options_set_log_file_time_to_roll(ro, do.log_file_time_to_roll);
        rdb.rocksdb_options_set_keep_log_file_num(ro, do.keep_log_file_num);
        rdb.rocksdb_options_set_recycle_log_file_num(ro, do.recycle_log_file_num);

        // compression
        rdb.rocksdb_options_set_compression(ro, @intFromEnum(do.compression));

        // misc
        rdb.rocksdb_options_set_skip_stats_update_on_db_open(ro, @intFromBool(do.skip_stats_update_on_db_open));
        rdb.rocksdb_options_set_allow_mmap_reads(ro, @intFromBool(do.allow_mmap_reads));
        rdb.rocksdb_options_set_allow_mmap_writes(ro, @intFromBool(do.allow_mmap_writes));
        rdb.rocksdb_options_set_use_direct_reads(ro, @intFromBool(do.use_direct_reads));
        rdb.rocksdb_options_set_use_direct_io_for_flush_and_compaction(ro, @intFromBool(do.use_direct_io_for_flush_and_compaction));
        rdb.rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(ro, @intFromBool(do.skip_checking_sst_file_sizes_on_db_open));
        rdb.rocksdb_options_set_enable_write_thread_adaptive_yield(ro, @intFromBool(do.enable_write_thread_adaptive_yield));
        rdb.rocksdb_options_set_allow_concurrent_memtable_write(ro, @intFromBool(do.allow_concurrent_memtable_write));
        rdb.rocksdb_options_set_avoid_unnecessary_blocking_io(ro, @intFromBool(do.avoid_unnecessary_blocking_io));

        return ro;
    }
};

pub const WalRecoveryMode = enum(c_int) {
    /// Original leveldb recovery mode. May lose data on WAL corruption.
    tolerate_corrupted_tail_records = 0,
    /// Recover only up to the point of corruption, then stop.
    absolute_consistency = 1,
    /// Recover as much as possible without losing data.
    point_in_time = 2,
    /// Skip any corrupted records. Dangerous: may silently drop writes.
    skip_any_corrupted_records = 3,
};

pub const InfoLogLevel = enum(c_int) {
    debug = 0,
    info = 1,
    warn = 2,
    err = 3,
    fatal = 4,
    header = 5,
};

test "DB clean init and deinit" {
    const ns = struct {
        pub fn run(allocator: Allocator, io: std.Io) !void {
            var dir = std.testing.tmpDir(.{});
            defer dir.cleanup();
            const path = try dir.dir.realPathFileAlloc(io, ".", allocator);
            defer allocator.free(path);

            var data: ?Data = null;
            const db, const cfs = try DB.open(
                allocator,
                path,
                .{
                    .create_if_missing = true,
                    .create_missing_column_families = true,
                },
                null,
                false,
                &data,
            );

            db.deinit();
            allocator.free(cfs);
        }
    };

    try ns.run(std.testing.allocator, std.testing.io);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, ns.run, .{std.testing.io});
}

test "DBOptions defaults" {
    try testDBOptions(DBOptions{}, rdb.rocksdb_options_create().?);
}

test "DBOptions custom" {
    const subject = DBOptions{
        .create_if_missing = true,
        .create_missing_column_families = true,
        .max_open_files = 1234,
    };

    const expected = rdb.rocksdb_options_create().?;
    rdb.rocksdb_options_set_create_if_missing(expected, 1);
    rdb.rocksdb_options_set_create_missing_column_families(expected, 1);
    rdb.rocksdb_options_set_max_open_files(expected, 1234);

    try testDBOptions(subject, expected);
}

fn testDBOptions(test_subject: DBOptions, expected: *rdb.struct_rocksdb_options_t) !void {
    const actual = test_subject.convert();

    inline for (@typeInfo(DBOptions).@"struct".fields) |field| {
        // Skip checking compression since the C API doesn't have a direct rocksdb_options_get_compression accessor
        if (comptime std.mem.eql(u8, field.name, "compression")) continue;
        
        const getter = "rocksdb_options_get_" ++ field.name;
        const expected_value = @call(.auto, @field(rdb, getter), .{expected});
        const actual_value = @call(.auto, @field(rdb, getter), .{actual});
        try std.testing.expectEqual(expected_value, actual_value);
    }
}

pub const ColumnFamilyDescription = struct {
    name: []const u8,
    options: ColumnFamilyOptions = .{},
};

pub const ColumnFamily = struct {
    name: []const u8,
    handle: ColumnFamilyHandle,
};

pub const ColumnFamilyHandle = *rdb.rocksdb_column_family_handle_t;

// ============================================================
// Enums
// ============================================================

pub const CompressionType = enum(c_int) {
    no_compression = 0x0,
    snappy = 0x1,
    zlib = 0x2,
    bz2 = 0x3,
    lz4 = 0x4,
    lz4hc = 0x5,
    xpress = 0x6,
    zstd = 0x7,
    zstd_not_final = 0x40,
    /// Sentinel: "do not override; inherit from the column-family-level setting."
    /// Only meaningful for `bottommost_compression`. Never use in
    /// `compression` or `compression_per_level`.
    disable_compression_option = 0xff,
};

pub const CompactionStyle = enum(c_int) {
    /// Classic level-based compaction (default). Good general-purpose choice;
    /// controls space amplification well.
    level = 0,
    /// Universal (size-tiered) compaction. Better write amplification at the
    /// cost of higher space amplification. Suited for write-heavy workloads.
    universal = 1,
    /// FIFO compaction. Files are dropped in creation order once the total
    /// size exceeds `fifo.max_table_files_size`. Intended for time-series /
    /// cache-like data where old data can simply be evicted.
    fifo = 2,
    /// No automatic compaction. The caller is responsible for triggering
    /// compactions manually via `CompactRange`.
    none = 3,
};

pub const CompactionPriority = enum(c_int) {
    /// Pick the file with the largest compensated size (default).
    by_compensated_size = 0,
    /// Among files with the same compensated size, prefer those whose
    /// largest sequence number is oldest — reduces key range overlap.
    oldest_largest_seq_first = 1,
    /// Prefer the file with the smallest sequence number overall. Keeps
    /// the key range coverage minimal.
    oldest_smallest_seq_first = 2,
    /// Minimise the ratio of overlapping bytes between the file being
    /// compacted and the next level. Reduces write amplification.
    min_overlapping_ratio = 3,
    /// Pick files in round-robin order. Spreads I/O evenly across the level.
    round_robin = 4,
};

pub const CompactionAccessPattern = enum(c_int) {
    /// No advisory hint to the OS about access patterns.
    none = 0,
    /// Normal (random) access pattern (default).
    normal = 1,
    /// Tell the OS to read-ahead sequentially; useful when compaction reads
    /// large files start-to-finish.
    sequential = 2,
    /// Hint WILLNEED to the OS so it pages in the file proactively.
    willneed = 3,
};

pub const EncodingType = enum(c_int) {
    /// Store keys without any prefix-based encoding (default).
    plain = 0,
    /// Encode keys using a shared prefix per restart interval, reducing
    /// storage for datasets with long common key prefixes. Requires a
    /// prefix extractor to be configured.
    prefix = 1,
};

pub const ChecksumType = enum(u8) {
    /// No checksum. Fastest, but no corruption detection.
    no_checksum = 0,
    /// CRC32c hardware-accelerated checksum (default). Good balance of
    /// speed and reliability.
    crc32c = 1,
    /// xxHash (32-bit). Slightly faster than CRC32c on some platforms.
    xxhash = 2,
    /// xxHash (64-bit). Better collision resistance than xxhash.
    xxhash64 = 3,
    /// XXH3. Fastest of the xx family; requires RocksDB 6.15+.
    xxh3 = 4,
};

pub const PrepopulateBlockCache = enum(c_int) {
    /// Do not pre-populate the block cache (default).
    disable = 0,
    /// Pre-populate the block cache with data blocks written during flush.
    /// Useful when the working set fits in cache and read-after-write latency
    /// matters.
    flush_only = 1,
};

pub const UniversalCompactionStopStyle = enum(c_int) {
    /// Stop merging when the total size of files being merged is within
    /// `size_ratio` percent of the next file's size.
    similar_size = 0,
    /// Stop merging when the total size of all files being merged exceeds
    /// `max_size_amplification_percent` of the total DB size (default).
    total_size = 1,
};

// ============================================================
// Sub-option structs
// ============================================================

pub const FifoCompactionOptions = struct {
    /// Files older than this many seconds will be deleted regardless of
    /// `max_table_files_size`. 0 = disabled (default).
    ttl: u64 = 0,

    /// If true, RocksDB may run normal compactions on the FIFO set to
    /// de-duplicate or drop tombstones before eviction, at the cost of
    /// more I/O. Default: false.
    allow_compaction: bool = false,

    /// Once the total size of all SST files in the CF exceeds this threshold,
    /// the oldest file is deleted. Default: 1 GiB.
    max_table_files_size: u64 = 1 * 1024 * 1024 * 1024,
};

pub const UniversalCompactionOptions = struct {
    /// Percentage flexibility when comparing file sizes. A candidate file
    /// at level N is eligible to be compacted with level N+1 only if
    /// its size is within `size_ratio`% of the next level's size.
    /// Default: 1.
    size_ratio: c_int = 1,

    /// Minimum number of files in a single compaction run.
    /// Default: 2.
    min_merge_width: c_int = 2,

    /// Maximum number of files in a single compaction run.
    /// Default: INT_MAX (no limit).
    max_merge_width: c_int = std.math.maxInt(c_int),

    /// The algorithm stops looking for a compaction once the space
    /// amplification ratio exceeds this percentage. For example, 200 means
    /// the DB is allowed to be 2× the size of the raw data before a full
    /// compaction is forced. Default: 200.
    max_size_amplification_percent: c_int = 200,

    /// If -1 (default), all output files follow the global `compression`
    /// setting. If a non-negative value N is given, files produced by
    /// compactions that reduce the total sorted runs to fewer than N will
    /// be compressed with the global `compression` type.
    compression_size_percent: c_int = -1,

    /// Controls when to stop accumulating files into a compaction candidate
    /// set. Default: .total_size.
    stop_style: UniversalCompactionStopStyle = .total_size,

};

// ============================================================
// Main struct
// ============================================================

pub const ColumnFamilyOptions = struct {
    // ------------------------------------------------------------------
    // Compression
    // ------------------------------------------------------------------

    /// Compression algorithm applied to all SST data blocks that don't have
    /// a more specific override (see `bottommost_compression` and
    /// `compression_per_level`). Default: .no_compression.
    compression: CompressionType = .no_compression,

    /// Compression applied specifically to the bottommost level, which
    /// typically holds the bulk of data. Set to a slower but higher-ratio
    /// algorithm (e.g. .zstd) to save space without impacting read-path
    /// performance on hot data. `.disable_compression_option` (default) means
    /// "inherit from `compression`".
    bottommost_compression: CompressionType = .disable_compression_option,

    /// Per-level compression overrides. When non-null, the slice must have
    /// exactly `num_levels` entries. Entry 0 corresponds to level 0.
    /// Never use `.disable_compression_option` in this slice; every entry
    /// must be an explicit algorithm. Null (default) uses `compression` for
    /// all levels.
    compression_per_level: ?[]const CompressionType = null,

    // ------------------------------------------------------------------
    // Write / MemTable
    // ------------------------------------------------------------------

    /// Amount of data to build up in memory before flushing to an SST file.
    /// Larger values reduce write amplification but increase memory usage and
    /// recovery time after a crash. Default: 64 MiB.
    write_buffer_size: usize = 64 * 1024 * 1024,

    /// Maximum number of write buffers (memtables) that can exist in memory
    /// at once, including the one currently being written to. A higher value
    /// allows more writes to proceed while earlier buffers are flushed.
    /// Default: 2.
    max_write_buffer_number: c_int = 2,

    /// Minimum number of write buffers that will be merged together before
    /// writing to storage. Increasing this reduces write amplification for
    /// small-value workloads. Default: 1.
    min_write_buffer_number_to_merge: c_int = 1,

    /// The total maximum size of write buffers to maintain in memory even
    /// after they have been flushed. This controls how much data is kept
    /// in memory for transaction conflict detection. 0 = use
    /// `write_buffer_size`. Default: 0.
    max_write_buffer_size_to_maintain: i64 = 0,

    /// Size of the memtable arena block. 0 lets RocksDB choose (usually 1/8
    /// of `write_buffer_size`). Increase if you see excessive small-block
    /// allocations. Default: 0.
    arena_block_size: usize = 0,

    /// If > 0, create a Bloom filter on the memtable keyed on the prefix
    /// extractor output. The value is the fraction of `write_buffer_size`
    /// to reserve for the filter (e.g. 0.1 = 10%). Reduces point-lookup I/O
    /// for prefix-keyed workloads. Default: 0 (disabled).
    memtable_prefix_bloom_size_ratio: f64 = 0,

    /// If true, the memtable Bloom filter covers the full key in addition
    /// to the prefix, giving exact-match short-circuit on point lookups.
    /// Requires `memtable_prefix_bloom_size_ratio` > 0. Default: false.
    memtable_whole_key_filtering: bool = false,

    /// If > 0, try to use huge TLB pages for the memtable arena. The value
    /// is the size of one huge page (typically 2 MiB). 0 = normal pages
    /// (default). Requires the OS to have huge pages available.
    memtable_huge_page_size: usize = 0,

    /// If true, allow RocksDB to update values in-place inside the memtable
    /// for a key that already exists there, avoiding the cost of writing a
    /// second copy. Only safe when the new value is always the same size as
    /// the existing one. Default: false.
    inplace_update_support: bool = false,

    /// Number of locks used to guard in-place update slots. Higher values
    /// reduce lock contention at the cost of memory. Only relevant when
    /// `inplace_update_support` is true. Default: 10 000.
    inplace_update_num_locks: usize = 10_000,

    // ------------------------------------------------------------------
    // Levels & SST layout
    // ------------------------------------------------------------------

    /// Total number of levels in the LSM tree. Level 0 is the landing zone
    /// for freshly flushed memtables; level `num_levels - 1` is the bottommost.
    /// Increasing this spreads data more finely but adds compaction overhead.
    /// Default: 7.
    num_levels: c_int = 7,

    /// Number of SST files in level 0 that triggers a compaction into level 1.
    /// Lower values keep read amplification low; higher values batch more
    /// writes together. Default: 4.
    level0_file_num_compaction_trigger: c_int = 4,

    /// Number of SST files in level 0 at which writes are slowed down to give
    /// compaction time to catch up. Default: 20.
    level0_slowdown_writes_trigger: c_int = 20,

    /// Number of SST files in level 0 at which all writes are stopped until
    /// compaction reduces the count below this threshold. Default: 36.
    level0_stop_writes_trigger: c_int = 36,

    /// Maximum total data size for level 1. When level 1 exceeds this,
    /// compaction into level 2 is triggered. Each subsequent level is
    /// `max_bytes_for_level_multiplier` times larger. Default: 256 MiB.
    max_bytes_for_level_base: u64 = 256 * 1024 * 1024,

    /// The growth factor between successive levels. Level N can hold
    /// `max_bytes_for_level_base * multiplier^(N-1)` bytes. Default: 10.
    max_bytes_for_level_multiplier: f64 = 10,

    /// Per-level overrides for the multiplier. When non-null, the slice must
    /// have `num_levels` entries. Entry 0 is ignored (level 0 is size-triggered
    /// by file count, not bytes). Null (default) uses a uniform multiplier.
    max_bytes_for_level_multiplier_additional: ?[]const c_int = null,

    /// Target size of each individual SST file produced by compaction at
    /// level 1. Files at level N are at most
    /// `target_file_size_base * target_file_size_multiplier^(N-1)` bytes.
    /// Default: 64 MiB.
    target_file_size_base: u64 = 64 * 1024 * 1024,

    /// Multiplier applied per level for target SST file size. 1 (default)
    /// keeps all levels at `target_file_size_base`; values > 1 allow deeper
    /// levels to have larger files, reducing file count.
    target_file_size_multiplier: c_int = 1,

    /// Maximum number of bytes in all compacted files in a single compaction
    /// job. 0 (default) means no limit beyond what the compaction scheduler
    /// naturally selects. Limiting this reduces I/O spikes.
    max_compaction_bytes: u64 = 0,

    /// Once the estimated total size of pending compaction work exceeds this
    /// value, writes are slowed. 0 disables the limit. Default: 64 GiB.
    soft_pending_compaction_bytes_limit: u64 = 64 * 1024 * 1024 * 1024,

    /// Once the estimated total size of pending compaction work exceeds this
    /// value, writes are stopped entirely until compaction catches up.
    /// 0 disables the limit. Default: 256 GiB.
    hard_pending_compaction_bytes_limit: u64 = 256 * 1024 * 1024 * 1024,

    // ------------------------------------------------------------------
    // Compaction behaviour
    // ------------------------------------------------------------------

    /// The compaction strategy for this column family. Each style has
    /// different trade-offs between write amplification, space amplification,
    /// and read amplification. Default: .level.
    compaction_style: CompactionStyle = .level,

    /// If true, automatic background compactions are completely disabled for
    /// this column family. Compactions must be triggered manually via
    /// `CompactRange`. Useful for bulk-load phases. Default: false.
    disable_auto_compactions: bool = false,

    /// Number of consecutive internal keys with the same user key that an
    /// iterator will skip before surfacing one. Higher values reduce CPU
    /// overhead when there are many versions of a key; lower values surface
    /// results faster for key-range scans. Default: 8.
    max_sequential_skip_in_iterations: u64 = 8,

    /// If true, record compaction and flush I/O statistics in the perf
    /// context and IOStats. Adds a small per-I/O overhead. Default: false.
    report_bg_io_stats: bool = false,

    /// Time-to-live in seconds for data in this column family. Keys are
    /// eligible for deletion once they are older than `ttl` seconds as
    /// measured by wall-clock time embedded in the SST. 0 = disabled
    /// (default). Note: use `fifo.ttl` instead when `compaction_style == .fifo`.
    ttl: u64 = 0,

    /// Trigger a full compaction of a file if it has not been compacted for
    /// this many seconds. Useful to reclaim space from expired TTL data or
    /// to ensure compaction filters are applied periodically. 0 = disabled
    /// (default).
    periodic_compaction_seconds: u64 = 0,

    /// Options specific to FIFO compaction. Only applied when
    /// `compaction_style == .fifo`.
    fifo: FifoCompactionOptions = .{},

    /// Options specific to universal compaction. Only applied when
    /// `compaction_style == .universal`.
    universal: UniversalCompactionOptions = .{},

    // ------------------------------------------------------------------
    // Bloom / index filters
    // ------------------------------------------------------------------

    /// If true, do not build Bloom filters for the last level of the LSM
    /// tree (where most data lives). Reduces memory and disk usage when the
    /// workload never looks up non-existent keys in that level. Default: false.
    optimize_filters_for_hits: bool = false,

    /// Control locality of Bloom filter data. Setting this to a power-of-2
    /// value causes the filter to be laid out in cache-friendly chunks of
    /// that size, improving cache efficiency for large filters.
    /// 0 = no locality hint (default).
    bloom_locality: u32 = 0,

    // ------------------------------------------------------------------
    // Block-based table / cache
    // ------------------------------------------------------------------

    /// Size of a data block in SST files before compression. Larger blocks
    /// improve sequential read throughput and compression ratio but increase
    /// read amplification for small point lookups. Default: 4 KiB.
    block_size: usize = 4096,

    /// If the last data block is smaller than this percentage of `block_size`,
    /// it is merged with the preceding block rather than being written as a
    /// partial block. Range: 1–100. Default: 10.
    block_size_deviation: c_int = 10,

    /// Number of keys between block restart points for prefix-delta encoding
    /// of data blocks. Smaller values give faster seeks at the cost of larger
    /// blocks; larger values compress better but make seeks slower. Default: 16.
    block_restart_interval: c_int = 16,

    /// Same as `block_restart_interval` but applied to index blocks.
    /// Default: 1.
    index_block_restart_interval: c_int = 1,

    /// Target size for each partitioned index/filter block when partitioned
    /// index/filters are enabled (`partition_filters = true`). Default: 4 KiB.
    metadata_block_size: u64 = 4096,

    /// If true, use a two-level partitioned filter structure. The top-level
    /// filter acts as an index into per-SST-partition filters, greatly
    /// reducing peak memory use for large SST files. Requires
    /// `cache_index_and_filter_blocks = true`. Default: false.
    partition_filters: bool = false,

    /// If true, encode the difference between successive keys (delta encoding)
    /// in index blocks, reducing index size. Disable if your comparator makes
    /// delta encoding meaningless. Default: true.
    use_delta_encoding: bool = true,

    /// If true, index and filter blocks are stored in the block cache rather
    /// than a separate in-memory structure. Allows their memory to be reclaimed
    /// under pressure at the cost of potential cache misses on cold reads.
    /// Default: false.
    cache_index_and_filter_blocks: bool = false,

    /// When `cache_index_and_filter_blocks` is true, insert these blocks at
    /// high priority in the block cache so they are the last to be evicted.
    /// Default: true.
    cache_index_and_filter_blocks_with_high_priority: bool = true,

    /// If true, pin the index and filter blocks for level-0 SST files in the
    /// block cache, preventing them from being evicted. Reduces read latency
    /// for the hottest data at the cost of a fixed memory reservation.
    /// Default: false.
    pin_l0_filter_and_index_blocks_in_cache: bool = false,

    /// If true, the top-level index of a partitioned filter/index is pinned
    /// in the block cache at all times. Default: true.
    pin_top_level_index_and_filter: bool = true,


    /// Checksum algorithm used to verify SST data blocks on read. A mismatch
    /// causes a `Corruption` error. Default: .crc32c.
    checksum: ChecksumType = .crc32c,

    /// Key encoding strategy for the block-based table. `.prefix` can yield
    /// significant space savings for datasets with long shared key prefixes
    /// but requires a compatible prefix extractor to be configured.
    /// Default: .plain.
    encoding_type: EncodingType = .plain,

    /// If true, disable the block cache entirely for this column family.
    /// All block reads bypass the block cache and go directly to the OS
    /// page cache. Useful for bulk scans that would otherwise pollute the
    /// cache. Default: false.
    no_block_cache: bool = false,

    /// If true, the block-based table filter is checked against the full key
    /// in addition to the prefix. Slightly more memory per filter but avoids
    /// false-positives for exact-match lookups. Default: true.
    whole_key_filtering: bool = true,



    /// SST file format version. Newer versions may add features or change
    /// on-disk layout. Older RocksDB versions cannot read newer format
    /// versions. Default: 5.
    format_version: i32 = 5,


    // ------------------------------------------------------------------
    // Misc
    // ------------------------------------------------------------------

    /// Maximum number of merge operations that will be combined in the
    /// memtable before a full merge is performed and the result written as
    /// a plain value. 0 = no limit (default). Setting a cap bounds the work
    /// done per read at the cost of more frequent full merges on write.
    max_successive_merges: usize = 0,


    // ------------------------------------------------------------------
    // convert() – builds a rocksdb_options_t*
    // ------------------------------------------------------------------
    pub fn convert(cfo: ColumnFamilyOptions) *rdb.struct_rocksdb_options_t {
        const ro = rdb.rocksdb_options_create().?;

        // ---- compression ----
        rdb.rocksdb_options_set_compression(ro, @intFromEnum(cfo.compression));
        rdb.rocksdb_options_set_bottommost_compression(ro, @intFromEnum(cfo.bottommost_compression));
        if (cfo.compression_per_level) |cpl| {
            rdb.rocksdb_options_set_compression_per_level(ro, @ptrCast(cpl.ptr), cpl.len);
        }

        // ---- write / memtable ----
        rdb.rocksdb_options_set_write_buffer_size(ro, cfo.write_buffer_size);
        rdb.rocksdb_options_set_max_write_buffer_number(ro, cfo.max_write_buffer_number);
        rdb.rocksdb_options_set_min_write_buffer_number_to_merge(ro, cfo.min_write_buffer_number_to_merge);
        rdb.rocksdb_options_set_max_write_buffer_size_to_maintain(ro, cfo.max_write_buffer_size_to_maintain);
        rdb.rocksdb_options_set_arena_block_size(ro, cfo.arena_block_size);
        rdb.rocksdb_options_set_memtable_prefix_bloom_size_ratio(ro, cfo.memtable_prefix_bloom_size_ratio);
        rdb.rocksdb_options_set_memtable_whole_key_filtering(ro, @intFromBool(cfo.memtable_whole_key_filtering));
        rdb.rocksdb_options_set_memtable_huge_page_size(ro, cfo.memtable_huge_page_size);
        rdb.rocksdb_options_set_inplace_update_support(ro, @intFromBool(cfo.inplace_update_support));
        rdb.rocksdb_options_set_inplace_update_num_locks(ro, cfo.inplace_update_num_locks);

        // ---- levels ----
        rdb.rocksdb_options_set_num_levels(ro, cfo.num_levels);
        rdb.rocksdb_options_set_level0_file_num_compaction_trigger(ro, cfo.level0_file_num_compaction_trigger);
        rdb.rocksdb_options_set_level0_slowdown_writes_trigger(ro, cfo.level0_slowdown_writes_trigger);
        rdb.rocksdb_options_set_level0_stop_writes_trigger(ro, cfo.level0_stop_writes_trigger);
        rdb.rocksdb_options_set_max_bytes_for_level_base(ro, cfo.max_bytes_for_level_base);
        rdb.rocksdb_options_set_max_bytes_for_level_multiplier(ro, cfo.max_bytes_for_level_multiplier);
        if (cfo.max_bytes_for_level_multiplier_additional) |mba| {
            rdb.rocksdb_options_set_max_bytes_for_level_multiplier_additional(ro, @constCast(@ptrCast(mba.ptr)), mba.len);
        }
        rdb.rocksdb_options_set_target_file_size_base(ro, cfo.target_file_size_base);
        rdb.rocksdb_options_set_target_file_size_multiplier(ro, cfo.target_file_size_multiplier);
        rdb.rocksdb_options_set_max_compaction_bytes(ro, cfo.max_compaction_bytes);
        rdb.rocksdb_options_set_soft_pending_compaction_bytes_limit(ro, cfo.soft_pending_compaction_bytes_limit);
        rdb.rocksdb_options_set_hard_pending_compaction_bytes_limit(ro, cfo.hard_pending_compaction_bytes_limit);

        // ---- compaction behaviour ----
        rdb.rocksdb_options_set_compaction_style(ro, @intFromEnum(cfo.compaction_style));
        rdb.rocksdb_options_set_disable_auto_compactions(ro, @intFromBool(cfo.disable_auto_compactions));
        rdb.rocksdb_options_set_max_sequential_skip_in_iterations(ro, cfo.max_sequential_skip_in_iterations);
        rdb.rocksdb_options_set_report_bg_io_stats(ro, @intFromBool(cfo.report_bg_io_stats));
        rdb.rocksdb_options_set_ttl(ro, cfo.ttl);
        rdb.rocksdb_options_set_periodic_compaction_seconds(ro, cfo.periodic_compaction_seconds);

        // ---- FIFO compaction ----
        {
            const fifo = rdb.rocksdb_fifo_compaction_options_create().?;
            defer rdb.rocksdb_fifo_compaction_options_destroy(fifo);
            rdb.rocksdb_fifo_compaction_options_set_max_table_files_size(fifo, cfo.fifo.max_table_files_size);
            rdb.rocksdb_fifo_compaction_options_set_allow_compaction(fifo, @intFromBool(cfo.fifo.allow_compaction));
            rdb.rocksdb_options_set_fifo_compaction_options(ro, fifo);
            // FIFO TTL is set via the same rocksdb_options_set_ttl call; it
            // overrides the CF-level `ttl` field when style == .fifo.
            rdb.rocksdb_options_set_ttl(ro, cfo.fifo.ttl);
        }

        // ---- universal compaction ----
        {
            const uo = rdb.rocksdb_universal_compaction_options_create().?;
            defer rdb.rocksdb_universal_compaction_options_destroy(uo);
            rdb.rocksdb_universal_compaction_options_set_size_ratio(uo, cfo.universal.size_ratio);
            rdb.rocksdb_universal_compaction_options_set_min_merge_width(uo, cfo.universal.min_merge_width);
            rdb.rocksdb_universal_compaction_options_set_max_merge_width(uo, cfo.universal.max_merge_width);
            rdb.rocksdb_universal_compaction_options_set_max_size_amplification_percent(uo, cfo.universal.max_size_amplification_percent);
            rdb.rocksdb_universal_compaction_options_set_compression_size_percent(uo, cfo.universal.compression_size_percent);
            rdb.rocksdb_universal_compaction_options_set_stop_style(uo, @intFromEnum(cfo.universal.stop_style));
            rdb.rocksdb_options_set_universal_compaction_options(ro, uo);
        }

        // ---- bloom / index ----
        rdb.rocksdb_options_set_optimize_filters_for_hits(ro, @intFromBool(cfo.optimize_filters_for_hits));
        rdb.rocksdb_options_set_bloom_locality(ro, cfo.bloom_locality);

        // ---- block-based table ----
        {
            const bbo = rdb.rocksdb_block_based_options_create().?;
            defer rdb.rocksdb_block_based_options_destroy(bbo);
            rdb.rocksdb_block_based_options_set_block_size(bbo, cfo.block_size);
            rdb.rocksdb_block_based_options_set_block_size_deviation(bbo, cfo.block_size_deviation);
            rdb.rocksdb_block_based_options_set_block_restart_interval(bbo, cfo.block_restart_interval);
            rdb.rocksdb_block_based_options_set_index_block_restart_interval(bbo, cfo.index_block_restart_interval);
            rdb.rocksdb_block_based_options_set_metadata_block_size(bbo, cfo.metadata_block_size);
            rdb.rocksdb_block_based_options_set_partition_filters(bbo, @intFromBool(cfo.partition_filters));
            rdb.rocksdb_block_based_options_set_use_delta_encoding(bbo, @intFromBool(cfo.use_delta_encoding));
            rdb.rocksdb_block_based_options_set_cache_index_and_filter_blocks(bbo, @intFromBool(cfo.cache_index_and_filter_blocks));
            rdb.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(bbo, @intFromBool(cfo.cache_index_and_filter_blocks_with_high_priority));
            rdb.rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(bbo, @intFromBool(cfo.pin_l0_filter_and_index_blocks_in_cache));
            rdb.rocksdb_block_based_options_set_pin_top_level_index_and_filter(bbo, @intFromBool(cfo.pin_top_level_index_and_filter));
            rdb.rocksdb_block_based_options_set_checksum(bbo, @intFromEnum(cfo.checksum));
            rdb.rocksdb_block_based_options_set_no_block_cache(bbo, @intFromBool(cfo.no_block_cache));
            rdb.rocksdb_block_based_options_set_whole_key_filtering(bbo, @intFromBool(cfo.whole_key_filtering));
            rdb.rocksdb_block_based_options_set_format_version(bbo, cfo.format_version);
            rdb.rocksdb_options_set_block_based_table_factory(ro, bbo);
        }

        // ---- misc ----
        rdb.rocksdb_options_set_max_successive_merges(ro, cfo.max_successive_merges);

        return ro;
    }
};

/// The metadata that describes a SST file
pub const LiveFile = struct {
    allocator: Allocator,
    /// Name of the column family the file belongs to
    column_family_name: []const u8,
    /// Name of the file
    name: []const u8,
    /// Size of the file
    size: usize,
    /// Level at which this file resides
    level: i32,
    /// Smallest user defined key in the file
    start_key: ?[]const u8,
    /// Largest user defined key in the file
    end_key: ?[]const u8,
    /// Number of entries/alive keys in the file
    num_entries: u64,
    /// Number of deletions/tomb key(s) in the file
    num_deletions: u64,

    pub fn deinit(self: LiveFile) void {
        self.allocator.free(self.column_family_name);
        self.allocator.free(self.name);
        if (self.start_key) |start_key| self.allocator.free(start_key);
        if (self.end_key) |end_key| self.allocator.free(end_key);
    }
};

const CallHandler = struct {
    /// The error string to pass into rocksdb.
    err_str_in: ?[*:0]u8 = null,
    /// The user's error string.
    err_str_out: *?Data,

    fn init(err_str_out: *?Data) CallHandler {
        return .{ .err_str_out = err_str_out };
    }

    fn errIn(self: *CallHandler) [*c][*c]u8 {
        return @ptrCast(&self.err_str_in);
    }

    fn handle(
        self: *CallHandler,
        ret: anytype,
        comptime err: anytype,
    ) @TypeOf(err)!@TypeOf(ret) {
        if (self.err_str_in) |s| {
            self.err_str_out.* = .{
                .data = std.mem.span(s),
                .free = rdb.rocksdb_free,
            };
            return err;
        } else {
            return ret;
        }
    }
};

const CfNameToHandleMap = struct {
    allocator: Allocator,
    map: std.StringHashMapUnmanaged(ColumnFamilyHandle),
    lock: RwLock,

    const Self = @This();

    fn create(allocator: Allocator) Allocator.Error!*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .map = .empty,
            .lock = .init,
        };
        return self;
    }

    fn destroy(self: *Self) void {
        var iter = self.map.iterator();
        while (iter.next()) |entry| {
            rdb.rocksdb_column_family_handle_destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.map.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn put(self: *Self, name: []const u8, handle: ColumnFamilyHandle) Allocator.Error!void {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        self.lock.lock();
        defer self.lock.unlock();

        try self.map.put(self.allocator, owned_name, handle);
    }

    fn get(self: *Self, name: []const u8) ?ColumnFamilyHandle {
        self.lock.lockShared();
        defer self.lock.unlockShared();
        return self.map.get(name);
    }
};

test DB {
    var err_str: ?Data = null;
    defer if (err_str) |e| e.deinit();
    runTest(&err_str) catch |e| {
        std.debug.print("{}: {?f}\n", .{ e, err_str });
        return e;
    };
}

fn runTest(err_str: *?Data) !void {
    const allocator = std.testing.allocator;

    {
        var db, const families = try DB.open(
            allocator,
            "test-state",
            .{
                .create_if_missing = true,
                .create_missing_column_families = true,
            },
            &.{
                .{ .name = "default" },
                .{ .name = "another" },
            },
            false,
            err_str,
        );
        defer db.deinit();
        defer allocator.free(families);
        const a_family = families[1].handle;

        _ = try db.put(a_family, "hello", "world", err_str);
        _ = try db.put(a_family, "zebra", "world", err_str);

        db = db.withDefaultColumnFamily(a_family);

        const val = try db.get(null, "hello", err_str);
        try std.testing.expect(std.mem.eql(u8, val.?.data, "world"));

        var iter = db.iterator(null, .forward, null);
        defer iter.deinit();
        var v = (try iter.nextValue(err_str)).?;
        try std.testing.expect(std.mem.eql(u8, "world", v.data));
        v = (try iter.nextValue(err_str)).?;
        try std.testing.expect(std.mem.eql(u8, "world", v.data));
        try std.testing.expect(null == try iter.next(err_str));

        try db.delete(null, "hello", err_str);

        const noval = try db.get(null, "hello", err_str);
        try std.testing.expect(null == noval);
    }

    var db, const families = try DB.open(
        allocator,
        "test-state",
        .{
            .create_if_missing = true,
            .create_missing_column_families = true,
        },
        &.{
            .{ .name = "default" },
            .{ .name = "another" },
        },
        false,
        err_str,
    );
    defer db.deinit();
    defer allocator.free(families);

    const lfs = try db.liveFiles(allocator);
    defer {
        for (lfs) |lf| lf.deinit();
        allocator.free(lfs);
    }
    try std.testing.expect(std.mem.eql(u8, "another", lfs[0].column_family_name));
}

// ==========================================
// NEW COMPRESSION TEST SUITE
// ==========================================
test "DB Compression" {
    var err_str: ?Data = null;
    
    defer if (err_str) |*e| e.deinit(); 

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    
    // We get a fresh directory path for this test
    const path = try dir.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(path);

    const allocator = std.testing.allocator;

    // Open a DB with column families set to use different compression algorithms
    var db, const families = try DB.open(
        allocator,
        path,
        .{
            .create_if_missing = true,
            .create_missing_column_families = true,
        },
        &.{
            .{ .name = "default", .options = .{ .compression = .no_compression } },
            .{ .name = "cf_lz4",  .options = .{ .compression = .lz4 } },
            .{ .name = "cf_zstd", .options = .{ .compression = .zstd } },
            .{ .name = "cf_snap", .options = .{ .compression = .snappy } },
        },
        false,
        &err_str,
    );
    defer db.deinit();
    defer allocator.free(families);

    // Create a highly compressible payload to make sure the algorithms actually run
    const payload: [1024]u8 = @splat('A');

    for (families) |cf| {
        // Write the data
        try db.put(cf.handle, "test_key", &payload, &err_str);
        
        // Force a flush so RocksDB moves it from memtable (uncompressed) 
        // to an SST file on disk (compressed). If the compression lib isn't 
        // linked correctly, RocksDB will silently fallback or error out here depending on settings.
        try db.flush(cf.handle, &err_str);

        // Verify we can read it back
        const val = try db.get(cf.handle, "test_key", &err_str);
        
        try std.testing.expect(val != null);
        try std.testing.expectEqualStrings(&payload, val.?.data);
    }
}