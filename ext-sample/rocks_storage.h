//
// Created by zzy on 2020/3/31.
//

#pragma once

#include <cassert>
#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <stdexcept>

#include "rocksdb/db.h"

#include "zpaxos/utility/common_define.h"

#include "utility/serialize_util.h"

#include "zpaxos.h"
#include "storage_ext.h"

namespace storage {

#define GROUP_COLUMN_FAMILY_PREFIX "group_"
#define EXT_COLUMN_FAMILY_NAME "zpaxos-ext"

    class CrocksInstComparator final : public rocksdb::Comparator {

        int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const final {
            if (a.size() != sizeof(zpaxos::instance_id_t) ||
                b.size() != sizeof(zpaxos::instance_id_t))
                throw std::runtime_error("Bad key for CrocksInstComparator.");

            auto inst_a = *reinterpret_cast<const zpaxos::instance_id_t *>(a.data());
            auto inst_b = *reinterpret_cast<const zpaxos::instance_id_t *>(b.data());

            if (inst_a == inst_b)
                return 0;

            return inst_a < inst_b ? -1 : 1;
        }

        const char *Name() const final {
            return "CrocksInstComparator for zpaxos.";
        }

        void FindShortestSeparator(std::string *start, const rocksdb::Slice &limit) const final {}

        void FindShortSuccessor(std::string *key) const final {}

    };

    class CrocksStorage final : public zpaxos::CstorageExt {

    NO_COPY_MOVE(CrocksStorage);

    private:

        CrocksInstComparator inst_comparator_;
        std::unique_ptr<rocksdb::DB> db_;
        std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> cfs_;
        std::unique_ptr<rocksdb::ColumnFamilyHandle> ext_cf_;

    public:

        CrocksStorage(const std::string &base_path, size_t group_number, bool large_node = false) {
            // Check protobuf.
            GOOGLE_PROTOBUF_VERIFY_VERSION;

            // Prob existing.
            std::vector<std::string> column_names;
            auto s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), base_path, &column_names);
            if (!s.ok())
                column_names.clear();

            if (!column_names.empty()) {
                // Existing DB.
                if (column_names.size() != group_number + 2) // default & ext
                    throw std::runtime_error("Storage group number mismatch.");

                for (zpaxos::group_id_t group_id = 0; group_id < group_number; ++group_id) {
                    char buf[32];
                    ::snprintf(buf, sizeof(buf), GROUP_COLUMN_FAMILY_PREFIX "%u", group_id);
                    if (column_names[group_id + 1] != buf)
                        throw std::runtime_error("Storage group name mismatch.");
                }

                if (column_names[1 + group_number] != EXT_COLUMN_FAMILY_NAME)
                    throw std::runtime_error("Storage ext column family mismatch.");
            }

            // Recalculate for every column family.
            size_t per_memory = (large_node ? 512 : 128) / group_number / 4 * 4 * 1024 * 1024;
            if (per_memory < 4 * 1024 * 1024)
                per_memory = 4 * 1024 * 1024; // At least 4MB.

            rocksdb::Options options;
            // 4core for flush and compaction for small node.
            options.IncreaseParallelism(large_node ? 16 : 4);
            options.OptimizeForSmallDb();
            options.OptimizeLevelStyleCompaction(per_memory);

            rocksdb::ColumnFamilyOptions columnFamilyOptions;
            columnFamilyOptions.OptimizeForSmallDb();
            columnFamilyOptions.OptimizeLevelStyleCompaction(per_memory);

            if (column_names.empty()) {
                // Create DB.
                options.create_if_missing = true;

                rocksdb::DB *db = nullptr;
                s = rocksdb::DB::Open(options, base_path, &db);
                if (!s.ok()) {
                    assert(nullptr == db);
                    throw std::runtime_error(s.getState());
                }
                assert(db != nullptr);
                db_.reset(db);

                // Init group column family.
                if (::strlen(GROUP_COLUMN_FAMILY_PREFIX) > 32) // Length check.
                    throw std::runtime_error("Storage group column family prefix too long.");
                columnFamilyOptions.comparator = &inst_comparator_;
                cfs_.reserve(group_number);
                for (zpaxos::group_id_t group_id = 0; group_id < group_number; ++group_id) {
                    char buf[64];
                    ::snprintf(buf, sizeof(buf), GROUP_COLUMN_FAMILY_PREFIX "%u", group_id);
                    rocksdb::ColumnFamilyHandle *handle = nullptr;
                    s = db_->CreateColumnFamily(columnFamilyOptions, buf, &handle);
                    if (!s.ok()) {
                        assert(nullptr == handle);
                        cfs_.clear();
                        db_.reset();
                        throw std::runtime_error(s.getState());
                    } else {
                        assert(handle != nullptr);
                        cfs_.emplace_back(handle);
                    }
                }

                // Init ext column family.
                columnFamilyOptions.comparator = rocksdb::BytewiseComparator();
                rocksdb::ColumnFamilyHandle *handle = nullptr;
                s = db_->CreateColumnFamily(columnFamilyOptions, EXT_COLUMN_FAMILY_NAME, &handle);
                if (!s.ok()) {
                    assert(nullptr == handle);
                    cfs_.clear();
                    db_.reset();
                    throw std::runtime_error(s.getState());
                } else {
                    assert(handle != nullptr);
                    ext_cf_.reset(handle);
                }
            } else {
                std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
                column_families.reserve(column_names.size());
                auto prefix_len = ::strlen(GROUP_COLUMN_FAMILY_PREFIX);
                for (const auto &name : column_names) {
                    if (name.size() >= prefix_len &&
                        0 == ::strncmp(name.c_str(), GROUP_COLUMN_FAMILY_PREFIX, prefix_len))
                        columnFamilyOptions.comparator = &inst_comparator_;
                    else
                        columnFamilyOptions.comparator = rocksdb::BytewiseComparator();
                    column_families.emplace_back(name, columnFamilyOptions);
                }

                // Open DB.
                std::vector<rocksdb::ColumnFamilyHandle *> handles;
                rocksdb::DB *db = nullptr;
                s = rocksdb::DB::Open(options, base_path, column_families, &handles, &db);
                if (!s.ok()) {
                    assert(nullptr == db);
                    throw std::runtime_error(s.getState());
                }
                assert(db != nullptr);
                db_.reset(db);

                assert(handles.size() == column_families.size());
                cfs_.reserve(group_number);
                for (zpaxos::group_id_t group_id = 0; group_id < group_number; ++group_id)
                    cfs_.emplace_back(handles[group_id + 1]);
                ext_cf_.reset(handles[1 + group_number]);
                delete handles[0]; // Delete default one.
            }
        }

        int get(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                zpaxos::state_t &state) final {
            if (group_id >= cfs_.size())
                return -1;

            std::string value;
            auto s = db_->Get(rocksdb::ReadOptions(), cfs_[group_id].get(),
                              rocksdb::Slice(reinterpret_cast<const char *>(&instance_id), sizeof(instance_id)),
                              &value);
            if (!s.ok()) {
                if (s.IsNotFound())
                    return 1;
                return -1;
            }
            return utility::Cserialize::deserialize(value, state) ? 0 : -1;
        }

        // Batch get. Get states [instance_id, instance_id + states.size()).
        int get(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                std::vector<zpaxos::state_t> &states) final {
            if (group_id >= cfs_.size())
                return -1;

            auto batch_size = states.size();
            std::vector<rocksdb::ColumnFamilyHandle *> cfs(batch_size, cfs_[group_id].get());
            std::vector<zpaxos::instance_id_t> insts(batch_size);
            for (auto i = 0; i < batch_size; ++i)
                insts[i] = instance_id + i;
            std::vector<rocksdb::Slice> keys;
            keys.reserve(batch_size);
            for (const auto &item : insts)
                keys.emplace_back(reinterpret_cast<const char *>(&item), sizeof(item));

            std::vector<std::string> vals;
            auto not_found_cnt = 0;
            auto status = db_->MultiGet(rocksdb::ReadOptions(), cfs, keys, &vals);
            for (const auto &item : status) {
                if (!item.ok()) {
                    if (item.IsNotFound())
                        ++not_found_cnt;
                    else
                        return -1;
                }
            }
            if (not_found_cnt > 0)
                return 1;

            for (auto i = 0; i < batch_size; ++i) {
                if (!utility::Cserialize::deserialize(vals[i], states[i]))
                    return -1;
            }
            return 0;
        }

        int put(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                const zpaxos::state_t &state, const zpaxos::write_options_t &options) final {
            if (group_id >= cfs_.size())
                return -1;

            std::string val;
            if (!utility::Cserialize::serialize(state, val))
                return -1;

            rocksdb::WriteOptions writeOptions;
            if (zpaxos::write_options_t::level_disk == options.safe_level)
                writeOptions.sync = true;
            auto s = db_->Put(writeOptions, cfs_[group_id].get(),
                              rocksdb::Slice(reinterpret_cast<const char *>(&instance_id), sizeof(instance_id)), val);
            return s.ok() ? 0 : -1;
        }

        // Batch put. Put states [instance_id, instance_id + states.size()).
        int put(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                const std::vector<zpaxos::state_t> &states, const zpaxos::write_options_t &options) final {
            if (group_id >= cfs_.size())
                return -1;

            rocksdb::WriteBatch writeBatch;
            for (auto i = 0; i < states.size(); ++i) {
                std::string val;
                if (!utility::Cserialize::serialize(states[i], val))
                    return -1;
                auto inst = instance_id + i;
                auto s = writeBatch.Put(cfs_[group_id].get(),
                                        rocksdb::Slice(reinterpret_cast<const char *>(&inst), sizeof(inst)),
                                        rocksdb::Slice(val));
                if (!s.ok())
                    return -1;
            }

            rocksdb::WriteOptions writeOptions;
            if (zpaxos::write_options_t::level_disk == options.safe_level)
                writeOptions.sync = true;
            auto s = db_->Write(writeOptions, &writeBatch);
            return s.ok() ? 0 : -1;
        }

        // Purging all state **less** than end_instance_id(not included).
        int compact(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &end_instance_id) final {
            if (group_id >= cfs_.size())
                return -1;

            zpaxos::instance_id_t begin = 0;
            auto s = db_->DeleteRange(rocksdb::WriteOptions(), cfs_[group_id].get(),
                                      rocksdb::Slice(reinterpret_cast<const char *>(&begin), sizeof(begin)),
                                      rocksdb::Slice(reinterpret_cast<const char *>(&end_instance_id),
                                                     sizeof(end_instance_id)));
            return s.ok() ? 0 : -1;
        }

        // Return value: 0 success, >0 empty, <0 error. This can >0 after compact.
        int get_min_instance_id(const zpaxos::group_id_t &group_id, zpaxos::instance_id_t &instance_id) final {
            if (group_id >= cfs_.size())
                return -1;

            std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions(), cfs_[group_id].get()));
            if (!iterator)
                return -1;
            iterator->SeekToFirst();
            if (iterator->Valid()) {
                if (iterator->key().size() != sizeof(zpaxos::instance_id_t))
                    return -1;
                instance_id = *reinterpret_cast<const zpaxos::instance_id_t *>(iterator->key().data());
                return 0;
            }
            return 1;
        }

        // Return value: 0 success, >0 empty, <0 error.
        int get_max_instance_id(const zpaxos::group_id_t &group_id, zpaxos::instance_id_t &instance_id) final {
            if (group_id >= cfs_.size())
                return -1;

            std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(rocksdb::ReadOptions(), cfs_[group_id].get()));
            if (!iterator)
                return -1;
            iterator->SeekToLast();
            if (iterator->Valid()) {
                if (iterator->key().size() != sizeof(zpaxos::instance_id_t))
                    return -1;
                instance_id = *reinterpret_cast<const zpaxos::instance_id_t *>(iterator->key().data());
                return 0;
            }
            return 1;
        }

        // Purging all state **less** than instance_id and put this state in a transaction(atomic operation).
        // Minimum instance will be this instance after invoking successfully.
        int reset_min_instance(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                               const zpaxos::state_t &state, const zpaxos::write_options_t &options) final {
            if (group_id >= cfs_.size())
                return -1;

            std::string val;
            if (!utility::Cserialize::serialize(state, val))
                return -1;

            rocksdb::WriteBatch writeBatch;
            zpaxos::instance_id_t begin = 0;
            auto s = writeBatch.DeleteRange(
                    cfs_[group_id].get(),
                    rocksdb::Slice(reinterpret_cast<const char *>(&begin), sizeof(begin)),
                    rocksdb::Slice(reinterpret_cast<const char *>(&instance_id), sizeof(instance_id)));
            if (!s.ok())
                return -1;
            s = writeBatch.Put(
                    cfs_[group_id].get(),
                    rocksdb::Slice(reinterpret_cast<const char *>(&instance_id), sizeof(instance_id)),
                    rocksdb::Slice(val));
            if (!s.ok())
                return -1;

            rocksdb::WriteOptions writeOptions;
            if (zpaxos::write_options_t::level_disk == options.safe_level)
                writeOptions.sync = true;
            s = db_->Write(writeOptions, &writeBatch);
            return s.ok() ? 0 : -1;
        }

        size_t group_number() const final {
            return cfs_.size();
        }

        int get(const zpaxos::group_id_t &group_id, const std::string &key, std::string &value) final {
            if (group_id >= cfs_.size())
                return -1;

            char buf[32];
            ::snprintf(buf, sizeof(buf), "_%u", group_id);
            auto s = db_->Get(rocksdb::ReadOptions(), ext_cf_.get(), key + buf, &value);
            return s.ok() ? 0 : (s.IsNotFound() ? 1 : -1);
        }

        int put(const zpaxos::group_id_t &group_id, const std::string &key, const std::string &value,
                const zpaxos::write_options_t &options) final {
            if (group_id >= cfs_.size())
                return -1;

            char buf[32];
            ::snprintf(buf, sizeof(buf), "_%u", group_id);
            rocksdb::WriteOptions writeOptions;
            if (zpaxos::write_options_t::level_disk == options.safe_level)
                writeOptions.sync = true;
            auto s = db_->Put(writeOptions, ext_cf_.get(), key + buf, value);
            return s.ok() ? 0 : -1;
        }

        int del(const zpaxos::group_id_t &group_id, const std::string &key,
                const zpaxos::write_options_t &options) final {
            if (group_id >= cfs_.size())
                return -1;

            char buf[32];
            ::snprintf(buf, sizeof(buf), "_%u", group_id);
            rocksdb::WriteOptions writeOptions;
            if (zpaxos::write_options_t::level_disk == options.safe_level)
                writeOptions.sync = true;
            auto s = db_->Delete(writeOptions, ext_cf_.get(), key + buf);
            return s.ok() ? 0 : -1;
        }

    };

}
