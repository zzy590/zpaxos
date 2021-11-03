/*
 * Copyright (c) 2019-2021 Zhenyu Zhang. All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

//
// Created by zzy on 2019-02-07.
//

#pragma once

#include <map>
#include <deque>
#include <mutex>

#include "../utility/common_define.h"
#include "../utility/time.h"

#include "../type.h"
#include "../storage.h"
#include "../base.h"
#include "../log.h"

namespace storage {

    class CmemoryStorage final : public zpaxos::Cstorage {

    NO_COPY_MOVE(CmemoryStorage);

    private:

        std::mutex lock_;
        std::map<zpaxos::group_id_t, std::pair<zpaxos::instance_id_t, std::deque<zpaxos::state_t>>> store_;

    public:

        CmemoryStorage() = default;

        int get(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                zpaxos::state_t &state) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                return -1;

            if (instance_id < it->second.first || instance_id >= it->second.second.size())
                return -2;

            state = it->second.second[instance_id];
            return 0;
        }

        // Batch get. Get states [instance_id, instance_id + states.size()).
        int get(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                std::vector<zpaxos::state_t> &states) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                return -1;

            if (instance_id < it->second.first || instance_id + states.size() > it->second.second.size())
                return -2;

            auto off = 0;
            for (auto &state : states)
                state = it->second.second[instance_id + off++];
            return 0;
        }

        int put(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                const zpaxos::state_t &state, const zpaxos::write_options_t &options) final {
            auto start_time_us = utility::Ctime::steady_us();

            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                it = store_.emplace(group_id, std::make_pair(0, std::deque<zpaxos::state_t>())).first;

            if (it->second.second.size() <= instance_id)
                it->second.second.resize(instance_id + 1);
            it->second.second[instance_id] = state;

            auto end_time_us = utility::Ctime::steady_us();
            if (end_time_us - start_time_us > 100) {
                LOG_WARN(("Dealing put slow {}us", end_time_us - start_time_us));
            }
            return 0;
        }

        // Batch put. Put states [instance_id, instance_id + states.size()).
        int put(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                const std::vector<zpaxos::state_t> &states, const zpaxos::write_options_t &options) final {
            auto start_time_us = utility::Ctime::steady_us();

            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                it = store_.emplace(group_id, std::make_pair(0, std::deque<zpaxos::state_t>())).first;

            if (it->second.second.size() < instance_id + states.size())
                it->second.second.resize(instance_id + states.size());

            auto off = 0;
            for (const auto &state : states)
                it->second.second[instance_id + off++] = state;

            auto end_time_us = utility::Ctime::steady_us();
            if (end_time_us - start_time_us > 100) {
                LOG_WARN(("Dealing put slow {}us", end_time_us - start_time_us));
            }
            return 0;
        }

        // Purging all state **less** than end_instance_id(not included).
        int compact(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &end_instance_id) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                return -1;

            it->second.first = std::max(it->second.first, end_instance_id);
            for (zpaxos::instance_id_t i = 0; i < it->second.first; ++i)
                it->second.second[i].reset_accepted(); // Clear compacted.
            return 0;
        }

        // Return value: 0 success, >0 empty, <0 error. This can >0 after compact.
        int get_min_instance_id(const zpaxos::group_id_t &group_id, zpaxos::instance_id_t &instance_id) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end() || it->second.second.empty())
                return 1; // empty

            instance_id = it->second.first;

            return 0;
        }

        // Return value: 0 success, >0 empty, <0 error.
        int get_max_instance_id(const zpaxos::group_id_t &group_id, zpaxos::instance_id_t &instance_id) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end() || it->second.second.empty())
                return 1; // empty

            instance_id = it->second.second.size() - 1;

            return 0;
        }

        int reset_min_instance(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                               const zpaxos::state_t &state, const zpaxos::write_options_t &options) final {
            std::lock_guard<std::mutex> lck(lock_);

            auto it = store_.find(group_id);
            if (it == store_.end())
                it = store_.emplace(group_id, std::make_pair(0, std::deque<zpaxos::state_t>())).first;

            if (it->second.second.size() <= instance_id)
                it->second.second.resize(instance_id + 1);
            it->second.second[instance_id] = state;

            it->second.first = std::max(it->second.first, instance_id);
            for (zpaxos::instance_id_t i = 0; i < it->second.first; ++i)
                it->second.second[i].reset_accepted(); // Clear compacted.
            return 0;
        }

    };

}
