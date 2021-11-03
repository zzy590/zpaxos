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
// Created by zzy on 2019/9/28.
//

#pragma once

#include <cstdint>

#include "utility/common_define.h"
#include "utility/atomicex.h"
#include "utility/time.h"

#include "type.h"

namespace zpaxos {

    // Record latest proposer as leader and ignore any other proposer for a while.
    class CleaderManager {

    NO_COPY_MOVE(CleaderManager);

    private:

        const uint16_t lease_;

        utility::CspinRWLock lock_;
        zpaxos::node_id_t leader_node_id_;
        int64_t expire_time_;

    public:

        explicit CleaderManager(uint16_t lease = 3000) // leader lease: 3s
                : lease_(lease), leader_node_id_(0), expire_time_(0) {}

        bool admit_propose(const zpaxos::node_id_t &node_id) {
            auto now_time = utility::Ctime::steady_ms();

            utility::CautoSpinRWLock lck(lock_);
            return node_id == leader_node_id_ || now_time >= expire_time_;
        }

        void record_leader(const zpaxos::node_id_t &node_id) {
            auto expire_time = utility::Ctime::steady_ms() + lease_;

            utility::CautoSpinRWLock lck(lock_, true);
            if (node_id != leader_node_id_)
                leader_node_id_ = node_id;
            expire_time_ = expire_time;
        }

        /**
         * Get now leader node id.
         * @param ignore_expired_leader Set true if you want to ignore expired leader.
         * @return 0 if no valid leader.
         */
        zpaxos::node_id_t get_leader(bool ignore_expired_leader = false) {
            int64_t now_time = 0;
            if (ignore_expired_leader)
                now_time = utility::Ctime::steady_ms();

            utility::CautoSpinRWLock lck(lock_);
            if (now_time < expire_time_)
                return leader_node_id_;
            return 0;
        }

    };

}
