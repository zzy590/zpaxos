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
// Created by zzy on 2018-12-30.
//

#pragma once

#include <vector>

#include "utility/common_define.h"

#include "type.h"

namespace zpaxos {

    struct write_options_t final {

        enum data_safe_level_e {
            level_none = 0, // Data may lost if application crash.
            level_system, // Data may lost if system crash.
            level_disk, // Data may lost if disk crash.
        } safe_level;

        explicit write_options_t(data_safe_level_e safe_level = level_system)
                : safe_level(safe_level) {}

    };

    class Cstorage {

    NO_COPY_MOVE(Cstorage);

    public:

        Cstorage() = default;

        virtual ~Cstorage() = default;

        virtual int get(const group_id_t &group_id, const instance_id_t &instance_id, state_t &state) = 0;

        // Batch get. Get states [instance_id, instance_id + states.size()).
        virtual int get(const group_id_t &group_id, const instance_id_t &instance_id, std::vector<state_t> &states) = 0;

        virtual int put(const group_id_t &group_id, const instance_id_t &instance_id,
                        const state_t &state, const write_options_t &options) = 0;

        // Batch put. Put states [instance_id, instance_id + states.size()).
        virtual int put(const group_id_t &group_id, const instance_id_t &instance_id,
                        const std::vector<state_t> &states, const write_options_t &options) = 0;

        // Purging all state **less** than end_instance_id(not included).
        virtual int compact(const group_id_t &group_id, const instance_id_t &end_instance_id) = 0;

        // Return value: 0 success, >0 empty, <0 error. This can >0 after compact.
        virtual int get_min_instance_id(const group_id_t &group_id, instance_id_t &instance_id) = 0;

        // Return value: 0 success, >0 empty, <0 error.
        virtual int get_max_instance_id(const group_id_t &group_id, instance_id_t &instance_id) = 0;

        // Purging all state **less** than instance_id and put this state in a transaction(atomic operation).
        // Minimum instance will be this instance after invoking successfully.
        virtual int reset_min_instance(const group_id_t &group_id, const instance_id_t &instance_id,
                                       const state_t &state, const write_options_t &options) = 0;

    };

}
