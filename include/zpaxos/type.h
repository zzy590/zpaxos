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

#include <cstdint>

namespace zpaxos {

    typedef uint64_t proposal_id_t; // (0, inf)
    typedef uint64_t node_id_t; // non-zero

    typedef uint32_t group_id_t; // [0, group_count)
    typedef uint64_t instance_id_t; // [0, inf)

    typedef uint32_t state_machine_id_t; // (0, inf)

    // Constance(s).
    static const instance_id_t INSTANCE_ID_INVALID = UINT64_C(0xFFFFFFFFFFFFFFFF);
    // Max, but never get consistent(Denied by peer manager).
    static const instance_id_t INSTANCE_ID_MAX = UINT64_C(0xFFFFFFFFFFFFFFFE);
    // Second max, which can reach.
    static const instance_id_t INSTANCE_ID_SECOND_MAX = UINT64_C(0xFFFFFFFFFFFFFFFD);

    // Pre-define.
    struct message_t;
    struct state_t;

}
