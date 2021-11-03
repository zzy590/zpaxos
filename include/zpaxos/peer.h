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
// Created by zzy on 2019-01-23.
//

#pragma once

#include <set>

#include "utility/common_define.h"

#include "type.h"

namespace zpaxos {

    class CpeerManager {

    NO_COPY_MOVE(CpeerManager);

    public:

        CpeerManager() = default;

        virtual ~CpeerManager() = default;

        virtual bool is_voter(const group_id_t &group_id, const instance_id_t &instance_id,
                              const node_id_t &node_id) = 0;

        virtual bool is_all_peer(const group_id_t &group_id, const instance_id_t &instance_id,
                                 const std::set<node_id_t> &node_set) = 0;

        virtual bool is_all_voter(const group_id_t &group_id, const instance_id_t &instance_id,
                                  const std::set<node_id_t> &node_set) = 0;

        virtual bool is_quorum(const group_id_t &group_id, const instance_id_t &instance_id,
                               const std::set<node_id_t> &node_set) = 0;

    };

}
