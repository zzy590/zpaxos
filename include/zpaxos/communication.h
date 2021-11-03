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

#include <functional>

#include "utility/common_define.h"
#include "utility/atomicex.h"

#include "type.h"

namespace zpaxos {

    class Ccommunication {

    NO_COPY_MOVE(Ccommunication);

    private:

        utility::CspinRWLock lock_;
        std::function<int(message_t && msg)> recv_cb_;

    public:

        Ccommunication() = default;

        virtual ~Ccommunication() = default;

        void set_message_callback(const std::function<int(message_t && msg)> &cb) {
            utility::CautoSpinRWLock lck(lock_, true);
            recv_cb_ = cb;
        }

        //
        // Return 0 means success.
        // Return >0 if no callback.
        // Return <0 means error.
        //
        int on_receive(message_t &&msg) {
            utility::CautoSpinRWLock lck(lock_);
            if (recv_cb_)
                return recv_cb_(std::forward<message_t>(msg));
            return 1;
        }

        virtual int send(const node_id_t &target_id, const message_t &message) = 0;

        enum broadcast_range_e {
            broadcast_voter = 0,
            broadcast_follower,
            broadcast_all,
        };

        enum broadcast_type_e {
            broadcast_self_first = 0,
            broadcast_self_last,
            broadcast_no_self,
        };

        virtual int broadcast(const message_t &message, broadcast_range_e range, broadcast_type_e type) = 0;

    };

}
