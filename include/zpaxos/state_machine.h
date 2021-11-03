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
// Created by zzy on 2019-07-07.
//

#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include <mutex>

#include "utility/common_define.h"
#include "utility/buffer.h"

#include "type.h"

namespace zpaxos {

    static const state_machine_id_t CUSTOM_SMID_BASE = 0x1000;

    class Csnapshot {

    NO_COPY_MOVE(Csnapshot);

    public:

        typedef std::unique_ptr<Csnapshot> ptr;
        typedef std::shared_ptr<Csnapshot> shared_ptr;

        Csnapshot() = default;

        virtual ~Csnapshot() = default;

        // Get global state machine id which identify myself, and this should be **unique**.
        virtual state_machine_id_t get_id() const = 0;

        // The snapshot represent the state machine which run to this id(not included).
        virtual const instance_id_t &get_next_instance_id() const = 0;

    };

    class CstateMachine {

    NO_COPY_MOVE(CstateMachine);

    public:

        typedef std::unique_ptr<CstateMachine> ptr;

        CstateMachine() = default;

        virtual ~CstateMachine() = default;

        // Get global state machine id which identify myself, and this should be **unique**.
        virtual state_machine_id_t get_id() const = 0;

        // This will be invoked sequentially by instance id,
        // and this callback should have ability to handle duplicate id caused by replay.
        // Should throw exception if get unexpected instance id.
        // If instance's chosen value is not for this SM, empty value will given.
        virtual void consume_sequentially(const instance_id_t &instance_id, const utility::Cslice &value) = 0;

        // Supply buffer which can move.
        virtual void consume_sequentially(const instance_id_t &instance_id, utility::Cbuffer &&value) {
            consume_sequentially(instance_id, value.slice());
        }

        // Return instance id which will execute on next round.
        // Can smaller than actual value only in concurrent with consuming, but **never** larger than real value.
        virtual instance_id_t get_next_execute_id() = 0;

        // Return smallest instance id which not been persisted.
        // Can smaller than actual value only in concurrent with consuming, but **never** larger than real value.
        virtual instance_id_t get_next_persist_id() = 0;

        // The get_next_instance_id of snapshot returned should >= get_next_persist_id().
        virtual int take_snapshot(Csnapshot::shared_ptr &snapshot) = 0;

        // next_persist_id should larger or equal to snapshot after successfully load.
        virtual int load_snapshot(const Csnapshot::shared_ptr &snapshot) = 0;

    };

    class CstateMachineBase : public CstateMachine {

    NO_COPY_MOVE(CstateMachineBase);

    private:

        std::mutex lock_;
        instance_id_t base_instance_id_;
        size_t buffer_pointer_;
        std::vector<utility::Cbuffer> buffers_;
        std::vector<bool> flags_;

    public:

        typedef std::unique_ptr<CstateMachineBase> ptr;

        explicit CstateMachineBase(size_t max_buffer_depth)
                : base_instance_id_(0), buffer_pointer_(0),
                  buffers_(max_buffer_depth), flags_(max_buffer_depth, false) {}

        ~CstateMachineBase() override = default;

        void reset_base_instance_id(const instance_id_t &instance_id) {
            std::lock_guard<std::mutex> lck(lock_);
            if (instance_id <= base_instance_id_)
                return; // Rollback is not allowed.
            if (instance_id - base_instance_id_ >= buffers_.size()) {
                // All clear and reset.
                base_instance_id_ = instance_id;
                buffer_pointer_ = 0;
                for (auto &buffer : buffers_)
                    buffer.resize(0);
                for (auto &&flag : flags_)
                    flag = false;
            } else {
                while (base_instance_id_ < instance_id) {
                    // Drop data if exists and push.
                    buffers_[buffer_pointer_].resize(0);
                    flags_[buffer_pointer_] = false;

                    ++base_instance_id_;
                    ++buffer_pointer_;
                    if (buffer_pointer_ == buffers_.size())
                        buffer_pointer_ = 0;
                }

                // Consume if can.
                while (flags_[buffer_pointer_]) {
                    consume_sequentially(base_instance_id_, std::move(buffers_[buffer_pointer_]));
                    buffers_[buffer_pointer_].resize(0);
                    flags_[buffer_pointer_] = false;

                    ++base_instance_id_;
                    ++buffer_pointer_;
                    if (buffer_pointer_ == buffers_.size())
                        buffer_pointer_ = 0;
                }
            }
        }

        // Return false if buffer overflow.
        bool push(const instance_id_t &instance_id, utility::Cbuffer &&value) {
            std::lock_guard<std::mutex> lck(lock_);

            // Check duplicate.
            if (instance_id < base_instance_id_)
                return true;
            // Check overflow.
            if (instance_id - base_instance_id_ >= buffers_.size())
                return false;

            // Store new.
            auto pos = (instance_id - base_instance_id_ + buffer_pointer_) % buffers_.size();
            buffers_[pos] = std::forward<utility::Cbuffer>(value);
            flags_[pos] = true;

            // Consume if can.
            while (flags_[buffer_pointer_]) {
                consume_sequentially(base_instance_id_, std::move(buffers_[buffer_pointer_]));
                buffers_[buffer_pointer_].resize(0);
                flags_[buffer_pointer_] = false;

                ++base_instance_id_;
                ++buffer_pointer_;
                if (buffer_pointer_ == buffers_.size())
                    buffer_pointer_ = 0;
            }

            return true;
        }

    };

}
