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
// Created by zzy on 2019-01-30.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <set>
#include <atomic>
#include <mutex>

#include "utility/common_define.h"
#include "utility/time.h"
#include "utility/timer_work_queue.h"
#include "utility/resetable_timeout_notify.h"
#include "utility/hash.h"

#include "type.h"
#include "base.h"
#include "log.h"
#include "communication.h"
#include "state_machine.h"

namespace zpaxos {

    template<class T>
    class ClearnerTimeoutNotify {

    NO_COPY_MOVE(ClearnerTimeoutNotify);

    private:

        inline void ping_timeout_notify() {
            static_cast<T *>(this)->ping_timeout_notify();
        }

        inline void learn_timeout_notify() {
            static_cast<T *>(this)->learn_timeout_notify();
        }

        class CinternalNotify : public utility::CresetableTimeoutNotify<CinternalNotify> {

        NO_COPY_MOVE(CinternalNotify);

        private:

            ClearnerTimeoutNotify &base_;
            bool ping_;

            friend class utility::CresetableTimeoutNotify<CinternalNotify>;

            void timeout_notify() {
                if (ping_)
                    base_.ping_timeout_notify();
                else
                    base_.learn_timeout_notify();
            }

        public:

            CinternalNotify(utility::CtimerWorkQueue &queue, uint32_t timeout_ms,
                            ClearnerTimeoutNotify &base, bool ping)
                    : utility::CresetableTimeoutNotify<CinternalNotify>(queue, timeout_ms),
                      base_(base), ping_(ping) {}

        } ping_, learn_;

    public:

        ClearnerTimeoutNotify(utility::CtimerWorkQueue &queue, uint32_t ping_timeout_ms, uint32_t learn_timeout_ms)
                : ping_(queue, ping_timeout_ms, *this, true), learn_(queue, learn_timeout_ms, *this, false) {}

        virtual ~ClearnerTimeoutNotify() {
            ping_.terminate();
            learn_.terminate();
        }

        // Caution: Call this function before destructing to prevent pure virtual function calling.
        virtual void terminate() {
            ping_.terminate();
            learn_.terminate();
        }

        uint32_t get_ping_timeout_ms() const {
            return ping_.get_timeout_ms();
        }

        void reset_ping() {
            ping_.reset();
        }

        uint32_t get_learn_timeout_ms() const {
            return learn_.get_timeout_ms();
        }

        void reset_learn() {
            learn_.reset();
        }

    };

    template<class T>
    class Clearner : private ClearnerTimeoutNotify<Clearner<T>> {

    NO_COPY_MOVE(Clearner);

    private:

        inline void value_learnt(const instance_id_t &instance_id, value_t &&value) {
            static_cast<T *>(this)->value_learnt(instance_id, std::forward<value_t>(value));
        }

        inline void learn_done() {
            static_cast<T *>(this)->learn_done();
        }

        // Snapshot's next_instance should larger than peer_instance_id or peer will learn nothing.
        inline typename Cbase<T>::routine_status_e take_snapshots(
                const instance_id_t &peer_instance_id, std::vector<Csnapshot::shared_ptr> &snapshots) {
            return static_cast<T *>(this)->take_snapshots(peer_instance_id, snapshots);
        }

        inline typename Cbase<T>::routine_status_e load_snapshots(const std::vector<Csnapshot::shared_ptr> &snapshots) {
            return static_cast<T *>(this)->load_snapshots(snapshots);
        }

        Cbase <T> &base_;

        const size_t max_learn_batch_size_;

        std::mutex lock_;

        enum learner_state_e {
            learner_idle,
            learner_wait_pong,
            learner_wait_learn,
            learner_learning
        } state_;
        int64_t action_time_;

        instance_id_t ping_instance_id_;
        std::set<node_id_t> pong_set_;
        node_id_t newest_node_;
        instance_id_t newest_instance_;
        node_id_t newest_node_with_log_;
        instance_id_t newest_instance_with_log_;

        std::atomic<bool> working_;

        class CscopeMarker final {

        NO_COPY_MOVE(CscopeMarker);

        private:

            std::atomic<bool> &marker_;

        public:

            explicit CscopeMarker(std::atomic<bool> &marker)
                    : marker_(marker) {
                marker_.store(true, std::memory_order_release);
            }

            ~CscopeMarker() {
                marker_.store(false, std::memory_order_release);
            }

        };

        friend class ClearnerTimeoutNotify<Clearner>;

        // Should call outside of scope lock.
        static void pack_log(const instance_id_t &start_instance_id, std::vector<state_t> &&states, message_t &msg) {
            msg.learn_batch.reserve(states.size());

            size_t offset = 0;
            for (auto &state : states) {
                msg.learn_batch.emplace_back(start_instance_id + offset, state.accepted, std::move(state.value));
                ++offset;
            }
        }

        // Can call outside of scope lock. Call with lock if need check state later.
        // Caution: This will move learn_t out from this msg, and never touch it again.
        bool load_by_log(message_t &&msg) {
            if (msg.learn_batch.empty())
                return true;

            auto pack_first_instance = msg.learn_batch.front().instance_id;
            auto pack_size = msg.learn_batch.size();

            // Check pack.
            for (auto idx = 0; idx < pack_size; ++idx) {
                const auto &learn = msg.learn_batch[idx];
                if (learn.instance_id != pack_first_instance + idx || !learn.accepted || !learn.value) {
                    LOG_ERROR(("[N({}) G({}) I({})] Clearner::load_by_log bad learn batch.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return false;
                }
            }

            auto now_instance = base_.instance_id().load(std::memory_order_acquire);
            std::vector<state_t> states;
            auto learn_start_instance = now_instance;

            while (now_instance >= pack_first_instance && now_instance < pack_first_instance + pack_size) {
                // Can learn.
                if (states.empty()) {
                    // First move.
                    states.reserve(pack_first_instance + pack_size - now_instance);
                    for (auto idx = now_instance - pack_first_instance; idx < pack_size; ++idx) {
                        auto &learn = msg.learn_batch[idx];
                        states.emplace_back(learn.accepted, learn.accepted, std::move(learn.value));
                    }
                    // learn_start_instance already set to now_instance.
                } else {
                    // Cut for new now inst id.
                    assert(now_instance > learn_start_instance);
                    states.erase(states.begin(), states.begin() + (now_instance - learn_start_instance));
                    learn_start_instance = now_instance;
                }

                // Batch put.
                auto lag = false;
                if (!base_.put_and_next_instance(learn_start_instance, states, lag)) {
                    if (lag) {
                        LOG_WARN(("[N({}) G({}) I({}) i({})] Clearner::load_by_log write lag.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), learn_start_instance));

                        // Reload now inst id and try again.
                        now_instance = base_.instance_id().load(std::memory_order_acquire);
                        continue;
                    }

                    // Bad write.
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::load_by_log write fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), learn_start_instance));
                    return false;
                }

                // Write success.
                LOG_TRACE(("[N({}) G({}) I({})] Clearner::load_by_log write inst[{},{}) success.",
                        base_.node_id(), base_.group_id(), base_.instance_id(),
                        learn_start_instance, pack_first_instance + pack_size));

                for (auto idx = 0; idx < states.size(); ++idx) {
                    LOG_TRACE(("[N({}) G({}) I({})] Clearner::load_by_log value_learnt for inst({}) value({}-{:x}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(),
                            learn_start_instance + idx,
                            states[idx].value.state_machine_id,
                            utility::Chash::crc32(states[idx].value.buffer.slice())));

                    value_learnt(learn_start_instance + idx, std::move(states[idx].value));
                }
                learn_done();
                return true;
            }
            return true; // Can't learn, so just ignore.
        }

        // Should call with lock.
        void ping(const instance_id_t &instance_id, message_t &msg) {
            msg.type = message_t::learn_ping;
            msg.group_id = base_.group_id();
            msg.instance_id = instance_id;
            msg.node_id = base_.node_id();

            ping_instance_id_ = instance_id;
            pong_set_.clear();
            pong_set_.insert(base_.node_id()); // Put myself first.
            newest_node_ = 0;
            newest_instance_ = instance_id;
            newest_node_with_log_ = 0;
            newest_instance_with_log_ = instance_id;

            state_ = learner_wait_pong;
            action_time_ = utility::Ctime::steady_ms();

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::ping.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), instance_id));
        }

        // Should call outside of scope lock.
        void pong(const instance_id_t &instance_id, message_t &msg) {
            msg.type = message_t::learn_pong;
            msg.group_id = base_.group_id();
            msg.instance_id = instance_id;
            msg.node_id = base_.node_id();

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::pong.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), instance_id));
        }

        // Should call with lock.
        void learn(const instance_id_t &instance_id, message_t &msg) {
            msg.type = message_t::learn_request;
            msg.group_id = base_.group_id();
            msg.instance_id = instance_id;
            msg.node_id = base_.node_id();

            state_ = learner_wait_learn;
            action_time_ = utility::Ctime::steady_ms();

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::learn request.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), instance_id));
        }

        // Should call with lock.
        bool select_and_learn(node_id_t &target_id, message_t &msg) {
            auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
            if (newest_node_with_log_ != 0 && newest_instance_with_log_ > now_instance_id) {
                // Catch up with log first.
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::select_and_learn find {}@{} can learn with log.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id,
                        newest_node_with_log_, newest_instance_with_log_));

                target_id = newest_node_with_log_;
                learn(now_instance_id, msg);
                return true;
            } else if (newest_node_ != 0 && newest_instance_ > now_instance_id) {
                // Or just learn from newest node.
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::select_and_learn find {}@{} can learn with snapshot.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id,
                        newest_node_, newest_instance_));

                target_id = newest_node_;
                learn(now_instance_id, msg);
                return true;
            }

            // No one to learn.
            LOG_INFO(("[N({}) G({}) I({}) i({})] Clearner::select_and_learn no one newer, back to idle.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id));

            target_id = 0;
            state_ = learner_idle;
            return false;
        }

        void ping_timeout_notify() {
            message_t learn_msg;
            node_id_t target_id = 0;

            {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != learner_wait_pong) {
                    LOG_INFO(("[N({}) G({}) I({})] Clearner::ping_timeout_notify state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return;
                }

                auto now_time = utility::Ctime::steady_ms();
                if (now_time - action_time_ < ClearnerTimeoutNotify<Clearner>::get_ping_timeout_ms()) {
                    LOG_WARN(("[N({}) G({}) I({})] Clearner::ping_timeout_notify bad timer.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));

                    ClearnerTimeoutNotify<Clearner>::reset_ping();
                    return;
                }

                select_and_learn(target_id, learn_msg);
            }

            if (learn_msg) {
                LOG_TRACE(("[N({}) G({}) I({})] Clearner::ping_timeout_notify learn request send to {}.",
                        base_.node_id(), base_.group_id(), learn_msg.instance_id, target_id));

                if (base_.send(target_id, learn_msg))
                    ClearnerTimeoutNotify<Clearner>::reset_learn();
                else
                    LOG_ERROR(("[N({}) G({}) I({})] Clearner::ping_timeout_notify learn request send fail.",
                            base_.node_id(), base_.group_id(), learn_msg.instance_id));
            }
        }

        void learn_timeout_notify() {
            std::lock_guard<std::mutex> lck(lock_);

            if (state_ != learner_wait_learn) {
                LOG_INFO(("[N({}) G({}) I({})] Clearner::learn_timeout_notify state mismatch.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return;
            }

            auto now_time = utility::Ctime::steady_ms();
            if (now_time - action_time_ < ClearnerTimeoutNotify<Clearner>::get_learn_timeout_ms()) {
                LOG_WARN(("[N({}) G({}) I({})] Clearner::learn_timeout_notify bad timer.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));

                ClearnerTimeoutNotify<Clearner>::reset_learn();
                return;
            }

            LOG_TRACE(("[N({}) G({}) I({})] Clearner::ping_timeout_notify trigger, back to idle.",
                    base_.node_id(), base_.group_id(), base_.instance_id()));

            state_ = learner_idle;
        }

        // Should call outside of scope lock.
        typename Cbase<T>::routine_status_e send_learn_response(const node_id_t &peer_node_id,
                                                                const instance_id_t &peer_instance_id,
                                                                const instance_id_t &my_instance_id,
                                                                bool send_snapshot) {
            if (my_instance_id > peer_instance_id) {
                CscopeMarker marker(working_); // Mark as working.

                auto pack_number = my_instance_id - peer_instance_id > max_learn_batch_size_ ?
                                   max_learn_batch_size_ :
                                   static_cast<size_t>(my_instance_id - peer_instance_id);

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response pack[{},{}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id,
                        peer_instance_id, peer_instance_id + pack_number));

                message_t learn_response;
                learn_response.type = message_t::learn_response;
                learn_response.group_id = base_.group_id();
                learn_response.instance_id = my_instance_id;
                learn_response.node_id = base_.node_id();

                // Read old state.
                std::vector<state_t> states(pack_number);
                if (base_.get(peer_instance_id, states)) {
                    // Pack values.
                    pack_log(peer_instance_id, std::move(states), learn_response);
                } else {
                    // Compacted?
                    instance_id_t min_instance_id;
                    auto iret = base_.get_min_instance_id(min_instance_id);
                    if (iret < 0) {
                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response get min inst fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id));
                        return Cbase<T>::routine_read_fail;
                    } else if (iret > 0)
                        min_instance_id = 0;

                    if (peer_instance_id >= min_instance_id) {
                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response read inst:[{},{}) fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id,
                                peer_instance_id, peer_instance_id + states.size()));
                        return Cbase<T>::routine_read_fail;
                    }

                    if (!send_snapshot)
                        return Cbase<T>::routine_read_fail;

                    // Send snapshot.
                    auto ret = take_snapshots(peer_instance_id, learn_response.snapshot_batch);
                    if (ret != Cbase<T>::routine_success) {
                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response get snapshots fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id));
                        return ret;
                    }
                }

                // Send to peer.
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response send learn response to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id, peer_node_id));

                if (!base_.send(peer_node_id, learn_response)) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::send_learn_response learn response send fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), my_instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e set_peer_info(message_t &msg) {
            // Set overload.
            msg.overload = working_.load(std::memory_order_acquire);

            // Set min stored instance.
            auto iret = base_.get_min_instance_id(msg.min_stored_instance_id);
            if (iret < 0) {
                LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::set_peer_info get min inst fail.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));
                return Cbase<T>::routine_read_fail;
            } else if (iret > 0)
                msg.min_stored_instance_id = 0;

            return Cbase<T>::routine_success;
        }

    protected:

        typename Cbase<T>::routine_status_e on_ping(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_ping group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t pong_msg;

            if (message_t::learn_ping == msg.type) {
                // No lock needed.

                auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                if (msg.instance_id > now_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Clearner::on_ping need learn, msg({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id,
                            msg.instance_id));
                    return Cbase<T>::routine_error_need_learn;
                }

                // Set peer info(min inst & overload) first before update state.
                auto ret = set_peer_info(pong_msg);
                if (ret != Cbase<T>::routine_success)
                    return ret;

                pong(now_instance_id, pong_msg);
            } else {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_ping error msg type.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return Cbase<T>::routine_error_type;
            }

            if (pong_msg) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::on_ping send pong to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), pong_msg.instance_id,
                        msg.node_id));

                if (!base_.send(msg.node_id, pong_msg)) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::on_ping pong send fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), pong_msg.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e on_pong(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_pong group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t learn_msg;
            node_id_t target_id = 0;

            if (message_t::learn_pong == msg.type) {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != learner_wait_pong) {
                    LOG_INFO(("[N({}) G({}) I({})] Clearner::on_pong state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return Cbase<T>::routine_error_state;
                }

                LOG_TRACE(("[N({}) G({}) I({})] Clearner::on_pong receive one pong, {}@{}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(),
                        msg.node_id, msg.instance_id));

                pong_set_.insert(msg.node_id);

                // Record as learn target when node is not overload.
                if (!msg.overload) {
                    if (msg.instance_id > newest_instance_) {
                        LOG_TRACE(("[N({}) G({}) I({})] Clearner::on_pong record newest {}@{}.",
                                base_.node_id(), base_.group_id(), base_.instance_id(),
                                msg.node_id, msg.instance_id));

                        newest_node_ = msg.node_id, newest_instance_ = msg.instance_id;
                    }

                    auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                    if (msg.min_stored_instance_id <= now_instance_id && msg.instance_id > newest_instance_with_log_) {
                        LOG_TRACE(("[N({}) G({}) I({})] Clearner::on_pong record newest with log {}@{}.",
                                base_.node_id(), base_.group_id(), base_.instance_id(),
                                msg.node_id, msg.instance_id));

                        newest_node_with_log_ = msg.node_id, newest_instance_with_log_ = msg.instance_id;
                    }
                }

                // pong_set_ already include myself.
                if (base_.is_all_peer(ping_instance_id_, pong_set_))
                    select_and_learn(target_id, learn_msg);
            } else {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_pong error msg type.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return Cbase<T>::routine_error_type;
            }

            if (learn_msg) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::on_pong send learn request to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), learn_msg.instance_id,
                        msg.node_id));

                if (base_.send(target_id, learn_msg))
                    ClearnerTimeoutNotify<Clearner>::reset_learn();
                else {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::on_ping learn request send fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), learn_msg.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e on_learn(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_learn group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            instance_id_t now_instance_id = 0;

            // Get my now instance.
            if (message_t::learn_request == msg.type) {
                // No lock needed.

                now_instance_id = base_.instance_id();
                if (msg.instance_id > now_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Clearner::on_learn need learn, msg({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id,
                            msg.instance_id));
                    return Cbase<T>::routine_error_need_learn;
                }
            } else {
                LOG_ERROR(("[N({}) G({}) I(?)] Clearner::on_learn error msg type.",
                        base_.node_id(), base_.group_id()));
                return Cbase<T>::routine_error_type;
            }

            return send_learn_response(msg.node_id, msg.instance_id, now_instance_id, true);
        }

        typename Cbase<T>::routine_status_e on_learn_response(message_t &&msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_learn_response group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t learn_msg;

            if (message_t::learn_response == msg.type) {
                {
                    std::lock_guard<std::mutex> lck(lock_);

                    // Ignore the state check, learn any time.
                    LOG_TRACE(("[N({}) G({}) I({})] Clearner::on_learn_response receive one pack.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));

                    if (learner_wait_learn == state_)
                        state_ = learner_learning; // Switch to learning.
                }

                typename Cbase<T>::routine_status_e ret;
                {
                    // Learning outside the lock.
                    CscopeMarker marker(working_); // Mark as working.

                    LOG_TRACE(("[N({}) G({}) I({})] Clearner::on_learn_response start learn, "
                               "snapshot size:{}, log size:{}.",
                            base_.node_id(), base_.group_id(), base_.instance_id(),
                            msg.snapshot_batch.size(), msg.learn_batch.size()));

                    // Load snapshot first.
                    ret = load_snapshots(msg.snapshot_batch);
                    if (Cbase<T>::routine_success == ret) {
                        // This will move learn_t out from this msg, and never touch it again.
                        if (!load_by_log(std::forward<message_t>(msg)))
                            ret = Cbase<T>::routine_write_fail;
                    }
                }

                {
                    // Lock again and switch state.
                    std::lock_guard<std::mutex> lck(lock_);

                    if (learner_learning == state_) {
                        if (Cbase<T>::routine_success == ret) {
                            // Check whether instance catch up the newest.
                            auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                            if (msg.instance_id > now_instance_id) {
                                // Learn next.
                                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::on_learn_response "
                                           "continue learn, msg({}).",
                                        base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id,
                                        msg.instance_id));

                                learn(now_instance_id, learn_msg);
                            } else {
                                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::on_learn_response catch up, turn idle.",
                                        base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id));

                                state_ = learner_idle;
                            }
                        } else {
                            LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_learn_response learn fail {}, turn idle.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), ret));

                            state_ = learner_idle;
                            return ret;
                        }
                    } else if (ret != Cbase<T>::routine_success) {
                        LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_learn_response learn fail {}, turn idle.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), ret));
                        return ret;
                    }
                }
            } else {
                LOG_ERROR(("[N({}) G({}) I({})] Clearner::on_learn_response error msg type.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return Cbase<T>::routine_error_type;
            }

            if (learn_msg) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::on_learn_response send learn request to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), learn_msg.instance_id,
                        msg.node_id));

                if (base_.send(msg.node_id, learn_msg))
                    ClearnerTimeoutNotify<Clearner>::reset_learn();
                else {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::on_learn_response learn request send fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), learn_msg.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

    public:

        Clearner(Cbase <T> &base, utility::CtimerWorkQueue &timer_work_queue,
                 size_t max_learn_batch_size = 64, uint32_t ping_timeout_ms = 500, uint32_t learn_timeout_ms = 1000)
                : base_(base),
                  ClearnerTimeoutNotify<Clearner>(timer_work_queue, ping_timeout_ms, learn_timeout_ms),
                  max_learn_batch_size_(max_learn_batch_size), state_(learner_idle), action_time_(0),
                  ping_instance_id_(0),
                  newest_node_(0), newest_instance_(0),
                  newest_node_with_log_(0), newest_instance_with_log_(0),
                  working_(false) {}

        ~Clearner() override {
            ClearnerTimeoutNotify<Clearner>::terminate();
        }

        // Caution: Call this function before destructing to prevent pure virtual function calling.
        void terminate() final {
            ClearnerTimeoutNotify<Clearner>::terminate();
        }

        typename Cbase<T>::routine_status_e ping() {
            message_t msg;

            {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != learner_idle) {
                    LOG_INFO(("[N({}) G({}) I({})] Clearner::ping state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return Cbase<T>::routine_error_state;
                }

                // Set peer info(min inst & overload) first before update state.
                auto ret = set_peer_info(msg);
                if (ret != Cbase<T>::routine_success)
                    return ret;

                ping(base_.instance_id().load(std::memory_order_acquire), msg);
            }

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::ping broadcast ping.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));

            if (base_.broadcast(msg, Ccommunication::broadcast_all, Ccommunication::broadcast_no_self))
                ClearnerTimeoutNotify<Clearner>::reset_ping();
            else {
                LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::ping ping broadcast fail.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));
                return Cbase<T>::routine_fatal_error;
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e learn(const node_id_t &target_node) {
            message_t msg;

            {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != learner_idle) {
                    LOG_INFO(("[N({}) G({}) I({})] Clearner::learn state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return Cbase<T>::routine_error_state;
                }

                learn(base_.instance_id().load(std::memory_order_acquire), msg);
            }

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Clearner::learn send learn request to {}.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id, target_node));

            if (base_.send(target_node, msg))
                ClearnerTimeoutNotify<Clearner>::reset_learn();
            else {
                LOG_ERROR(("[N({}) G({}) I({}) i({})] Clearner::learn learn request send fail.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));
                return Cbase<T>::routine_fatal_error;
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e learn_response(
                const node_id_t &target_node, const instance_id_t &target_instance, bool send_snapshot = false) {
            auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
            if (target_instance > now_instance_id) {
                LOG_INFO(("[N({}) G({}) I({}) i({})] Clearner::learn_response need learn, target({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), now_instance_id, target_instance));
                return Cbase<T>::routine_error_need_learn;
            }

            return send_learn_response(target_node, target_instance, now_instance_id, send_snapshot);
        }

    };

}
