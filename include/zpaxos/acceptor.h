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
// Created by zzy on 2019-01-26.
//

#pragma once

#include <atomic>
#include <mutex>

#include "utility/common_define.h"
#include "utility/hash.h"

#include "type.h"
#include "base.h"
#include "log.h"

namespace zpaxos {

    template<class T>
    class Cacceptor {

    NO_COPY_MOVE(Cacceptor);

    private:

        inline void value_chosen(const instance_id_t &instance_id, value_t &&value) {
            static_cast<T *>(this)->value_chosen(instance_id, std::forward<value_t>(value));
        }

        Cbase <T> &base_;

        // Lock to sync state and buffer.
        std::mutex lock_;

        instance_id_t working_instance_id_;

        state_t state_;

        // Should invoke with lock.
        bool load() {
            auto iret = base_.get_max_instance_id(working_instance_id_);
            base_.set_instance_id(working_instance_id_);
            if (iret != 0) {
                if (iret > 0) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::load storage report empty.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return true;
                }
                LOG_ERROR(("[N({}) G({})] Cacceptor::load storage get_max_instance_id fail.",
                        base_.node_id(), base_.group_id()));
                return false;
            }

            if (!base_.get(working_instance_id_, state_)) {
                LOG_ERROR(("[N({}) G({}) I({}) i({})] Cacceptor::load storage get instance fail.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                return false;
            }

            LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::load instance ok.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
            return true;
        }

        // Should invoke with lock.
        bool persist(bool &lag) {
            return base_.put(working_instance_id_, state_, lag);
        }

    protected:

        typename Cbase<T>::routine_status_e on_prepare(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Cacceptor::on_prepare group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t response;
            response.group_id = base_.group_id();
            response.node_id = base_.node_id();
            response.proposal_id = msg.proposal_id;

            if (message_t::prepare == msg.type) {
                std::lock_guard<std::mutex> lck(lock_);

                // Check working instance lag.
                auto now_instance_id = base_.instance_id().load(std::memory_order_relaxed);
                if (working_instance_id_ < now_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare lag.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    // Init for new instance.
                    working_instance_id_ = now_instance_id;
                    state_.reset_accepted();
                }

                if (msg.instance_id != working_instance_id_) {
                    if (msg.instance_id > working_instance_id_) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare need learn, msg({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                msg.instance_id));
                        return Cbase<T>::routine_error_need_learn;
                    }
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare instance mismatch, msg({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            msg.instance_id));
                    return Cbase<T>::routine_error_instance;
                }

                if (!base_.is_voter(working_instance_id_)) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare not voter, ignore.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return Cbase<T>::routine_error_role;
                }

                response.instance_id = working_instance_id_;

                if (msg.ballot >= state_.promised) {
                    // Promise.
                    response.type = message_t::prepare_promise;
                    if (state_.accepted) {
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare promise"
                                   " ({},{}) with ({},{},{}-{:x}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                msg.ballot.node_id, msg.ballot.proposal_id,
                                state_.accepted.node_id, state_.accepted.proposal_id,
                                state_.value.state_machine_id, utility::Chash::crc32(
                                state_.value.buffer.slice())));

                        response.ballot = state_.accepted;
                        response.value = state_.value;
                    } else
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare promise for ({},{}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                msg.ballot.node_id, msg.ballot.proposal_id));

                    state_.promised = msg.ballot;

                    auto lag = false;
                    if (!persist(lag)) {
                        if (lag) {
                            LOG_WARN(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare write lag.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                            return Cbase<T>::routine_error_lag;
                        }

                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare write fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                        return Cbase<T>::routine_write_fail;
                    }
                } else {
                    // Reject.
                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare reject by promised ({},{}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            state_.promised.node_id, state_.promised.proposal_id));

                    response.type = message_t::prepare_reject;
                    response.ballot = state_.promised;
                }
            } else {
                LOG_ERROR(("[N({}) G({}) I({})] Cacceptor::on_prepare error msg type.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return Cbase<T>::routine_error_type;
            }

            if (response) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare send response to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), response.instance_id,
                        msg.node_id));

                if (!base_.send(msg.node_id, response)) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Cacceptor::on_prepare response send fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), response.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e on_accept(message_t &&msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Cacceptor::on_accept group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t response;
            response.group_id = base_.group_id();
            response.node_id = base_.node_id();
            response.proposal_id = msg.proposal_id;

            if (message_t::accept == msg.type) {
                std::lock_guard<std::mutex> lck(lock_);

                // Check working instance lag.
                auto now_instance_id = base_.instance_id().load(std::memory_order_relaxed);
                if (working_instance_id_ < now_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept lag.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    // Init for new instance.
                    working_instance_id_ = now_instance_id;
                    state_.reset_accepted();
                }

                if (msg.instance_id != working_instance_id_) {
                    if (msg.instance_id > working_instance_id_) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept need learn, msg({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                msg.instance_id));
                        return Cbase<T>::routine_error_need_learn;
                    }
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept instance mismatch, msg({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            msg.instance_id));
                    return Cbase<T>::routine_error_instance;
                }

                if (!base_.is_voter(working_instance_id_)) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept not voter, ignore.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return Cbase<T>::routine_error_role;
                }

                response.instance_id = working_instance_id_;

                if (msg.ballot >= state_.promised) {
                    // Accept.
                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept accept and update to ({},{},{}-{:x}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            msg.ballot.node_id, msg.ballot.proposal_id,
                            msg.value.state_machine_id, utility::Chash::crc32(msg.value.buffer.slice())));

                    response.type = message_t::accept_accept;

                    state_.promised = msg.ballot;
                    state_.accepted = msg.ballot;
                    state_.value = std::move(msg.value); // Move value to local variable.

                    auto lag = false;
                    if (!persist(lag)) {
                        if (lag) {
                            LOG_WARN(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept write lag.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                            return Cbase<T>::routine_error_lag;
                        }

                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept write fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                        return Cbase<T>::routine_write_fail;
                    }
                } else {
                    // Reject.
                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept reject by promised ({},{}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            state_.promised.node_id, state_.promised.proposal_id));

                    response.type = message_t::accept_reject;
                    response.ballot = state_.promised;
                }
            } else {
                LOG_ERROR(("[N({}) G({}) I({})] Cacceptor::on_accept error msg type.",
                        base_.node_id(), base_.group_id(), base_.instance_id()));
                return Cbase<T>::routine_error_type;
            }

            if (response) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept send response to {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), response.instance_id,
                        msg.node_id));

                if (!base_.send(msg.node_id, response)) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Cacceptor::on_accept response fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), response.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        typename Cbase<T>::routine_status_e on_chosen(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Cacceptor::on_chosen group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            instance_id_t chosen_instance_id = 0;
            value_t chosen_value;

            if (message_t::value_chosen == msg.type) {
                std::lock_guard<std::mutex> lck(lock_);

                // Check working instance lag.
                auto now_instance_id = base_.instance_id().load(std::memory_order_relaxed);
                if (working_instance_id_ < now_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen lag.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    // Init for new instance.
                    working_instance_id_ = now_instance_id;
                    state_.reset_accepted();
                }

                if (msg.instance_id != working_instance_id_) {
                    if (msg.instance_id > working_instance_id_) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen need learn, msg({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                msg.instance_id));
                        return Cbase<T>::routine_error_need_learn;
                    }
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen instance mismatch, msg({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            msg.instance_id));
                    return Cbase<T>::routine_error_instance;
                }

                if (!base_.is_voter(working_instance_id_)) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen not voter, ignore.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return Cbase<T>::routine_error_role;
                }

                if (msg.ballot != state_.accepted) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen chosen mismatch, my({},{}) msg({},{}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            state_.accepted.node_id, state_.accepted.proposal_id,
                            msg.ballot.node_id, msg.ballot.proposal_id));
                    return Cbase<T>::routine_error_proposal;
                }

                // Chosen value.
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen chose ({}-{:x}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                        state_.value.state_machine_id, utility::Chash::crc32(state_.value.buffer.slice())));

                if (base_.next_instance(working_instance_id_, state_.value)) {
                    chosen_instance_id = working_instance_id_;
                    chosen_value = state_.value;
                } else {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cacceptor::on_chosen update instance lag.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return Cbase<T>::routine_error_lag;
                }
            } else {
                LOG_ERROR(("[N({}) G({}) I(?)] Cacceptor::on_chosen error msg type.",
                        base_.node_id(), base_.group_id()));
                return Cbase<T>::routine_error_type;
            }

            value_chosen(chosen_instance_id, std::move(chosen_value));
            return Cbase<T>::routine_success;
        }

    public:

        explicit Cacceptor(Cbase <T> &base)
                : base_(base), working_instance_id_(0) {}

        virtual ~Cacceptor() = default;

        bool restore() {
            std::lock_guard<std::mutex> lck(lock_);
            return load();
        }

        proposal_id_t get_latest_proposal_id() {
            std::lock_guard<std::mutex> lck(lock_);
            return state_.promised.proposal_id;
        }

        void init_for_new_instance() {
            std::lock_guard<std::mutex> lck(lock_);

            auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
            if (now_instance_id > working_instance_id_) {
                working_instance_id_ = now_instance_id;

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::init_for_new_instance.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                state_.reset_accepted();
            }
        }

        bool persist_now_state(instance_id_t &persisted_instance_id) {
            std::lock_guard<std::mutex> lck(lock_);

            while (true) {
                auto lag = false;
                if (persist(lag)) {
                    persisted_instance_id = working_instance_id_;
                    return true;
                }
                if (!lag)
                    return false;

                // Lag and reset for new instance.
                auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                if (now_instance_id > working_instance_id_) {
                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cacceptor::persist_now_state lag and switch to new inst.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    working_instance_id_ = now_instance_id;
                    state_.reset_accepted();
                } else
                    return false;
            }
        }

        // Rest working after apply the snapshot, and this will update base instance id too.
        bool reset_working_instance(const instance_id_t &instance_id) {
            std::lock_guard<std::mutex> lck(lock_);

            if (instance_id < working_instance_id_)
                return false;
            else if (instance_id == working_instance_id_) {
                // Just persist and return.
                auto lag = false;
                if (!persist(lag)) {
                    if (!lag)
                        return false; // Write error.
                }
                // Success or lag will ignore.
            } else {
                // instance_id > working_instance_id_.
                auto copy = state_;
                copy.reset_accepted();
                if (!base_.reset_min_instance(instance_id, copy))
                    return false;
                working_instance_id_ = instance_id;
                state_.reset_accepted();
            }
            return true;
        }

    };

}
