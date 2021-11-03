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
// Created by zzy on 2019-01-21.
//

//
// Critical fix:
// 1. Remove broadcast_self_last for accept, to fix chosen before self accept.
//    This happens when others reply accept before self get the accept, and then
//    chosen message arrive before self accept. This cause self acceptor ignore the
//    chosen message, which makes proposer stay in final state.
// 2. Reuse the old value when instance not increased, because multi-paxos may fail when
//    same ballot number with different value. This happens when use same ballot number
//    in multi-paxos with bypass prepare, and send accept message which carry different
//    value with same ballot number.
// 3. To prevent missing propose id when unexpected exception, self prepare must success
//    before other operation.
// 4. Don't send idle notify when receive reply and find that we are lag.
//    This may cause idle notify happens before value chosen or learnt notify.
//    Which may cause Cinstance lost the information that rejected proposal value used
//    by others and successfully proposed.
// 5. Proposer final state may block all operations till init_for_new_instance is invoked.
//    It's Cinstance's duty to invoke init_for_new_instance to start a new round when
//    chosen message is received. However, others' propose with higher ballot may cause
//    failure of chosen operation, which may block all operations on myself. So switch to
//    idle when fail on self chosen, and no matter who start a new ballot on this instance,
//    value will not change(this is ensured by paxos), and everything goes on as normal.
// 6. Fix for critical fix 4. Still send idle notify and dealing it in Cinstance.
//    Ignore this notify may cause some missing notify cause we invoke callback outside the lock.
//

#pragma once

#include <cstdint>
#include <set>
#include <atomic>
#include <mutex>
#include <utility>

#include "utility/common_define.h"
#include "utility/time.h"
#include "utility/timer_work_queue.h"
#include "utility/resetable_timeout_notify.h"
#include "utility/hash.h"

#include "type.h"
#include "base.h"
#include "log.h"
#include "communication.h"

namespace zpaxos {

    template<class T>
    class CproposerTimeoutNotify {

    NO_COPY_MOVE(CproposerTimeoutNotify);

    private:

        inline void prepare_timeout_notify() {
            static_cast<T *>(this)->prepare_timeout_notify();
        }

        inline void accept_timeout_notify() {
            static_cast<T *>(this)->accept_timeout_notify();
        }

        class CinternalNotify : public utility::CresetableTimeoutNotify<CinternalNotify> {

        NO_COPY_MOVE(CinternalNotify);

        private:

            CproposerTimeoutNotify &base_;
            bool prepare_;

            friend class utility::CresetableTimeoutNotify<CinternalNotify>;

            void timeout_notify() {
                if (prepare_)
                    base_.prepare_timeout_notify();
                else
                    base_.accept_timeout_notify();
            }

        public:

            CinternalNotify(utility::CtimerWorkQueue &queue, uint32_t timeout_ms,
                            CproposerTimeoutNotify &base, bool prepare)
                    : utility::CresetableTimeoutNotify<CinternalNotify>(queue, timeout_ms),
                      base_(base), prepare_(prepare) {}

        } prepare_, accept_;

    public:

        CproposerTimeoutNotify(utility::CtimerWorkQueue &queue, uint32_t prepare_timeout_ms, uint32_t accept_timeout_ms)
                : prepare_(queue, prepare_timeout_ms, *this, true),
                  accept_(queue, accept_timeout_ms, *this, false) {}

        virtual ~CproposerTimeoutNotify() {
            prepare_.terminate();
            accept_.terminate();
        }

        // Caution: Call this function before destructing to prevent pure virtual function calling.
        virtual void terminate() {
            prepare_.terminate();
            accept_.terminate();
        }

        uint32_t get_prepare_timeout_ms() const {
            return prepare_.get_timeout_ms();
        }

        void reset_prepare() {
            prepare_.reset();
        }

        uint32_t get_accept_timeout_ms() const {
            return accept_.get_timeout_ms();
        }

        void reset_accept() {
            accept_.reset();
        }

    };

    template<class T>
    class Cproposer : private CproposerTimeoutNotify<Cproposer<T>> {

    NO_COPY_MOVE(Cproposer);

    public:

        enum proposer_last_error_e {
            error_success,
            error_prepare_rejected,
            error_prepare_timeout,
            error_accept_rejected,
            error_accept_timeout,
            error_instance_lag
        };

    private:

        // Self prepare before all other operations. Make sure the proposal id be saved.
        inline typename Cbase<T>::routine_status_e self_prepare(const message_t &msg) {
            return static_cast<T *>(this)->self_prepare(msg);
        }

        // Self chosen before all other peers to determine whether switching to idle.
        inline typename Cbase<T>::routine_status_e self_chosen(const message_t &msg) {
            return static_cast<T *>(this)->self_chosen(msg);
        }

        inline void on_proposer_idle() {
            static_cast<T *>(this)->on_proposer_idle();
        }

        inline void on_proposer_final(const instance_id_t &instance_id, value_t &&value) {
            static_cast<T *>(this)->on_proposer_final(instance_id, std::forward<value_t>(value));
        }

        Cbase <T> &base_;

        // Lock to sync state, id and all collections.
        std::mutex lock_;

        instance_id_t working_instance_id_;

        enum proposer_state_e {
            proposer_idle,
            proposer_wait_prepare,
            proposer_wait_accept,
            proposer_final,
        } state_;
        int64_t action_time_;

        proposer_last_error_e last_error_;

        // For prepare/accept.
        bool can_skip_prepare_;
        bool has_rejected_; // This has no effect on correctness, just an optimization of fail fast.

        proposal_id_t highest_other_proposal_id_;
        proposal_id_t now_proposal_id_;

        value_t value_;

        std::set<node_id_t> response_set_;
        std::set<node_id_t> promise_or_accept_set_;
        std::set<node_id_t> reject_set_;
        ballot_number_t highest_ballot_;

        friend class CproposerTimeoutNotify<Cproposer>;

        inline void reset_collections() {
            response_set_.clear();
            promise_or_accept_set_.clear();
            reject_set_.clear();
        }

        inline bool internal_try_init_for_new_instance(const instance_id_t &instance_id) {
            if (working_instance_id_ >= instance_id)
                return false;

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::internal_try_init_for_new_instance.",
                    base_.node_id(), base_.group_id(), base_.instance_id(), instance_id));

            working_instance_id_ = instance_id;
            reset_collections();
            highest_other_proposal_id_ = 0;
            value_.reset();
            state_ = proposer_idle;
            return true;
        }

        void prepare_timeout_notify() {
            {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != proposer_wait_prepare) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::prepare_timeout_notify state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return;
                }

                auto now_time = utility::Ctime::steady_ms();
                if (now_time - action_time_ < CproposerTimeoutNotify<Cproposer>::get_prepare_timeout_ms()) {
                    LOG_WARN(("[N({}) G({}) I({}) i({})] Cproposer::prepare_timeout_notify bad timer.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    CproposerTimeoutNotify<Cproposer>::reset_prepare();
                    return;
                }

                LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::prepare_timeout_notify trigger, back to idle.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                state_ = proposer_idle;
                last_error_ = error_prepare_timeout;
            }

            on_proposer_idle();
        }

        void accept_timeout_notify() {
            {
                std::lock_guard<std::mutex> lck(lock_);

                if (state_ != proposer_wait_accept) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::accept_timeout_notify state mismatch.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return;
                }

                auto now_time = utility::Ctime::steady_ms();
                if (now_time - action_time_ < CproposerTimeoutNotify<Cproposer>::get_accept_timeout_ms()) {
                    LOG_WARN(("[N({}) G({}) I({}) i({})] Cproposer::accept_timeout_notify bad timer.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                    CproposerTimeoutNotify<Cproposer>::reset_accept();
                    return;
                }

                LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::accept_timeout_notify trigger, back to idle.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                state_ = proposer_idle;
                last_error_ = error_accept_timeout;
            }

            on_proposer_idle();
        }

        void prepare(message_t &msg) {
            if (has_rejected_) {
                auto max_id = now_proposal_id_ > highest_other_proposal_id_ ? now_proposal_id_
                                                                            : highest_other_proposal_id_;
                now_proposal_id_ = max_id + 1;

                LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::prepare use new proposal_id({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                        now_proposal_id_));
            }

            msg.type = message_t::prepare;
            msg.group_id = base_.group_id();
            msg.instance_id = working_instance_id_;
            msg.node_id = base_.node_id();
            msg.proposal_id = now_proposal_id_;
            msg.ballot.proposal_id = now_proposal_id_;
            msg.ballot.node_id = base_.node_id();

            reset_collections();
            highest_ballot_.reset();

            can_skip_prepare_ = false;
            has_rejected_ = false;

            state_ = proposer_wait_prepare;
            action_time_ = utility::Ctime::steady_ms();

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::prepare ({},{}).",
                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                    msg.ballot.node_id, msg.ballot.proposal_id));
        }

        void accept(message_t &msg) {
            msg.type = message_t::accept;
            msg.group_id = base_.group_id();
            msg.instance_id = working_instance_id_;
            msg.node_id = base_.node_id();
            msg.proposal_id = now_proposal_id_;
            msg.ballot.proposal_id = now_proposal_id_;
            msg.ballot.node_id = base_.node_id();
            msg.value = value_;

            reset_collections();

            state_ = proposer_wait_accept;
            action_time_ = utility::Ctime::steady_ms();

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::accept ({},{},{}-{:x}).",
                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                    msg.ballot.node_id, msg.ballot.proposal_id,
                    value_.state_machine_id, utility::Chash::crc32(value_.buffer.slice())));
        }

        void chosen(message_t &msg) {
            msg.type = message_t::value_chosen;
            msg.group_id = base_.group_id();
            msg.instance_id = working_instance_id_;
            msg.node_id = base_.node_id();
            msg.proposal_id = now_proposal_id_;
            msg.ballot.proposal_id = now_proposal_id_;
            msg.ballot.node_id = base_.node_id();

            state_ = proposer_final;

            LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::chosen ({},{}).",
                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                    msg.ballot.node_id, msg.ballot.proposal_id));
        }

        void record_other_proposal_id(const message_t &msg) {
            if (msg.ballot && msg.ballot.proposal_id > highest_other_proposal_id_) {
                highest_other_proposal_id_ = msg.ballot.proposal_id;

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::record_other_proposal_id {}.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                        highest_other_proposal_id_));
            }
        }

        // Caution: This will move value to local variable, and never touch it again.
        void update_ballot_and_value(message_t &&msg) {
            if (msg.ballot && msg.ballot > highest_ballot_ && msg.value) {
                highest_ballot_ = msg.ballot;
                value_ = std::move(msg.value);

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::update_ballot_and_value to ({},{},{}-{:x}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                        highest_ballot_.node_id, highest_ballot_.proposal_id,
                        value_.state_machine_id, utility::Chash::crc32(value_.buffer.slice())));
            }
        }

    protected:

        typename Cbase<T>::routine_status_e on_prepare_reply(message_t &&msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Cproposer::on_prepare_reply group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t accept_msg;
            auto status_ret = Cbase<T>::routine_success;
            auto notify_idle = false;

            switch (msg.type) {
                case message_t::prepare_promise:
                case message_t::prepare_reject: {
                    std::lock_guard<std::mutex> lck(lock_);

                    // Check working instance lag.
                    auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                    if (working_instance_id_ < now_instance_id) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply lag.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        // Is reject?
                        if ((msg.instance_id == working_instance_id_ ||
                             msg.instance_id + 1 == working_instance_id_) &&
                            message_t::prepare_reject == msg.type) {
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply lag and reject message.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                            has_rejected_ = true;
                            record_other_proposal_id(msg);
                        }

                        // Auto init for new instance.
                        // Still send idle notify. Mentioned in critical fix 4 & 6.
                        notify_idle = internal_try_init_for_new_instance(now_instance_id);
                        last_error_ = error_instance_lag;
                        status_ret = Cbase<T>::routine_error_lag;
                        break;
                    }

                    if (msg.instance_id != working_instance_id_) {
                        if (msg.instance_id > working_instance_id_) {
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply need learn, msg({}).",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                    msg.instance_id));
                            return Cbase<T>::routine_error_need_learn;
                        } else if (msg.instance_id + 1 == working_instance_id_) {
                            // Expired message.
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply expired message.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                            if (message_t::prepare_reject == msg.type) {
                                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply expired reject.",
                                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                                has_rejected_ = true;
                                record_other_proposal_id(msg);
                            }
                        } else
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply inst mismatch, msg({}).",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                    msg.instance_id));
                        return Cbase<T>::routine_error_instance;
                    }

                    if (state_ != proposer_wait_prepare) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply state mismatch.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                        return Cbase<T>::routine_error_state;
                    }

                    if (msg.proposal_id != now_proposal_id_) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply id mismatch, my({}) msg({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                now_proposal_id_, msg.proposal_id));
                        return Cbase<T>::routine_error_proposal;
                    }

                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply receive one {} ({},{},{}-{:x}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            message_t::prepare_promise == msg.type ? "promise" : "reject",
                            msg.ballot.node_id, msg.ballot.proposal_id,
                            msg.value.state_machine_id, utility::Chash::crc32(msg.value.buffer.slice())));

                    response_set_.insert(msg.node_id);

                    if (message_t::prepare_promise == msg.type) {
                        // Promise.
                        promise_or_accept_set_.insert(msg.node_id);
                        // Caution: This will move value to local variable, and never touch it again.
                        update_ballot_and_value(std::forward<message_t>(msg));
                    } else {
                        // Reject.
                        reject_set_.insert(msg.node_id);
                        has_rejected_ = true;
                        record_other_proposal_id(msg);
                    }

                    if (base_.is_quorum(working_instance_id_, promise_or_accept_set_)) {
                        // Prepare success.
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply prepare success.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        can_skip_prepare_ = true;
                        accept(accept_msg);
                    } else if (base_.is_quorum(working_instance_id_, reject_set_) ||
                               base_.is_all_voter(working_instance_id_, response_set_)) {
                        // Prepare fail.
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply prepare fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        state_ = proposer_idle;
                        last_error_ = error_prepare_rejected;
                        notify_idle = true;
                    }
                }
                    break;

                default:
                    LOG_ERROR(("[N({}) G({}) I({})] Cproposer::on_prepare_reply error msg type.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return Cbase<T>::routine_error_type;
            }

            if (accept_msg) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply accept broadcast.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), accept_msg.instance_id));

                auto bret = base_.broadcast(accept_msg,
                                            Ccommunication::broadcast_voter,
                                            Ccommunication::broadcast_self_first);
                CproposerTimeoutNotify<Cproposer>::reset_accept();
                if (!bret) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Cproposer::on_prepare_reply accept broadcast fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), accept_msg.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            } else if (notify_idle)
                on_proposer_idle();

            return status_ret;
        }

        typename Cbase<T>::routine_status_e on_accept_reply(const message_t &msg) {
            // Check group first.
            if (msg.group_id != base_.group_id()) {
                LOG_ERROR(("[N({}) G({}) I({})] Cproposer::on_accept_reply group id mismatch, msg({}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.group_id));
                return Cbase<T>::routine_error_group;
            }

            message_t chosen_msg;
            value_t chosen_value;
            auto status_ret = Cbase<T>::routine_success;
            auto notify_idle = false;

            switch (msg.type) {
                case message_t::accept_accept:
                case message_t::accept_reject: {
                    std::lock_guard<std::mutex> lck(lock_);

                    // Check working instance lag.
                    auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
                    if (working_instance_id_ < now_instance_id) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply lag.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        // Is reject?
                        if ((msg.instance_id == working_instance_id_ ||
                             msg.instance_id + 1 == working_instance_id_) &&
                            message_t::accept_reject == msg.type) {
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply lag and reject message.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                            has_rejected_ = true;
                            record_other_proposal_id(msg);
                        }

                        // Auto init for new instance.
                        // Still send idle notify. Mentioned in critical fix 4 & 6.
                        notify_idle = internal_try_init_for_new_instance(now_instance_id);
                        last_error_ = error_instance_lag;
                        status_ret = Cbase<T>::routine_error_lag;
                        break;
                    }

                    if (msg.instance_id != working_instance_id_) {
                        if (msg.instance_id > working_instance_id_) {
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply need learn, msg({}).",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                    msg.instance_id));
                            return Cbase<T>::routine_error_need_learn;
                        } else if (msg.instance_id + 1 == working_instance_id_) {
                            // Expired message.
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply expired message.",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                            if (message_t::accept_reject == msg.type) {
                                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply expired reject.",
                                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                                has_rejected_ = true;
                                record_other_proposal_id(msg);
                            }
                        } else
                            LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply inst mismatch, msg({}).",
                                    base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                    msg.instance_id));
                        return Cbase<T>::routine_error_instance;
                    }

                    if (state_ != proposer_wait_accept) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply state mismatch.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                        return Cbase<T>::routine_error_state;
                    }

                    if (msg.proposal_id != now_proposal_id_) {
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply id mismatch, my({}) msg({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                                now_proposal_id_, msg.proposal_id));
                        return Cbase<T>::routine_error_proposal;
                    }

                    LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply receive one {}.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            message_t::accept_accept == msg.type ? "accept" : "reject"));

                    response_set_.insert(msg.node_id);

                    if (message_t::accept_accept == msg.type) {
                        // Accept.
                        promise_or_accept_set_.insert(msg.node_id);
                    } else {
                        // Reject.
                        reject_set_.insert(msg.node_id);
                        has_rejected_ = true;
                        record_other_proposal_id(msg);
                    }

                    if (base_.is_quorum(working_instance_id_, promise_or_accept_set_)) {
                        // Accept success.
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply accept success.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        chosen(chosen_msg);
                        chosen_value = value_;
                    } else if (base_.is_quorum(working_instance_id_, reject_set_) ||
                               base_.is_all_voter(working_instance_id_, response_set_)) {
                        // Accept fail.
                        LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply accept fail.",
                                base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));

                        state_ = proposer_idle;
                        last_error_ = error_accept_rejected;
                        notify_idle = true;
                    }
                }
                    break;

                default:
                    LOG_ERROR(("[N({}) G({}) I({})] Cproposer::on_accept_reply error msg type.",
                            base_.node_id(), base_.group_id(), base_.instance_id()));
                    return Cbase<T>::routine_error_type;
            }

            if (chosen_msg) {
                on_proposer_final(chosen_msg.instance_id, std::move(chosen_value));

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply chosen broadcast.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), chosen_msg.instance_id));

                if (!base_.broadcast(chosen_msg, Ccommunication::broadcast_voter, Ccommunication::broadcast_no_self)) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Cproposer::on_accept_reply chosen broadcast fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), chosen_msg.instance_id));
                    status_ret = Cbase<T>::routine_fatal_error;
                }

                auto status = self_chosen(chosen_msg);
                if (status != Cbase<T>::routine_success) {
                    // Fail to self chosen. So switch to idle if we still at final. Mentioned in critical fix 5.
                    std::lock_guard<std::mutex> lck(lock_);
                    if (working_instance_id_ == chosen_msg.instance_id && proposer_final == state_) {
                        state_ = proposer_idle;
                        notify_idle = true;
                    }
                }
            }

            if (notify_idle)
                on_proposer_idle();

            return status_ret;
        }

    public:

        Cproposer(Cbase <T> &base, utility::CtimerWorkQueue &timer_work_queue,
                  uint32_t prepare_timeout_ms = 1000, uint32_t accept_timeout_ms = 500)
                : base_(base),
                  CproposerTimeoutNotify<Cproposer>(timer_work_queue, prepare_timeout_ms, accept_timeout_ms),
                  working_instance_id_(0), state_(proposer_idle), action_time_(0), last_error_(error_success),
                  can_skip_prepare_(false), has_rejected_(false),
                  highest_other_proposal_id_(0), now_proposal_id_(1) {}

        ~Cproposer() override {
            CproposerTimeoutNotify<Cproposer>::terminate();
        }

        // Caution: Call this function before destructing to prevent pure virtual function calling.
        void terminate() final {
            CproposerTimeoutNotify<Cproposer>::terminate();
        }

        void set_start_propose_id(const proposal_id_t &propose_id) {
            std::lock_guard<std::mutex> lck(lock_);
            if (propose_id > now_proposal_id_)
                now_proposal_id_ = propose_id;
        }

        bool init_for_new_instance() {
            std::lock_guard<std::mutex> lck(lock_);
            auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);
            return internal_try_init_for_new_instance(now_instance_id);
        }

        instance_id_t get_working_instance_id() {
            std::lock_guard<std::mutex> lck(lock_);
            return working_instance_id_;
        }

        bool is_idle() {
            std::lock_guard<std::mutex> lck(lock_);
            return proposer_idle == state_;
        }

        // Return the error of last **successfully** call of propose.
        proposer_last_error_e last_error() {
            std::lock_guard<std::mutex> lck(lock_);
            return last_error_;
        }

        typename Cbase<T>::routine_status_e propose(const instance_id_t &expected_instance_id, const value_t &value) {
            message_t msg;

            {
                std::lock_guard<std::mutex> lck(lock_);

                auto now_instance_id = base_.instance_id().load(std::memory_order_acquire);

                internal_try_init_for_new_instance(now_instance_id);

                if (state_ != proposer_idle) {
                    LOG_INFO(("[N({}) G({}) I({})] Cproposer::propose state mismatch, now({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), state_));
                    return Cbase<T>::routine_error_state;
                }

                if (working_instance_id_ != expected_instance_id) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::propose expected inst mismatch, exp({}).",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                            expected_instance_id));
                    return Cbase<T>::routine_error_instance;
                }

                if (!base_.is_voter(working_instance_id_)) {
                    LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::propose not voter, deny.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_));
                    return Cbase<T>::routine_error_role;
                }

                //
                // Caution: Must use same value when same ballot.
                // When no reject message, the ballot number will not increase.
                // So we should not change the value when it has proposed.
                //
                if (!value_)
                    value_ = value;

                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::propose value({}-{:x}) actual({}-{:x}).",
                        base_.node_id(), base_.group_id(), base_.instance_id(), working_instance_id_,
                        value.state_machine_id, utility::Chash::crc32(value.buffer.slice()),
                        value_.state_machine_id, utility::Chash::crc32(value_.buffer.slice())));

                if (can_skip_prepare_ && !has_rejected_) {
                    // Direct accept.
                    accept(msg);
                } else {
                    // Prepare.
                    prepare(msg);
                }

                last_error_ = error_success;
            }

            if (msg) {
                LOG_TRACE(("[N({}) G({}) I({}) i({})] Cproposer::propose message broadcast.",
                        base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));

                auto prepare = message_t::prepare == msg.type;
                if (prepare) {
                    auto status = self_prepare(msg);
                    if (status != Cbase<T>::routine_success) {
                        std::lock_guard<std::mutex> lck(lock_);
                        if (working_instance_id_ == msg.instance_id && proposer_wait_prepare == state_)
                            state_ = proposer_idle;
                        LOG_INFO(("[N({}) G({}) I({}) i({})] Cproposer::propose call self_prepare fail({}).",
                                base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id,
                                status));
                        return status;
                    }
                }

                auto bret = base_.broadcast(msg, Ccommunication::broadcast_voter,
                                            prepare ? Ccommunication::broadcast_no_self
                                                    : Ccommunication::broadcast_self_first);
                if (prepare)
                    CproposerTimeoutNotify<Cproposer>::reset_prepare();
                else
                    CproposerTimeoutNotify<Cproposer>::reset_accept();
                if (!bret) {
                    LOG_ERROR(("[N({}) G({}) I({}) i({})] Cproposer::propose message broadcast fail.",
                            base_.node_id(), base_.group_id(), base_.instance_id(), msg.instance_id));
                    return Cbase<T>::routine_fatal_error;
                }
            }

            return Cbase<T>::routine_success;
        }

        void cancel_skip_prepare() {
            std::lock_guard<std::mutex> lck(lock_);
            can_skip_prepare_ = false;
        }

    };

}
