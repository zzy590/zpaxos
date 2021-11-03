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
// Created by zzy on 2018-12-29.
//

#pragma once

#include <vector>
#include <atomic>
#include <mutex>
#include <stdexcept>

#include "utility/common_define.h"
#include "utility/buffer.h"

#include "type.h"
#include "storage.h"
#include "communication.h"
#include "peer.h"
#include "state_machine.h"

namespace zpaxos {

    struct ballot_number_t final {

        proposal_id_t proposal_id;
        node_id_t node_id;

        ballot_number_t()
                : proposal_id(0), node_id(0) {}

        ballot_number_t(const proposal_id_t &proposal_id, const node_id_t &node_id)
                : proposal_id(proposal_id), node_id(node_id) {}

        bool operator==(const ballot_number_t &another) const {
            return proposal_id == another.proposal_id && node_id == another.node_id;
        }

        bool operator!=(const ballot_number_t &another) const {
            return proposal_id != another.proposal_id || node_id != another.node_id;
        }

        bool operator>(const ballot_number_t &another) const {
            if (proposal_id == another.proposal_id)
                return node_id > another.node_id;
            else
                return proposal_id > another.proposal_id;
        }

        bool operator>=(const ballot_number_t &another) const {
            if (proposal_id == another.proposal_id)
                return node_id >= another.node_id;
            else
                return proposal_id > another.proposal_id;
        }

        explicit operator bool() const {
            return proposal_id != 0;
        }

        void reset() {
            proposal_id = 0, node_id = 0;
        }

    };

    struct value_t final {

        state_machine_id_t state_machine_id;
        utility::Cbuffer buffer;

        value_t()
                : state_machine_id(0) {}

        value_t(const state_machine_id_t &state_machine_id, const utility::Cslice &slice)
                : state_machine_id(state_machine_id), buffer(slice) {}

        value_t(const state_machine_id_t &state_machine_id, utility::Cbuffer &&slice)
                : state_machine_id(state_machine_id), buffer(std::forward<utility::Cbuffer>(slice)) {}

        explicit operator bool() const {
            return state_machine_id != 0 || buffer;
        }

        void reset() {
            state_machine_id = 0;
            buffer.resize(0);
        }

        bool operator==(const value_t &another) const {
            return state_machine_id == another.state_machine_id && buffer == another.buffer;
        }

    };

    struct state_t final {

        ballot_number_t promised, accepted;
        value_t value;

        state_t() = default;

        state_t(const ballot_number_t &promised, const ballot_number_t &accepted, const value_t &value)
                : promised(promised), accepted(accepted), value(value) {}

        state_t(const ballot_number_t &promised, const ballot_number_t &accepted, value_t &&value)
                : promised(promised), accepted(accepted), value(std::forward<value_t>(value)) {}

        void reset_accepted() {
            accepted.reset();
            value.reset();
        }

    };

    struct learn_t final {

        instance_id_t instance_id;
        ballot_number_t accepted;
        value_t value;

        learn_t()
                : instance_id(0) {}

        learn_t(const instance_id_t &instance_id, const ballot_number_t &accepted, const value_t &value)
                : instance_id(instance_id), accepted(accepted), value(value) {}

        learn_t(const instance_id_t &instance_id, const ballot_number_t &accepted, value_t &&value)
                : instance_id(instance_id), accepted(accepted), value(std::forward<value_t>(value)) {}

    };

    struct message_t final {

        enum type_e {
            noop = 0,
            prepare,
            prepare_promise,
            prepare_reject,
            accept,
            accept_accept,
            accept_reject,
            value_chosen,
            learn_ping,
            learn_pong,
            learn_request,
            learn_response
        } type;

        // Sender info.
        group_id_t group_id;
        instance_id_t instance_id;
        node_id_t node_id;

        /**
         * Following field may optional.
         */

        // As sequence number for reply.
        proposal_id_t proposal_id;

        ballot_number_t ballot;
        value_t value;

        // For learner data transmit.
        bool overload; // Used in ping & pong. This should be consider when send learn request.
        instance_id_t min_stored_instance_id; // Used in ping and pong.
        std::vector<learn_t> learn_batch;
        std::vector<Csnapshot::shared_ptr> snapshot_batch;

        message_t()
                : type(noop), group_id(0), instance_id(0), node_id(0), proposal_id(0),
                  overload(false), min_stored_instance_id(0) {}

        explicit operator bool() const {
            return type != noop;
        }

    };

    template<class T>
    class Cbase {

    NO_COPY_MOVE(Cbase);

    public:

        enum routine_status_e {
            routine_success = 0,
            routine_error_type,
            routine_error_state,
            routine_error_group,
            routine_error_lag,
            routine_error_instance,
            routine_error_need_learn,
            routine_error_proposal,
            routine_write_fail,
            routine_read_fail,
            routine_error_role,
            routine_fatal_error
        };

    private:

        // Caution: This cb will hold multiple locks which may include
        //          acceptor lock, learner lock and base instance update lock.
        //          So be careful to avoid dead lock.
        inline void synced_value_done(const instance_id_t &instance_id, const value_t &value) {
            static_cast<T *>(this)->synced_value_done(instance_id, value);
        }

        // Caution: This cb will hold multiple locks which may include
        //          acceptor lock and base instance update lock.
        //          So be careful to avoid dead lock.
        inline void synced_reset_instance(const instance_id_t &from, const instance_id_t &to) {
            static_cast<T *>(this)->synced_reset_instance(from, to);
        }

        // Const info of instance.
        const node_id_t node_id_;
        const group_id_t group_id_;
        const write_options_t default_write_options_;

        std::mutex update_lock_;
        std::atomic<instance_id_t> instance_id_;

        Cstorage &storage_;
        Ccommunication &communication_;
        CpeerManager &peer_manager_;

    public:

        Cbase(const node_id_t &node_id, const group_id_t &group_id,
              Cstorage &storage, Ccommunication &communication, CpeerManager &peer_manager,
              const write_options_t &default_write_options)
                : node_id_(node_id), group_id_(group_id),
                  default_write_options_(default_write_options), instance_id_(0),
                  storage_(storage), communication_(communication), peer_manager_(peer_manager) {}

        virtual ~Cbase() = default;

        // Const refer.

        const node_id_t &node_id() const {
            return node_id_;
        }

        const group_id_t &group_id() const {
            return group_id_;
        }

        const std::atomic<instance_id_t> &instance_id() const {
            return instance_id_;
        }

        // Common function(s).

        bool is_voter(const instance_id_t &instance_id) {
            return peer_manager_.is_voter(group_id_, instance_id, node_id_);
        }

        bool is_all_peer(const instance_id_t &instance_id, const std::set<node_id_t> &node_set) {
            return peer_manager_.is_all_peer(group_id_, instance_id, node_set);
        }

        bool is_all_voter(const instance_id_t &instance_id, const std::set<node_id_t> &node_set) {
            return peer_manager_.is_all_voter(group_id_, instance_id, node_set);
        }

        bool is_quorum(const instance_id_t &instance_id, const std::set<node_id_t> &node_set) {
            return peer_manager_.is_quorum(group_id_, instance_id, node_set);
        }

        // Return value: 0 success, >0 empty, <0 error. This can >0 after compact.
        int get_min_instance_id(instance_id_t &instance_id) {
            return storage_.get_min_instance_id(group_id_, instance_id);
        }

        // Return value: 0 success, >0 empty, <0 error.
        int get_max_instance_id(instance_id_t &instance_id) {
            return storage_.get_max_instance_id(group_id_, instance_id);
        }

        void set_instance_id(instance_id_t &instance_id) {
            std::lock_guard<std::mutex> lck(update_lock_);
            instance_id_.store(instance_id, std::memory_order_release);
        }

        bool get(const instance_id_t &instance_id, state_t &state) {
            return 0 == storage_.get(group_id_, instance_id, state);
        }

        // Batch get.
        bool get(const instance_id_t &instance_id, std::vector<state_t> &states) {
            return 0 == storage_.get(group_id_, instance_id, states);
        }

        bool put(const instance_id_t &instance_id, const state_t &state, bool &lag) {
            // Fail fast.
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            lag = false;
            // Lock.
            std::lock_guard<std::mutex> lck(update_lock_);
            // Check in lock.
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            return 0 == storage_.put(group_id_, instance_id, state, default_write_options_);
        }

        bool next_instance(const instance_id_t &instance_id, const value_t &chosen_value) {
            // Fail fast.
            if (instance_id != instance_id_.load(std::memory_order_acquire))
                return false;
            auto expected = instance_id;
            // Lock.
            std::lock_guard<std::mutex> lck(update_lock_);
            if (instance_id != instance_id_.load(std::memory_order_acquire))
                return false;
            synced_value_done(instance_id, chosen_value);
            if (!instance_id_.compare_exchange_strong(expected, instance_id + 1))
                throw std::runtime_error("Cbase::next_instance CAS fatal error.");
            return true;
        }

        bool put_and_next_instance(const instance_id_t &instance_id, const state_t &state, bool &lag) {
            // Fail fast.
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            lag = false;
            auto expected = instance_id;
            // Lock.
            std::lock_guard<std::mutex> lck(update_lock_);
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            if (storage_.put(group_id_, instance_id, state, default_write_options_) != 0)
                return false;
            synced_value_done(instance_id, state.value);
            if (!instance_id_.compare_exchange_strong(expected, instance_id + 1))
                throw std::runtime_error("Cbase::put_and_next_instance CAS fatal error.");
            return true;
        }

        // Batch put and next.
        bool put_and_next_instance(const instance_id_t &instance_id, const std::vector<state_t> &states, bool &lag) {
            // Fail fast.
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            lag = false;
            auto expected = instance_id;
            // Lock.
            std::lock_guard<std::mutex> lck(update_lock_);
            if (instance_id != instance_id_.load(std::memory_order_acquire)) {
                lag = true;
                return false;
            }
            if (storage_.put(group_id_, instance_id, states, default_write_options_) != 0)
                return false;
            auto offset = 0;
            for (const auto &state : states) {
                synced_value_done(instance_id + offset, state.value);
                ++offset;
            }
            if (!instance_id_.compare_exchange_strong(expected, instance_id + states.size()))
                throw std::runtime_error("Cbase::put_and_next_instance CAS fatal error.");
            return true;
        }

        // Note: Allow to reset to current.
        bool reset_min_instance(const instance_id_t &instance_id, const state_t &state) {
            // Fail fast.
            if (instance_id < instance_id_.load(std::memory_order_acquire))
                return false;
            // Lock.
            std::lock_guard<std::mutex> lck(update_lock_);
            auto expected = instance_id_.load(std::memory_order_acquire);
            if (instance_id < expected)
                return false;
            if (storage_.reset_min_instance(group_id_, instance_id, state, default_write_options_) != 0)
                return false;
            synced_reset_instance(expected, instance_id);
            if (!instance_id_.compare_exchange_strong(expected, instance_id))
                throw std::runtime_error("Cbase::reset_min_instance CAS fatal error.");
            return true;
        }

        bool broadcast(const message_t &msg,
                       Ccommunication::broadcast_range_e range,
                       Ccommunication::broadcast_type_e type) {
            return 0 == communication_.broadcast(msg, range, type);
        }

        bool send(const node_id_t &target_id, const message_t &msg) {
            return 0 == communication_.send(target_id, msg);
        }

    };

}
