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
// Created by zzy on 2019-01-31.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <random>
#include <functional>
#include <memory>
#include <stdexcept>

#include "utility/common_define.h"
#include "utility/atomicex.h"
#include "utility/time.h"
#include "utility/timer_work_queue.h"

#include "type.h"
#include "base.h"
#include "log.h"
#include "storage.h"
#include "communication.h"
#include "peer.h"
#include "proposer.h"
#include "acceptor.h"
#include "learner.h"
#include "leader_manager.h"

namespace zpaxos {

    class Cinstance final
            : private Cbase<Cinstance>,
              private Cproposer<Cinstance>,
              private Cacceptor<Cinstance>,
              private Clearner<Cinstance>,
              public CleaderManager {

    NO_COPY_MOVE(Cinstance);

    private:

        utility::CtimerWorkQueue &work_queue_;
        std::atomic<int> queued_task_cnt_;

        std::atomic<int> queued_;
        std::timed_mutex transaction_lock_;

        std::mt19937_64 rand_;

        struct instance_notifier_t final {
            std::mutex lock;
            std::condition_variable cv;

            instance_id_t instance_id;
            bool done;
            enum {
                failed,
                bypassed,
                succeeded,
                compromised
            } state;
            const value_t *value;

            instance_notifier_t()
                    : instance_id(0), done(true), state(failed), value(nullptr) {}
        } notifier_;

        struct instance_waiter_t final {
            std::mutex lock;
            std::condition_variable cv;

            bool idle;

            instance_waiter_t()
                    : idle(false) {}
        } waiter_;

        utility::CspinRWLock cb_lock_;
        std::function<void(
                const group_id_t &group_id,
                const instance_id_t &instance_id,
                const value_t &value)> synced_callback_;
        std::function<void(
                const group_id_t &group_id,
                const instance_id_t &instance_id,
                value_t &&value)> callback_;
        std::function<bool(
                const group_id_t &group_id,
                const instance_id_t &peer_instance_id,
                std::vector<Csnapshot::shared_ptr> &snapshots)> take_snapshot_cb_;
        std::function<bool(
                const group_id_t &group_id,
                const std::vector<Csnapshot::shared_ptr> &snapshots)> load_snapshot_cb_;

        const size_t max_replay_batch_size_;

        friend class Cbase;

        friend class Cproposer;

        friend class Cacceptor;

        friend class Clearner;

        void synced_value_done(const instance_id_t &instance_id, const value_t &value) {
            // Be careful that we hold multiple locks.
            utility::CautoSpinRWLock lck(cb_lock_);
            if (synced_callback_)
                synced_callback_(Cbase::group_id(), instance_id, value);
        }

        void synced_reset_instance(const instance_id_t &from, const instance_id_t &to) {
            // Be careful that we hold multiple locks.

            // Inform notifier.
            {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                // Note: Notifier may lag.
                if (notifier_.instance_id >= from && notifier_.instance_id < to) {
                    // Set once.
                    notifier_.done = true;
                    notifier_.state = instance_notifier_t::bypassed;
                    notifier_.cv.notify_one();
                }
            }
        }

        Cbase::routine_status_e self_prepare(const message_t &msg) {
            // Call Cacceptor::on_prepare directly to make sure max proposed id be saved.
            if (CleaderManager::admit_propose(msg.node_id))
                return Cacceptor::on_prepare(msg);
            else
                return Cbase::routine_error_role; // Not leader.
        }

        Cbase::routine_status_e self_chosen(const message_t &msg) {
            // Call Cacceptor::on_chosen directly to switch to a appropriate state by status.
            CleaderManager::record_leader(msg.node_id);
            return Cacceptor::on_chosen(msg);
        }

        void on_proposer_idle() {
            // Proposer become idle. Check notify.

            // Inform notifier.
            {
                auto now_instance_id = Cbase::instance_id().load(std::memory_order_acquire);
                std::lock_guard<std::mutex> lck(notifier_.lock);
                // Note: Notifier may lag.
                if (notifier_.instance_id == now_instance_id && !notifier_.done) {
                    // Set once.
                    notifier_.done = true;
                    notifier_.state = instance_notifier_t::failed;
                    notifier_.cv.notify_one();
                }
            }

            // Inform waiter.
            {
                std::lock_guard<std::mutex> lck(waiter_.lock);
                if (!waiter_.idle) {
                    waiter_.idle = true;
                    waiter_.cv.notify_one();
                }
            }
        }

        void on_proposer_final(const instance_id_t &instance_id, value_t &&value) {
            // Proposer become final. Check notify or reset.

            // Inform notifier only.
            {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                if (notifier_.instance_id == instance_id) {
                    // Always update.
                    notifier_.done = true;
                    if (nullptr == notifier_.value)
                        notifier_.state = instance_notifier_t::compromised;
                    else
                        notifier_.state = value == *notifier_.value ? instance_notifier_t::succeeded
                                                                    : instance_notifier_t::compromised;
                    notifier_.cv.notify_one();
                }
            }
        }

        void value_chosen(const instance_id_t &instance_id, value_t &&value) {
            // Check notify, and reset all components for new instance.

            // Inform notifier.
            {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                if (notifier_.instance_id == instance_id) {
                    // Always update.
                    notifier_.done = true;
                    if (nullptr == notifier_.value)
                        notifier_.state = instance_notifier_t::compromised;
                    else
                        notifier_.state = value == *notifier_.value ? instance_notifier_t::succeeded
                                                                    : instance_notifier_t::compromised;
                    notifier_.cv.notify_one();
                }
            }

            {
                utility::CautoSpinRWLock lck(cb_lock_);
                if (callback_)
                    callback_(Cbase::group_id(), instance_id, std::forward<value_t>(value));
            }

            Cproposer::init_for_new_instance();
            Cacceptor::init_for_new_instance();

            // Inform waiter.
            {
                std::lock_guard<std::mutex> lck(waiter_.lock);
                if (!waiter_.idle) {
                    waiter_.idle = true;
                    waiter_.cv.notify_one();
                }
            }
        }

        void value_learnt(const instance_id_t &instance_id, value_t &&value) {
            // Check notify.

            // Inform notifier only.
            {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                if (notifier_.instance_id == instance_id) {
                    // Always update.
                    notifier_.done = true;
                    if (nullptr == notifier_.value)
                        notifier_.state = instance_notifier_t::compromised;
                    else
                        notifier_.state = value == *notifier_.value ? instance_notifier_t::succeeded
                                                                    : instance_notifier_t::compromised;
                    notifier_.cv.notify_one();
                }
            }

            {
                utility::CautoSpinRWLock lck(cb_lock_);
                if (callback_)
                    callback_(Cbase::group_id(), instance_id, std::forward<value_t>(value));
            }
        }

        void learn_done() {
            // Reset all components for new instance.
            Cproposer::init_for_new_instance();
            Cacceptor::init_for_new_instance();

            // Inform waiter only.
            {
                std::lock_guard<std::mutex> lck(waiter_.lock);
                if (!waiter_.idle) {
                    waiter_.idle = true;
                    waiter_.cv.notify_one();
                }
            }
        }

        Cbase::routine_status_e take_snapshots(
                const instance_id_t &peer_instance_id, std::vector<Csnapshot::shared_ptr> &snapshots) {
            utility::CautoSpinRWLock lck(cb_lock_);
            if (take_snapshot_cb_)
                return take_snapshot_cb_(Cbase::group_id(), peer_instance_id, snapshots) ? Cbase::routine_success
                                                                                         : Cbase::routine_read_fail;
            return Cbase::routine_fatal_error;
        }

        Cbase::routine_status_e load_snapshots(const std::vector<Csnapshot::shared_ptr> &snapshots) {
            if (snapshots.empty())
                return Cbase::routine_success;
            utility::CautoSpinRWLock lck(cb_lock_);
            if (load_snapshot_cb_)
                return load_snapshot_cb_(Cbase::group_id(), snapshots) ? Cbase::routine_success
                                                                       : Cbase::routine_write_fail;
            return Cbase::routine_fatal_error;
        }

        void internal_reset_wait_idle() {
            std::lock_guard<std::mutex> lck(waiter_.lock);
            waiter_.idle = false;
        }

        bool internal_wait_idle(uint32_t timeout_ms) {
            std::unique_lock<std::mutex> lck(waiter_.lock);
            if (!waiter_.idle) {
                waiter_.cv.wait_for(lck, std::chrono::milliseconds(timeout_ms));
                return waiter_.idle;
            }
            return true;
        }

    public:

        Cinstance(const node_id_t &node_id, const group_id_t &group_id,
                  Cstorage &storage, Ccommunication &communication, CpeerManager &peer_manager,
                  utility::CtimerWorkQueue &timer_work_queue,
                  size_t max_learn_replay_batch_size = 64,
                  uint32_t prepare_timeout_ms = 1000, uint32_t accept_timeout_ms = 500,
                  uint32_t ping_timeout_ms = 500, uint32_t learn_timeout_ms = 1000,
                  uint16_t leader_lease_ms = 3000,
                  write_options_t default_write_options = write_options_t())
                : Cbase(node_id, group_id, storage, communication, peer_manager, default_write_options),
                  Cproposer(static_cast<Cbase<Cinstance> &>(*this),
                            timer_work_queue, prepare_timeout_ms, accept_timeout_ms),
                  Cacceptor(static_cast<Cbase<Cinstance> &>(*this)),
                  Clearner(static_cast<Cbase<Cinstance> &>(*this),
                           timer_work_queue, max_learn_replay_batch_size, ping_timeout_ms, learn_timeout_ms),
                  CleaderManager(leader_lease_ms),
                  work_queue_(timer_work_queue),
                  queued_task_cnt_(0),
                  queued_(0),
                  rand_((static_cast<uint64_t>(utility::Ctime::steady_us()) << 16u) | (node_id << 8u) | group_id),
                  max_replay_batch_size_(max_learn_replay_batch_size) {
            if (!Cacceptor::restore())
                throw std::runtime_error("Cinstance::Cacceptor::restore() error.");
            Cproposer::set_start_propose_id(Cacceptor::get_latest_proposal_id() + 1);
        }

        ~Cinstance() final {
            work_queue_.clear_all_task();
            while (queued_task_cnt_.load(std::memory_order_acquire) > 0)
                utility::Ctime::sleep_ms(1);
            Cproposer::terminate();
            Clearner::terminate();
        }

        // Only used for critical SM which need sync, eg. dynamic peer management SM or leader election SM.
        // CB will called before normal CB and hold multiple locks, so be careful to avoid dead lock.
        void set_synced_callback(const std::function<void(
                const group_id_t &group_id,
                const instance_id_t &instance_id,
                const value_t &value)> &cb) {
            utility::CautoSpinRWLock lck(cb_lock_, true);
            synced_callback_ = cb;
        }

        void set_callback(const std::function<void(
                const group_id_t &group_id,
                const instance_id_t &instance_id,
                value_t &&value)> &cb) {
            utility::CautoSpinRWLock lck(cb_lock_, true);
            callback_ = cb;
        }

        void set_take_snapshot_callback(const std::function<bool(
                const group_id_t &group_id,
                const instance_id_t &peer_instance_id,
                std::vector<Csnapshot::shared_ptr> &snapshots)> &cb) {
            utility::CautoSpinRWLock lck(cb_lock_, true);
            take_snapshot_cb_ = cb;
        }

        void set_load_snapshot_callback(const std::function<bool(
                const group_id_t &group_id,
                const std::vector<Csnapshot::shared_ptr> &snapshots)> &cb) {
            utility::CautoSpinRWLock lck(cb_lock_, true);
            load_snapshot_cb_ = cb;
        }

        bool replay(const instance_id_t &start_id, instance_id_t &now_id) {
            now_id = Cbase::instance_id().load(std::memory_order_acquire);

            std::vector<state_t> states;
            instance_id_t offset = 0;

            while (start_id + offset < now_id) {
                auto batch_size = now_id - (start_id + offset) > max_replay_batch_size_ ? max_replay_batch_size_
                                                                                        : now_id - (start_id + offset);

                states.resize(batch_size);
                if (!Cbase::get(start_id + offset, states))
                    return false;

                {
                    utility::CautoSpinRWLock lck(cb_lock_);
                    for (auto idx = 0; idx < batch_size; ++idx) {
                        if (synced_callback_)
                            synced_callback_(Cbase::group_id(), start_id + offset + idx, states[idx].value);
                        if (callback_)
                            callback_(Cbase::group_id(), start_id + offset + idx, std::move(states[idx].value));
                    }
                }

                offset += batch_size;
            }

            return true;
        }

        // TODO: Store and retry msg for acceptor is a good idea.

        void dispatch_message(message_t &&msg) {
            // Check group first.
            if (msg.group_id != Cbase::group_id())
                return;

            Cbase::routine_status_e state_ret = Cbase::routine_success;

            switch (msg.type) {
                case message_t::prepare:
                    if (CleaderManager::admit_propose(msg.node_id))
                        state_ret = Cacceptor::on_prepare(msg);
                    else
                        state_ret = Cbase::routine_error_role; // Not leader.
                    break;

                case message_t::prepare_promise:
                case message_t::prepare_reject:
                    state_ret = Cproposer::on_prepare_reply(std::forward<message_t>(msg));
                    break;

                case message_t::accept:
                    state_ret = Cacceptor::on_accept(std::forward<message_t>(msg));
                    break;

                case message_t::accept_accept:
                case message_t::accept_reject:
                    state_ret = Cproposer::on_accept_reply(msg);
                    break;

                case message_t::value_chosen:
                    CleaderManager::record_leader(msg.node_id);
                    state_ret = Cacceptor::on_chosen(msg);
                    break;

                case message_t::learn_ping:
                    state_ret = Clearner::on_ping(msg);
                    break;

                case message_t::learn_pong:
                    state_ret = Clearner::on_pong(msg);
                    break;

                case message_t::learn_request:
                    state_ret = Clearner::on_learn(msg);
                    break;

                case message_t::learn_response:
                    state_ret = Clearner::on_learn_response(std::forward<message_t>(msg));
                    break;

                case message_t::noop:
                default:
                    return;
            }

            // Caution: value_t(value & learn_batch) may moved from msg, so never touch it again.
            switch (state_ret) {
                case Cbase::routine_error_need_learn: {
                    // I need learn.
                    static auto last_time = utility::Ctime::steady_ms();
                    auto now_time = utility::Ctime::steady_ms();
                    if (now_time - last_time > 100 && queued_task_cnt_.load(std::memory_order_acquire) <= 0) {
                        auto target = msg.node_id;
                        ++queued_task_cnt_;
                        work_queue_.push_timeout(std::move((new utility::CtimerWorkQueue::Cfunction(
                                [this, target]() {
                                    Clearner::learn(target);
                                }, [this]() {
                                    --queued_task_cnt_;
                                }))->gen_task()));
                        last_time = now_time;
                    }
                }
                    break;

                case Cbase::routine_error_instance: {
                    // Peer need learn.
                    static auto last_time = utility::Ctime::steady_ms();
                    auto now_time = utility::Ctime::steady_ms();
                    if (now_time - last_time > 100 && queued_task_cnt_.load(std::memory_order_acquire) <= 0) {
                        auto target = msg.node_id;
                        auto inst = msg.instance_id;
                        ++queued_task_cnt_;
                        work_queue_.push_timeout(std::move((new utility::CtimerWorkQueue::Cfunction(
                                [this, target, inst]() {
                                    Clearner::learn_response(target, inst);
                                }, [this]() {
                                    --queued_task_cnt_;
                                }))->gen_task()));
                        last_time = now_time;
                    }
                }
                    break;

                default:
                    break;
            }

            // Task check.
            assert(queued_task_cnt_.load(std::memory_order_acquire) >= 0);
        }

        instance_id_t now_instance_id() const {
            return Cbase::instance_id().load(std::memory_order_acquire);
        }

        enum commit_status_e {
            commit_success,
            commit_queue_drop,
            commit_queue_timeout,
            commit_wait_timeout,
            commit_propose_send_error,
            commit_propose_pending, // May not fail.
            commit_propose_compromised, // Propose others value.
            commit_propose_bypassed // Snapshot learnt.
        };

        // Set instance_id = INSTANCE_ID_INVALID to use auto instance id assign.
        commit_status_e commit(const value_t &value, instance_id_t &instance_id,
                               uint32_t queue_timeout_ms = 3000, uint32_t commit_timeout_ms = 3000,
                               uint32_t retry_base_time_ms = 10, uint32_t retry_variable_time_ms = 30,
                               size_t max_queued = 3) {
            // Check waiting queue.
            utility::CatomicLatchCounter<int> counter(queued_);
            if (static_cast<int>(counter) >= max_queued)
                return commit_queue_drop;

            // Lock transaction with timeout.
            std::unique_lock<std::timed_mutex> t_lck(transaction_lock_, std::chrono::milliseconds(queue_timeout_ms));
            if (!t_lck)
                return commit_queue_timeout;

            // Set start time.
            auto start_time = utility::Ctime::steady_ms();

            internal_reset_wait_idle();

            if (!Cproposer::is_idle()) {
                // Need wait for idle.
                bool wait_done;
                int wait_count = 0;
                do {
                    auto now_time = utility::Ctime::steady_ms();
                    if (now_time - start_time >= static_cast<int64_t>(commit_timeout_ms))
                        return commit_wait_timeout;

                    auto wait_time = start_time + commit_timeout_ms - now_time;
                    if (wait_time > 500)
                        wait_time = 500;
                    wait_done = internal_wait_idle(static_cast<uint32_t>(wait_time));

                    ++wait_count;
                    if (wait_count >= 3)
                        Clearner::ping(); // Start learn if 2 fail(at least 1000ms).
                } while (!wait_done);
            }

            // Now idle, set notifier first.
            if (INSTANCE_ID_INVALID == instance_id)
                instance_id = Cbase::instance_id().load(std::memory_order_acquire);
            {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                notifier_.instance_id = instance_id;
                notifier_.done = false;
                notifier_.state = instance_notifier_t::failed;
                notifier_.value = &value;
            }

            // Check again after set a notifier to prevent missing the instance change.
            // Because any change to instance_id will follow a notify(chosen or learnt) which help us to break the wait.
            if (instance_id != Cbase::instance_id().load(std::memory_order_acquire)) {
                std::lock_guard<std::mutex> lck(notifier_.lock);
                notifier_.done = true;
                notifier_.value = nullptr;
                return commit_propose_send_error;
            }

            // Propose value with multiply try.
            auto propose_time = utility::Ctime::steady_ms();
            auto try_cnt = 0;
            do {
                // Propose, fail if instance id changed.
                if (Cproposer::propose(instance_id, value) != Cbase::routine_success) {
                    // Check working instance id.
                    auto instance_changed = instance_id != Cbase::instance_id().load(std::memory_order_acquire);

                    std::unique_lock<std::mutex> lck(notifier_.lock);
                    if (instance_changed) {
                        // Wait last instance info.
                        while (!notifier_.done) {
                            auto now_time = utility::Ctime::steady_ms();
                            if (now_time - start_time >= static_cast<int64_t>(commit_timeout_ms)) {
                                notifier_.done = true;
                                notifier_.value = nullptr;
                                LOG_ERROR(("[N({}) G({}) I({}) i({})] Cinstance::commit pending at propose fail. "
                                           "now_time: {} propose_start: {} try_cnt: {}",
                                        Cbase::node_id(), Cbase::group_id(), Cbase::instance_id(), instance_id,
                                        now_time, propose_time, try_cnt));
                                return commit_propose_pending;
                            }

                            auto wait_time = start_time + commit_timeout_ms - now_time;
                            notifier_.cv.wait_for(lck, std::chrono::milliseconds(wait_time));
                        }
                        notifier_.value = nullptr;
                        switch (notifier_.state) {
                            case instance_notifier_t::bypassed:
                                return commit_propose_bypassed;
                            case instance_notifier_t::succeeded:
                                return commit_success;
                            case instance_notifier_t::compromised:
                                return commit_propose_compromised;
                            default:
                                return commit_propose_send_error;
                        }
                    }
                    notifier_.done = true;
                    notifier_.value = nullptr;
                    return commit_propose_send_error;
                }

                // Now hold the notifier lock and wait done.
                std::unique_lock<std::mutex> lck(notifier_.lock);
                while (!notifier_.done) {
                    auto now_time = utility::Ctime::steady_ms();
                    if (now_time - start_time >= static_cast<int64_t>(commit_timeout_ms)) {
                        notifier_.done = true;
                        notifier_.value = nullptr;
                        LOG_ERROR(("[N({}) G({}) I({}) i({})] Cinstance::commit pending at propose success. "
                                   "now_time: {} propose_start: {} try_cnt: {}",
                                Cbase::node_id(), Cbase::group_id(), Cbase::instance_id(), instance_id,
                                now_time, propose_time, try_cnt));
                        return commit_propose_pending;
                    }

                    auto wait_time = start_time + commit_timeout_ms - now_time;
                    notifier_.cv.wait_for(lck, std::chrono::milliseconds(wait_time));
                }

                if (instance_notifier_t::failed == notifier_.state) {
                    // Fail by timeout or reject.
                    notifier_.done = false; // Reset notifier.
                    lck.unlock(); // Prevent dead lock.

                    auto last_err = Cproposer::last_error();
                    if (Cproposer::error_prepare_timeout == last_err ||
                        Cproposer::error_accept_timeout == last_err)
                        Clearner::ping(); // Maybe my instance id mismatch.

                    // Sleep and retry.
                    utility::Ctime::sleep_ms(rand_() % retry_variable_time_ms + retry_base_time_ms);
                } else {
                    // Done.
                    notifier_.value = nullptr;
                    switch (notifier_.state) {
                        case instance_notifier_t::bypassed:
                            return commit_propose_bypassed;
                        case instance_notifier_t::succeeded:
                            return commit_success;
                        case instance_notifier_t::compromised:
                            return commit_propose_compromised;
                        default: // Never happen.
                            return commit_propose_send_error;
                    }
                }
                ++try_cnt;
            } while (true);
        }

        // Use this to collect garbage of peer manager,
        // because working_instance_id always growing and former peer info can be dropped.
        instance_id_t get_proposer_working_instance_id() {
            return Cproposer::get_working_instance_id();
        }

        bool persist_acceptor_state(instance_id_t &persisted_instance_id) {
            return Cacceptor::persist_now_state(persisted_instance_id);
        }

        bool reset_acceptor_instance(const instance_id_t &instance_id) {
            return Cacceptor::reset_working_instance(instance_id);
        }

        Cbase::routine_status_e sync() {
            return Clearner::ping();
        }

    };

}
