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

#include <iostream>
#include <random>
#include <memory>
#include <vector>
#include <atomic>

using namespace std;

#include "zpaxos/utility/timer_work_queue.h"
#include "zpaxos/utility/hash.h"
#include "zpaxos/utility/atomicex.h"
#include "zpaxos/network/memory_peer_network.h"
#include "zpaxos/storage/memory_storage.h"

#include "zpaxos/storage.h"
#include "zpaxos/communication.h"
#include "zpaxos/peer.h"
#include "zpaxos/proposer.h"
#include "zpaxos/acceptor.h"
#include "zpaxos/learner.h"
#include "zpaxos/instance.h"
#include "zpaxos/state_machine.h"

#include "zpaxos/log.h"

namespace zpaxos {

    class CunitTest {

    NO_CONSTRUCTOR(CunitTest);
    NO_COPY_MOVE(CunitTest);

        static std::mutex &output_mutex() {
            static std::mutex lock;
            return lock;
        }

    public:

        static void test_single_instance() {
            utility::CtimerWorkQueue queue;

            network::CfixedPeerNetwork peer(1, true);
            storage::CmemoryStorage storage;

            Cinstance instance(1, 0, storage, peer, peer, queue,
                               64,
                               1000, 500,
                               500, 1000,
                               10);

            // Bind CB.
            instance.set_callback([](const group_id_t &group_id,
                                     const instance_id_t &instance_id,
                                     value_t &&value) {
                cout << "group: " << group_id << " instance: " << instance_id << " value: "
                     << reinterpret_cast<const char *>(value.buffer.data()) << endl;
            });

            // Replay.
            instance_id_t now_inst;
            if (!instance.replay(0, now_inst)) {
                LOG_ERROR(("!!! replay error. now inst {}.", now_inst));
            }

            // Start network.
            peer.set_message_callback([&instance](message_t &&msg) {
                instance.dispatch_message(std::forward<message_t>(msg));
                return 0;
            });

            std::string data("hello world.");
            value_t val;
            val.state_machine_id = 999;
            val.buffer.set(data.c_str(), data.length() + 1);
            instance_id_t instance_id = INSTANCE_ID_INVALID;

            auto start_time = utility::Ctime::steady_ms();

            bool bret = Cinstance::commit_success == instance.commit(val, instance_id);

            auto end_time = utility::Ctime::steady_ms();

            cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                 << " total_time: " << end_time - start_time << "ms" << endl;

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        struct single_instance_t final {

            typedef unique_ptr<single_instance_t> ptr;

            const bool bad_network_;
            const node_id_t my_node_id;

            utility::CtimerWorkQueue queue;
            network::CfixedPeerNetwork peer;
            storage::CmemoryStorage storage;

            Cinstance instance;

            mutex lock;
            atomic<zpaxos::instance_id_t> next_inst;
            atomic<uint64_t> chk_sum;

            class CsimpleSnapshot : public Csnapshot {

            NO_COPY_MOVE(CsimpleSnapshot);

            private:

                const zpaxos::instance_id_t next_;
                const uint64_t chk_sum_;

            public:

                CsimpleSnapshot(const instance_id_t next, const uint64_t chk_sum)
                        : next_(next), chk_sum_(chk_sum) {}

                state_machine_id_t get_id() const final {
                    return 999;
                }

                // The snapshot represent the state machine which run to this id(not included).
                const instance_id_t &get_next_instance_id() const final {
                    return next_;
                }

                uint64_t get_chk_sum() const {
                    return chk_sum_;
                }
            };

            single_instance_t(const node_id_t &node_id, bool voter, bool bad_network = false)
                    : bad_network_(bad_network), my_node_id(node_id), peer(node_id, voter, bad_network),
                      instance(node_id, 0, storage, peer, peer, queue,
                               64,
                               bad_network ? 100 : 1000, bad_network ? 50 : 500,
                               bad_network ? 50 : 500, bad_network ? 100 : 1000,
                               bad_network ? 0 : 10),
                      next_inst(0), chk_sum(0) {
                instance.set_callback([this](const group_id_t &group_id,
                                             const instance_id_t &instance_id,
                                             value_t &&value) {
                    if (bad_network_) {
                        std::lock_guard<std::mutex> lck(output_mutex());
                        cout << "node: " << my_node_id << " group: " << group_id << " instance: " << instance_id
                             << " value: " << utility::Chash::crc32(value.buffer.slice()) << endl;
                    }

                    lock_guard<mutex> lck(lock);

                    if (instance_id != next_inst.load(std::memory_order_acquire))
                        return;

                    auto buf = std::move(value.buffer);
                    auto crc = utility::Chash::crc32(buf.slice());
                    auto sum = (instance_id + 1) * (value.state_machine_id + crc);
                    chk_sum.fetch_add(sum);
                    ++next_inst;
                });

                instance.set_take_snapshot_callback([this](const group_id_t &group_id,
                                                           const instance_id_t &peer_instance_id,
                                                           std::vector<Csnapshot::shared_ptr> &snapshots) {
                    LOG_CRITICAL(("!!! Take snapshot."));
                    if (next_inst.load(std::memory_order_acquire) <= peer_instance_id)
                        throw runtime_error("SM lag?");
                    lock_guard<mutex> lck(lock);
                    snapshots.emplace_back(std::make_shared<CsimpleSnapshot>(next_inst, chk_sum));
                    return true;
                });

                instance.set_load_snapshot_callback([this](const group_id_t &group_id,
                                                           const std::vector<Csnapshot::shared_ptr> &snapshots) {
                    LOG_CRITICAL(("!!! Load snapshot."));
                    if (1 == snapshots.size() && snapshots[0]->get_id() == 999) {
                        auto ss = dynamic_cast<CsimpleSnapshot *>(snapshots[0].get());
                        lock_guard<mutex> lck(lock);
                        if (ss->get_next_instance_id() > next_inst) {
                            next_inst = ss->get_next_instance_id();
                            chk_sum = ss->get_chk_sum();
                            instance.reset_acceptor_instance(next_inst);
                        }
                        return true;
                    }
                    return false;
                });

                // Replay.
                instance_id_t now_inst;
                if (!instance.replay(0, now_inst)) {
                    LOG_ERROR(("!!! replay error. now inst {}.", now_inst));
                }

                // Start network.
                peer.set_message_callback([this](message_t &&msg) {
                    instance.dispatch_message(std::forward<message_t>(msg));
                    return 0;
                });
            }

            ~single_instance_t() {
                peer.set_message_callback(nullptr); // Close network before deconstruct.
            }

        };

        static void test_multi_instance() {
            vector<single_instance_t::ptr> insts;

            for (int i = 1; i <= 3; ++i)
                insts.emplace_back(single_instance_t::ptr(new single_instance_t(i, true)));

            utility::Cbuffer data(256);
            sprintf(reinterpret_cast<char *>(data.data()), "Hello world.");
            value_t val;
            val.state_machine_id = 999;
            val.buffer.set(data.data(), data.length());
            instance_id_t instance_id = INSTANCE_ID_INVALID;

            auto start_time = utility::Ctime::steady_ms();

            for (int i = 0; i < 1000000; ++i) {
                instance_id = INSTANCE_ID_INVALID;
                auto one_start_time = utility::Ctime::steady_ms();
                Cinstance::commit_status_e status = insts[0]->instance.commit(val, instance_id);
                auto one_end_time = utility::Ctime::steady_ms();

                if (instance_id != 0 && one_end_time - one_start_time > 1) {
                    cerr << "ERROR!!!!!! hung " << (one_end_time - one_start_time) << "ms" << endl;
                    //break;
                }

                if (status != Cinstance::commit_success) {
                    std::lock_guard<std::mutex> lck(output_mutex());
                    cerr << "ERROR!!!!!! " << status << endl;
                    break;
                }
            }

            auto end_time = utility::Ctime::steady_ms();

            {
                std::lock_guard<std::mutex> lck(output_mutex());
                cout << "inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        static void test_multi_propose_correctness() {
            const static int VOTER_CNT = 27;
            const static int PER_PROPOSE_CNT = 10000;

            vector<single_instance_t::ptr> insts;

            for (int i = 1; i <= VOTER_CNT; ++i)
                insts.emplace_back(single_instance_t::ptr(new single_instance_t(i, true)));

            vector<thread> threads;
            threads.reserve(VOTER_CNT);
            utility::CatomicExtreme<instance_id_t> max_inst(0);

            auto start_time = utility::Ctime::steady_ms();

            for (int thread_id = 0; thread_id < VOTER_CNT; ++thread_id) {
                threads.emplace_back([&insts, &max_inst, thread_id, start_time]() {
                    mt19937_64 rand(thread_id + start_time);

                    for (int i = 0; i < PER_PROPOSE_CNT; ++i) {
                        // Make a value.
                        utility::Cbuffer buf(sizeof(uint64_t));
                        *reinterpret_cast<uint64_t *>(buf.data()) = rand();
                        value_t val(thread_id, std::move(buf));

                        auto one_start_time = utility::Ctime::steady_ms();

                        instance_id_t instance_id;
                        Cinstance::commit_status_e status;
                        do {
                            while (true) {
                                auto leader = insts[thread_id]->instance.get_leader(true);
                                if (0 == leader || leader == insts[thread_id]->my_node_id) {
                                    break;
                                }
                                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            }

                            instance_id = INSTANCE_ID_INVALID;
                            status = insts[thread_id]->instance.commit(val, instance_id);
                        } while (status == Cinstance::commit_propose_compromised ||
                                 status == Cinstance::commit_propose_send_error);

                        auto one_end_time = utility::Ctime::steady_ms();

                        if (status != Cinstance::commit_success) {
                            std::lock_guard<std::mutex> lck(output_mutex());
                            cerr << "ERROR!!!!!! " << status << endl;
                            break;
                        } else {
                            // Success.
                            max_inst.record_max(instance_id);
                            if (one_end_time - one_start_time > 200) {
                                std::lock_guard<std::mutex> lck(output_mutex());
                                cerr << "ERROR!!!!!! hung " << (one_end_time - one_start_time) << "ms" << endl;
                                //break;
                            }
                        }
                    }
                });
            }

            for (auto &th : threads)
                th.join();


            auto end_time = utility::Ctime::steady_ms();

            // Sync all.
            for (auto &item : insts)
                item->instance.sync();

            std::this_thread::sleep_for(std::chrono::seconds(2));

            {
                std::lock_guard<std::mutex> lck(output_mutex());
                cout << "now_inst: " << static_cast<std::atomic<instance_id_t> &>(max_inst).load()
                     << " total_time: " << end_time - start_time << "ms" << endl;

                auto base_sum = insts[0]->chk_sum.load();
                for (const auto &item : insts) {
                    auto sum = item->chk_sum.load();
                    cout << "node: " << item->my_node_id
                         << " chk_sum=" << sum
                         << " cnt: " << item->next_inst.load() << endl;
                    if (sum != base_sum)
                        cerr << "ERROR!!!!!! inconsistency!!!!" << endl;
                }
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        static void test_multi_propose_bad_network_correctness() {
            const static int VOTER_CNT = 27;
            const static int PER_PROPOSE_CNT = 100;

            vector<single_instance_t::ptr> insts;

            for (int i = 1; i <= VOTER_CNT; ++i)
                insts.emplace_back(single_instance_t::ptr(new single_instance_t(i, true, true)));

            vector<thread> threads;
            threads.reserve(VOTER_CNT);
            utility::CatomicExtreme<instance_id_t> max_inst(0);

            auto start_time = utility::Ctime::steady_ms();

            for (int thread_id = 0; thread_id < VOTER_CNT; ++thread_id) {
                threads.emplace_back([&insts, &max_inst, thread_id, start_time]() {
                    mt19937_64 rand(thread_id + start_time);

                    for (int i = 0; i < PER_PROPOSE_CNT; ++i) {
                        // Make a value.
                        utility::Cbuffer buf(sizeof(uint64_t));
                        *reinterpret_cast<uint64_t *>(buf.data()) = rand();
                        value_t val(thread_id, std::move(buf));

                        auto one_start_time = utility::Ctime::steady_ms();

                        instance_id_t instance_id;
                        Cinstance::commit_status_e status;
                        do {
                            while (true) {
                                auto leader = insts[thread_id]->instance.get_leader(true);
                                if (0 == leader || leader == insts[thread_id]->my_node_id) {
                                    break;
                                }
                                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                            }

                            instance_id = INSTANCE_ID_INVALID;
                            status = insts[thread_id]->instance.commit(val, instance_id);
                        } while (status == Cinstance::commit_propose_compromised ||
                                 status == Cinstance::commit_propose_send_error);

                        auto one_end_time = utility::Ctime::steady_ms();

                        if (status != Cinstance::commit_success) {
                            // This may happen. pending.
                            std::lock_guard<std::mutex> lck(output_mutex());
                            cerr << "ERROR!!!!!! " << status << endl;
                            break;
                        } else {
                            // Success.
                            max_inst.record_max(instance_id);
                            if (one_end_time - one_start_time > 200) {
                                std::lock_guard<std::mutex> lck(output_mutex());
                                cerr << "ERROR!!!!!! hung " << (one_end_time - one_start_time) << "ms" << endl;
                                //break;
                            }
                        }
                    }
                });
            }

            for (auto &th : threads)
                th.join();

            auto end_time = utility::Ctime::steady_ms();

            // May less than number.
            while (true) {
                uint64_t max_cnt = 0;
                for (auto &item : insts) {
                    if (item->next_inst > max_cnt)
                        max_cnt = item->next_inst;
                }

                bool all_good = true;
                for (auto &item : insts) {
                    if (item->next_inst != max_cnt) {
                        all_good = false;
                        break;
                    }
                }
                if (all_good)
                    break;

                // Sync all.
                for (auto &item : insts)
                    item->instance.sync();

                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            {
                std::lock_guard<std::mutex> lck(output_mutex());
                cout << "now_inst: " << static_cast<std::atomic<instance_id_t> &>(max_inst).load()
                     << " total_time: " << end_time - start_time << "ms" << endl;

                auto base_sum = insts[0]->chk_sum.load();
                for (const auto &item : insts) {
                    auto sum = item->chk_sum.load();
                    cout << "node: " << item->my_node_id
                         << " chk_sum=" << sum
                         << " cnt: " << item->next_inst.load() << endl;
                    if (sum != base_sum)
                        cerr << "ERROR!!!!!! inconsistency!!!!" << endl;
                }
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        static void test_reset_instance() {
            auto inst = new single_instance_t(1, true);

            std::string data("hello world.");
            value_t val;
            val.state_machine_id = 999;
            val.buffer.set(data.c_str(), data.length() + 1);

            {
                instance_id_t instance_id = INSTANCE_ID_INVALID;
                auto start_time = utility::Ctime::steady_ms();
                bool bret = Cinstance::commit_success == inst->instance.commit(val, instance_id);
                auto end_time = utility::Ctime::steady_ms();
                cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            inst->instance.reset_acceptor_instance(100);

            {
                instance_id_t instance_id = INSTANCE_ID_INVALID;
                auto start_time = utility::Ctime::steady_ms();
                bool bret = Cinstance::commit_success == inst->instance.commit(val, instance_id);
                auto end_time = utility::Ctime::steady_ms();
                cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        static void test_snapshot() {
            auto inst0 = new single_instance_t(1, true);
            auto inst1 = new single_instance_t(2, true);

            std::string data("hello world.");
            value_t val;
            val.state_machine_id = 999;
            val.buffer.set(data.c_str(), data.length() + 1);

            {
                instance_id_t instance_id = INSTANCE_ID_INVALID;
                auto start_time = utility::Ctime::steady_ms();
                bool bret = Cinstance::commit_success == inst0->instance.commit(val, instance_id);
                auto end_time = utility::Ctime::steady_ms();
                cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cout << "inst0: " << inst0->next_inst << "-" << inst0->chk_sum << endl;
            cout << "inst1: " << inst1->next_inst << "-" << inst1->chk_sum << endl;

            inst1->instance.reset_acceptor_instance(100);
            inst1->next_inst = 100;
            inst1->chk_sum = 123456;

            inst0->instance.sync();
            std::this_thread::sleep_for(std::chrono::seconds(1));

            cout << "inst0: " << inst0->next_inst << "-" << inst0->chk_sum << endl;
            cout << "inst1: " << inst1->next_inst << "-" << inst1->chk_sum << endl;

            {
                instance_id_t instance_id = INSTANCE_ID_INVALID;
                auto start_time = utility::Ctime::steady_ms();
                bool bret = Cinstance::commit_success == inst0->instance.commit(val, instance_id);
                auto end_time = utility::Ctime::steady_ms();
                cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cout << "inst0: " << inst0->next_inst << "-" << inst0->chk_sum << endl;
            cout << "inst1: " << inst1->next_inst << "-" << inst1->chk_sum << endl;

            {
                instance_id_t instance_id = INSTANCE_ID_INVALID;
                auto start_time = utility::Ctime::steady_ms();
                bool bret = Cinstance::commit_success == inst1->instance.commit(val, instance_id);
                auto end_time = utility::Ctime::steady_ms();
                cout << "push " << (bret ? "true" : "false") << " inst id " << instance_id
                     << " total_time: " << end_time - start_time << "ms" << endl;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cout << "inst0: " << inst0->next_inst << "-" << inst0->chk_sum << endl;
            cout << "inst1: " << inst1->next_inst << "-" << inst1->chk_sum << endl;

            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

    };

}
