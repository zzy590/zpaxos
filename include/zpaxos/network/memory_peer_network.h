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
// Created by zzy on 2020/3/15.
//

#pragma once

#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "../utility/common_define.h"
#include "../utility/time.h"

#include "../type.h"
#include "../communication.h"
#include "../peer.h"
#include "../base.h"
#include "../log.h"

namespace network {

    template<class T, size_t size>
    class CblockQueue {

    NO_COPY_MOVE(CblockQueue);

    private:

        const bool m_randomShuffle;

        std::mutex m_mutex;
        std::condition_variable m_cvFull;
        std::condition_variable m_cvEmpty;

        T *m_data[size];
        size_t m_front;
        size_t m_back;
        size_t m_length;

        inline void shuffle() {
            for (auto i = 0; i < m_length; ++i) {
                auto k = rand() % (i + 1);
                std::swap(m_data[(m_back + k) % size], m_data[(m_back + i) % size]);
            }
        }

        inline bool tryEnqueue(T *element) {
            if (m_length >= size) {
                return false;
            }
            m_data[m_front] = element;
            if (++m_front == size)
                m_front = 0;
            ++m_length;
            if (m_randomShuffle && (rand() % 10) < 2)
                shuffle(); // Random shuffle.
            return true;
        }

        inline T *tryDequeue(bool &bSuccess) {
            if (0 == m_length) {
                bSuccess = false;
                return nullptr;
            }
            if (m_randomShuffle && (rand() % 10) < 2)
                utility::Ctime::sleep_ms(2); // Random delay.
            T *ret = m_data[m_back];
            m_data[m_back] = nullptr;
            if (++m_back == size)
                m_back = 0;
            --m_length;
            bSuccess = true;
            return ret;
        }

    public:

        explicit CblockQueue(bool randomShuffle = false)
                : m_randomShuffle(randomShuffle), m_front(0), m_back(0), m_length(0) {}

        void push(T *element) {
            std::unique_lock<std::mutex> lck(m_mutex);
            while (!tryEnqueue(element))
                m_cvFull.wait(lck);
            m_cvEmpty.notify_one();
        }

        T *pop() {
            T *ret;
            bool bSuccess;
            std::unique_lock<std::mutex> lck(m_mutex);
            while (ret = tryDequeue(bSuccess), !bSuccess)
                m_cvEmpty.wait(lck);
            m_cvFull.notify_one();
            return ret;
        }

        size_t getLength() const {
            return m_length;
        }

    };

    class CfixedPeerNetwork final : public zpaxos::Ccommunication, public zpaxos::CpeerManager {

    NO_COPY_MOVE(CfixedPeerNetwork);

    private:

        const bool random_drop_duplicate_shuffle_;
        const zpaxos::node_id_t my_node_id_;
        const bool voter_;

        CblockQueue<zpaxos::message_t, 4096> queue_;
        std::thread thread_;
        std::atomic<bool> exited_;

        int run_msg(zpaxos::message_t &&msg) {
            auto start_time_us = utility::Ctime::steady_us();
            auto iret = on_receive(std::forward<zpaxos::message_t>(msg));
            auto end_time_us = utility::Ctime::steady_us();
            if (end_time_us - start_time_us > 100) {
                LOG_WARN(("Dealing msg slow noed-{} {}us {}type",
                        my_node_id_, end_time_us - start_time_us, msg.type));
            }
            return iret;
        }

        void run() {
            zpaxos::message_t *msg;
            while ((msg = queue_.pop()) != nullptr) {
                run_msg(std::move(*msg));
                delete msg;
            }
            exited_.store(true, std::memory_order_release);
        }

        static std::map<zpaxos::node_id_t, CfixedPeerNetwork *> &globalMap() {
            static std::map<zpaxos::node_id_t, CfixedPeerNetwork *> global;
            return global;
        }

    public:

        CfixedPeerNetwork(const zpaxos::node_id_t &my_node_id, bool voter,
                          bool random_drop_duplicate_shuffle = false)
                : random_drop_duplicate_shuffle_(random_drop_duplicate_shuffle),
                  my_node_id_(my_node_id), voter_(voter),
                  queue_(random_drop_duplicate_shuffle),
                  thread_(&CfixedPeerNetwork::run, this),
                  exited_(false) {
            globalMap().emplace(my_node_id_, this);
        }

        ~CfixedPeerNetwork() final {
            globalMap().erase(my_node_id_);
            while (!exited_.load(std::memory_order_acquire)) {
                queue_.push(nullptr);
                utility::Ctime::sleep_ms(10);
            }
            thread_.join();
        }

        int send(const zpaxos::node_id_t &target_id, const zpaxos::message_t &message) final {
            if (random_drop_duplicate_shuffle_ && (rand() % 10) < 2) // Random drop.
                return 0;

            int ret;
            do {
                if (target_id == my_node_id_) {
                    auto copy = message;
                    ret = run_msg(std::move(copy));
                } else {
                    auto it = globalMap().find(target_id);
                    if (it == globalMap().end())
                        return -1;
                    auto copy = new zpaxos::message_t;
                    *copy = message;
                    it->second->queue_.push(copy);
                    if (it->second->queue_.getLength() > 3000) {
                        cerr << "Queue amost full node-" << my_node_id_ << endl;
                    }
                    ret = 0;
                }
            } while (random_drop_duplicate_shuffle_ && (rand() % 10) < 2); // Random duplicate.
            return ret;
        }

        int broadcast(const zpaxos::message_t &message, broadcast_range_e range, broadcast_type_e type) final {
            if (type == broadcast_self_first) {
                switch (range) {
                    case broadcast_voter:
                        if (voter_)
                            send(my_node_id_, message);
                        break;

                    case broadcast_follower:
                        if (!voter_)
                            send(my_node_id_, message);
                        break;

                    case broadcast_all:
                        send(my_node_id_, message);
                        break;

                    default:
                        break;
                }
            }

            // Send to others.
            switch (range) {
                case broadcast_voter:
                    for (const auto &pair : globalMap()) {
                        if (pair.first != my_node_id_ && pair.second->voter_)
                            send(pair.second->my_node_id_, message);
                    }
                    break;

                case broadcast_follower:
                    for (const auto &pair : globalMap()) {
                        if (pair.first != my_node_id_ && !pair.second->voter_)
                            send(pair.second->my_node_id_, message);
                    }
                    break;

                case broadcast_all:
                    for (const auto &pair : globalMap()) {
                        if (pair.first != my_node_id_)
                            send(pair.second->my_node_id_, message);
                    }
                    break;

                default:
                    break;
            }

            if (type == broadcast_self_last) {
                switch (range) {
                    case broadcast_voter:
                        if (voter_)
                            send(my_node_id_, message);
                        break;

                    case broadcast_follower:
                        if (!voter_)
                            send(my_node_id_, message);
                        break;

                    case broadcast_all:
                        send(my_node_id_, message);
                        break;

                    default:
                        break;
                }
            }
            return 0;
        }

        bool is_voter(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                      const zpaxos::node_id_t &node_id) final {
            if (node_id == my_node_id_)
                return voter_;

            const auto it = globalMap().find(node_id);
            if (it != globalMap().end())
                return it->second->voter_;
            return false;
        }

        bool is_all_peer(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                         const std::set<zpaxos::node_id_t> &node_set) final {
            for (const auto &pair : globalMap()) {
                if (node_set.find(pair.first) == node_set.end())
                    return false;
            }
            return true;
        }

        bool is_all_voter(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                          const std::set<zpaxos::node_id_t> &node_set) final {
            for (const auto &pair : globalMap()) {
                if (pair.second->voter_ && node_set.find(pair.first) == node_set.end())
                    return false;
            }
            return true;
        }

        bool is_quorum(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                       const std::set<zpaxos::node_id_t> &node_set) final {
            auto hitCnt = 0, totalCnt = 0;
            for (const auto &pair : globalMap()) {
                if (pair.second->voter_) {
                    ++totalCnt;
                    if (node_set.find(pair.first) != node_set.end())
                        ++hitCnt;
                }
            }
            return hitCnt >= totalCnt / 2 + 1;
        }

    };

}
