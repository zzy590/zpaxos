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
// Created by zzy on 2019-02-04.
//

#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>
#include <utility>
#include <mutex>

using namespace std;

#include "zpaxos/network/memory_peer_network.h"

namespace network {

    class CunitTest {

    NO_CONSTRUCTOR(CunitTest);
    NO_COPY_MOVE(CunitTest);

    private:

        static std::mutex &output_mutex() {
            static std::mutex lock;
            return lock;
        }

        static zpaxos::message_t prepareMsg(const string &str) {
            zpaxos::message_t msg;
            msg.value.buffer.set(str.c_str(), str.length() + 1);
            return msg;
        }

    public:

        static void test_memory_network() {
            vector<unique_ptr<CfixedPeerNetwork>> peers;
            zpaxos::node_id_t id = 1;

            for (int i = 0; i < 3; ++i) {
                peers.emplace_back(unique_ptr<CfixedPeerNetwork>(new CfixedPeerNetwork(id++, true)));
                peers.back()->set_message_callback([i](zpaxos::message_t &&msg) {
                    std::lock_guard<std::mutex> lck(output_mutex());
                    cout << "voter-" << i << " recv: "
                         << reinterpret_cast<const char *>(msg.value.buffer.data())
                         << endl;
                    return 0;
                });
            }

            for (int i = 0; i < 2; ++i) {
                peers.emplace_back(unique_ptr<CfixedPeerNetwork>(new CfixedPeerNetwork(id++, false)));
                peers.back()->set_message_callback([i](zpaxos::message_t &&msg) {
                    std::lock_guard<std::mutex> lck(output_mutex());
                    cout << "follower-" << i << " recv: "
                         << reinterpret_cast<const char *>(msg.value.buffer.data())
                         << endl;
                    return 0;
                });
            }

            peers.front()->send(2, prepareMsg("send to 2"));

            peers.front()->broadcast(prepareMsg("broadcast voter 1 first"),
                                     zpaxos::Ccommunication::broadcast_voter,
                                     zpaxos::Ccommunication::broadcast_self_first);
            peers.front()->broadcast(prepareMsg("broadcast voter 1 last"),
                                     zpaxos::Ccommunication::broadcast_voter,
                                     zpaxos::Ccommunication::broadcast_self_last);
            peers.front()->broadcast(prepareMsg("broadcast voter 1 no"),
                                     zpaxos::Ccommunication::broadcast_voter,
                                     zpaxos::Ccommunication::broadcast_no_self);

            peers.front()->broadcast(prepareMsg("broadcast follower 1 first"),
                                     zpaxos::Ccommunication::broadcast_follower,
                                     zpaxos::Ccommunication::broadcast_self_first);
            peers.front()->broadcast(prepareMsg("broadcast follower 1 last"),
                                     zpaxos::Ccommunication::broadcast_follower,
                                     zpaxos::Ccommunication::broadcast_self_last);
            peers.front()->broadcast(prepareMsg("broadcast follower 1 no"),
                                     zpaxos::Ccommunication::broadcast_follower,
                                     zpaxos::Ccommunication::broadcast_no_self);

            peers.front()->broadcast(prepareMsg("broadcast all 1 first"),
                                     zpaxos::Ccommunication::broadcast_all,
                                     zpaxos::Ccommunication::broadcast_self_first);
            peers.front()->broadcast(prepareMsg("broadcast all 1 last"),
                                     zpaxos::Ccommunication::broadcast_all,
                                     zpaxos::Ccommunication::broadcast_self_last);
            peers.front()->broadcast(prepareMsg("broadcast all 1 no"),
                                     zpaxos::Ccommunication::broadcast_all,
                                     zpaxos::Ccommunication::broadcast_no_self);

            cout << "voter 1 is voter? " << peers.front()->is_voter(0, 0, 1) << endl;

            set<zpaxos::node_id_t> peer_set;
            peer_set.insert(1);
            peer_set.insert(2);
            peer_set.insert(3);
            peer_set.insert(4);
            peer_set.insert(5);
            cout << "12345 is all peer? " << peers.front()->is_all_peer(0, 0, peer_set) << endl;
            peer_set.erase(2);
            cout << "1345 is all peer? " << peers.front()->is_all_peer(0, 0, peer_set) << endl;

            set<zpaxos::node_id_t> voter_set;
            voter_set.insert(1);
            voter_set.insert(2);
            voter_set.insert(3);
            cout << "123 is all voter? " << peers.front()->is_all_voter(0, 0, voter_set) << endl;
            voter_set.erase(2);
            cout << "13 is all voter? " << peers.front()->is_all_voter(0, 0, voter_set) << endl;

            set<zpaxos::node_id_t> quorum_set;
            quorum_set.insert(1);
            quorum_set.insert(2);
            cout << "12 is all quorum? " << peers.front()->is_quorum(0, 0, quorum_set) << endl;
            quorum_set.erase(2);
            cout << "1 is quorum? " << peers.front()->is_quorum(0, 0, quorum_set) << endl;
        }

    };

}
