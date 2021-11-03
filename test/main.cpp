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
// Created by zzy on 2019-06-30.
//

#include <iostream>

using namespace std;

#include "allocator_test.h"

#include "network_unit_test.h"
#include "storage_unit_test.h"
#include "utility_unit_test.h"
#include "zpaxos_unit_test.h"

int main() {
    std::cout << "Hello, World!" << std::endl;

//    storage::CunitTest::test_memory_storage();
//    network::CunitTest::test_memory_network();

//    utility::CunitTest::test_CtimerWorkQueue();
//    utility::CunitTest::test_CresetableTimeoutNotify();
//    utility::CunitTest::test_rwlock();
//    utility::CunitTest::test_fast_hash();
//    utility::CunitTest::test_queue();
//    utility::CunitTest::test_correctness_queue();
//    utility::CunitTest::test_allocator();

//    zpaxos::CunitTest::test_single_instance();
//    zpaxos::CunitTest::test_multi_instance();
//    zpaxos::CunitTest::test_multi_propose_correctness();
//    zpaxos::CunitTest::test_multi_propose_bad_network_correctness();
//    zpaxos::CunitTest::test_reset_instance();
    zpaxos::CunitTest::test_snapshot();

    return 0;
}
