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
// Created by zzy on 2019-02-07.
//

#pragma once

#include <iostream>
#include <string>
#include <memory>

using namespace std;

#include "zpaxos/base.h"
#include "zpaxos/storage/memory_storage.h"

namespace storage {

    class CunitTest {

    NO_CONSTRUCTOR(CunitTest);
    NO_COPY_MOVE(CunitTest);

    public:

        static void test_memory_storage() {
            std::unique_ptr<zpaxos::Cstorage> storage(new CmemoryStorage());

            std::string data("hello world.");
            utility::Cbuffer buf(data.c_str(), data.length() + 1);
            zpaxos::value_t value;
            value.state_machine_id = 999;
            value.buffer = buf;
            zpaxos::state_t state;
            state.value = value;

            int iret = storage->put(0, 0, state, zpaxos::write_options_t());
            cout << "put " << iret << endl;
            iret = storage->put(0, 1, state, zpaxos::write_options_t());
            cout << "put " << iret << endl;

            zpaxos::instance_id_t instance_id;
            iret = storage->get_max_instance_id(0, instance_id);
            cout << "max " << iret << " is " << instance_id << endl;

            zpaxos::state_t state_read;
            iret = storage->get(0, 1, state_read);
            cout << "get " << iret << " is " << reinterpret_cast<const char *>(state_read.value.buffer.data()) << endl;
        }

    };

}
