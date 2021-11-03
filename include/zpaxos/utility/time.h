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
// Created by zzy on 2018-12-31.
//

#pragma once

#include <cstdint>
#include <thread>
#include <chrono>

#include "common_define.h"

namespace utility {

    class Ctime final {

    NO_CONSTRUCTOR(Ctime);
    NO_COPY_MOVE(Ctime);

    public:

        static int64_t system_ms() {
            auto now_time = chrono::system_clock::now();
            return chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch()).count();
        }

        static int64_t steady_ms() {
            auto now_time = chrono::steady_clock::now();
            return chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch()).count();
        }

        static void sleep_ms(int64_t time_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(time_ms));
        }

        static int64_t steady_us() {
            auto now_time = chrono::steady_clock::now();
            return chrono::duration_cast<chrono::microseconds>(now_time.time_since_epoch()).count();
        }

    };

}
