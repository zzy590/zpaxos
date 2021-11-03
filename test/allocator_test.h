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
// Created by zzy on 2020/3/28.
//

#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstdio>

using namespace std;

namespace utility {

    class CglobalAllocator {

    private:

        static void *allocate(std::size_t sz) {
            //cout << "CglobalAllocator::allocate " << sz << endl;
            return ::malloc(sz);
        }

        static void *allocate_aligned(std::size_t sz) {
            //cout << "CglobalAllocator::allocate_aligned " << sz << endl;
            return ::malloc(sz);
        }

        static void deallocate(void *ptr) {
            //cout << "CglobalAllocator::deallocate " << ptr << endl;
            ::free(ptr);
        }

        // Set object size and prefer reserved number for allocation.
        static void reserve(std::size_t sz, std::size_t max_number) {
            // Ignore.
            //cout << "CglobalAllocator::reserve " << sz << " " << max_number << endl;
        }

        template<class T>
        friend
        class CallocatorImpl;

    };

}
