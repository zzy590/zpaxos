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
#include <new>

#include "common_define.h"

namespace utility {

    template<class T>
    class CallocatorImpl {

    public:

        /**
         * Customized allocator should have following four function.
         */

        inline static void *allocate(std::size_t sz) {
            return T::allocate(sz);
        }

        inline static void *allocate_aligned(std::size_t sz) {
            return T::allocate_aligned(sz);
        }

        inline static void deallocate(void *ptr) {
            T::deallocate(ptr);
        }

        // Set object size and prefer reserved number for allocation.
        inline static void reserve(std::size_t sz, std::size_t max_number) {
            T::reserve(sz, max_number);
        }

        /**
         * Internal function(s).
         */

        inline static void *allocate_throw(std::size_t sz) {
            auto ptr = allocate(sz);
            if (UNLIKELY(nullptr == ptr))
                throw std::bad_alloc();
            return ptr;
        }

    protected:

        // Use protected destructor to only allow inherited class.
        virtual ~CallocatorImpl() = default;

    public:

        /**
         * Override functions of new/delete operator.
         */

        void *operator new(std::size_t size) {
            return allocate_throw(size);
        }

        void *operator new(std::size_t size, const std::nothrow_t &nothrow_value) {
            return allocate(size);
        }

        void *operator new[](std::size_t size) {
            return allocate_throw(size);
        }

        void *operator new[](std::size_t size, const std::nothrow_t &nothrow_value) {
            return allocate(size);
        }

        void operator delete(void *ptr) {
            deallocate(ptr);
        }

        void operator delete(void *ptr, const std::nothrow_t &nothrow_constant) {
            deallocate(ptr);
        }

        void operator delete[](void *ptr) {
            deallocate(ptr);
        }

        void operator delete[](void *ptr, const std::nothrow_t &nothrow_constant) {
            deallocate(ptr);
        }

        // For C++14.
        void operator delete(void *ptr, std::size_t size) {
            deallocate(ptr);
        }

        void operator delete(void *ptr, std::size_t size, const std::nothrow_t &nothrow_constant) {
            deallocate(ptr);
        }

        void operator delete[](void *ptr, std::size_t size) {
            deallocate(ptr);
        }

        void operator delete[](void *ptr, std::size_t size, const std::nothrow_t &nothrow_constant) {
            deallocate(ptr);
        }

    };

    // Note: Implement this class and define before this header to manage all memory allocations.
    class CglobalAllocator;

    typedef CallocatorImpl<CglobalAllocator> Callocator;

}
