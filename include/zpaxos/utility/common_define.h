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
// Created by zzy on 2018-12-30.
//

#pragma once

//
// Disable/default class copy & move.
//

#define NO_CONSTRUCTOR(_class) \
        public: \
            _class() = delete; \
        private:

#define NO_COPY_MOVE(_class) \
        public: \
            _class(const _class &another) = delete; \
            _class(_class &&another) = delete; \
            _class &operator=(const _class &another) = delete; \
            _class &operator=(_class &&another) = delete; \
        private:

#define NO_COPY(_class) \
        public: \
            _class(const _class &another) = delete; \
            _class &operator=(const _class &another) = delete; \
        private:

#define NO_MOVE(_class) \
        public: \
            _class(_class &&another) = delete; \
            _class &operator=(_class &&another) = delete; \
        private:

#define DEFAULT_COPY_MOVE(_class) \
        public: \
            _class(const _class &another) = default; \
            _class(_class &&another) = default; \
            _class &operator=(const _class &another) = default; \
            _class &operator=(_class &&another) = default; \
        private:

#define DEFAULT_COPY(_class) \
        public: \
            _class(const _class &another) = default; \
            _class &operator=(const _class &another) = default; \
        private:

#define DEFAULT_MOVE(_class) \
        public: \
            _class(_class &&another) = default; \
            _class &operator=(_class &&another) = default; \
        private:

//
// Align.
//

#if defined(_MSC_VER)
#  define ALIGNED_(x) __declspec(align(x))
#elif defined(__GNUC__)
#  define ALIGNED_(x) __attribute__ ((aligned(x)))
#else
#  error "Unknown compiler."
#endif

#define ALIGNED_TYPE(t, x) typedef t ALIGNED_(x)

/* Usage
 *
 * #pragma pack(push, 8)
 * ALIGNED_TYPE(struct, 16) tag_xxx {
 * } xxx_t;
 * #pragma pack(pop)
 *
 */

//
// Likely.
//

#if defined(__GNUC__) && __GNUC__ >= 4
#  define LIKELY(x)   (__builtin_expect((x), 1))
#  define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#  define LIKELY(x)   (x)
#  define UNLIKELY(x) (x)
#endif
