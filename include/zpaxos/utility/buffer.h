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

#include <cstddef>
#include <cstring>
#include <utility>

#include "allocator.h"

namespace utility {

    class Cslice final : public Callocator {

    private:

        void *data_;
        size_t length_;

    public:

        Cslice()
                : data_(nullptr), length_(0) {}

        Cslice(void *data, size_t length)
                : data_(data), length_(length) {}

        Cslice(const Cslice &another)
                : data_(another.data_), length_(another.length_) {}

        Cslice(Cslice &&another) noexcept
                : data_(another.data_), length_(another.length_) {}

        Cslice &operator=(const Cslice &another) {
            data_ = another.data_;
            length_ = another.length_;
            return *this;
        }

        Cslice &operator=(Cslice &&another) noexcept {
            data_ = another.data_;
            length_ = another.length_;
            return *this;
        }

        void set(void *data, size_t length) {
            data_ = data;
            length_ = length;
        }

        void *data() const {
            return data_;
        }

        size_t length() const {
            return length_;
        }

        explicit operator bool() const {
            return length_ != 0;
        }

    };

    template<size_t internal_length>
    class CbufferImpl final : public Callocator {

    private:

        // Buffer length.
        size_t length_;

        // Hold the data if less than internal_length.
        unsigned char internal_buffer_[internal_length];

        // Hold the data larger than internal_length.
        void *alloc_ptr_;
        size_t alloc_length_;

    public:

        CbufferImpl()
                : length_(0), alloc_ptr_(nullptr), alloc_length_(0) {}

        explicit CbufferImpl(size_t length) {
            length_ = length;
            if (length_ <= internal_length) {
                alloc_ptr_ = nullptr;
                alloc_length_ = 0;
            } else {
                alloc_ptr_ = Callocator::allocate_throw(length_);
                alloc_length_ = length_;
            }
        }

        CbufferImpl(const void *data, size_t length) {
            length_ = length;
            if (length_ <= internal_length) {
                if (length_ > 0)
                    ::memcpy(internal_buffer_, data, length_);
                alloc_ptr_ = nullptr;
                alloc_length_ = 0;
            } else {
                alloc_ptr_ = Callocator::allocate_throw(length_);
                ::memcpy(alloc_ptr_, data, length_);
                alloc_length_ = length_;
            }
        }

        explicit CbufferImpl(const Cslice &slice)
                : CbufferImpl(slice.data(), slice.length()) {}

        CbufferImpl(const CbufferImpl &another) {
            length_ = another.length_;
            if (length_ <= internal_length) {
                if (length_ > 0)
                    ::memcpy(internal_buffer_, another.internal_buffer_, length_);
                alloc_ptr_ = nullptr;
                alloc_length_ = 0;
            } else {
                alloc_ptr_ = Callocator::allocate_throw(length_);
                ::memcpy(alloc_ptr_, another.alloc_ptr_, length_);
                alloc_length_ = length_;
            }
        }

        CbufferImpl(CbufferImpl &&another) noexcept
                : length_(another.length_), alloc_ptr_(another.alloc_ptr_), alloc_length_(another.alloc_length_) {
            if (length_ <= internal_length)
                ::memcpy(internal_buffer_, another.internal_buffer_, length_);
            another.length_ = 0;
            another.alloc_ptr_ = nullptr;
            another.alloc_length_ = 0;
        }

        ~CbufferImpl() final {
            Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
        }

        CbufferImpl &operator=(const CbufferImpl &another) {
            length_ = another.length_;
            if (length_ <= internal_length) {
                if (length_ > 0)
                    ::memcpy(internal_buffer_, another.internal_buffer_, length_);
                // Leave alloc_ptr and alloc_length as former state.
            } else if (length_ <= alloc_length_)
                ::memcpy(alloc_ptr_, another.alloc_ptr_, length_);
            else {
                Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
                alloc_ptr_ = Callocator::allocate_throw(length_);
                ::memcpy(alloc_ptr_, another.alloc_ptr_, length_);
                alloc_length_ = length_;
            }
            return *this;
        }

        CbufferImpl &operator=(CbufferImpl &&another) noexcept {
            length_ = another.length_;
            if (length_ <= internal_length) {
                if (length_ > 0)
                    ::memcpy(internal_buffer_, another.internal_buffer_, length_);
            }
            Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
            alloc_ptr_ = another.alloc_ptr_;
            alloc_length_ = another.alloc_length_;
            another.length_ = 0;
            another.alloc_ptr_ = nullptr;
            another.alloc_length_ = 0;
            return *this;
        }

        bool operator==(const CbufferImpl &another) const {
            return length_ == another.length_ && 0 == ::memcmp(data(), another.data(), length_);
        }

        void set(const void *data, size_t length) {
            length_ = length;
            if (length_ <= internal_length) {
                if (length_ > 0)
                    ::memcpy(internal_buffer_, data, length_);
                // Leave alloc_ptr and alloc_length as former state.
            } else if (length_ <= alloc_length_)
                ::memcpy(alloc_ptr_, data, length_);
            else {
                Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
                alloc_ptr_ = Callocator::allocate_throw(length_);
                ::memcpy(alloc_ptr_, data, length_);
                alloc_length_ = length_;
            }
        }

        void set(const Cslice &slice) {
            set(slice.data(), slice.length());
        }

        CbufferImpl &operator=(const Cslice &slice) {
            set(slice);
            return *this;
        }

        void resize(size_t prefer_length, const size_t valid_length = 0) {
            if (prefer_length <= internal_length) {
                // Use internal buffer.
                if (length_ > internal_length) {
                    // alloc -> internal.
                    size_t copy = valid_length > length_ ? length_ : valid_length;
                    if (copy > prefer_length)
                        copy = prefer_length;
                    if (copy > 0)
                        ::memcpy(internal_buffer_, alloc_ptr_, copy);
                    // Leave alloc_ptr and alloc_length as former state.
                }
                length_ = prefer_length;
            } else if (prefer_length <= alloc_length_) {
                // Use alloc buffer.
                if (length_ <= internal_length) {
                    // internal -> alloc
                    size_t copy = valid_length > length_ ? length_ : valid_length;
                    if (copy > prefer_length)
                        copy = prefer_length;
                    if (copy > 0)
                        ::memcpy(alloc_ptr_, internal_buffer_, copy);
                }
                length_ = prefer_length;
            } else {
                // We need to enlarge the buffer.
                size_t copy = valid_length > length_ ? length_ : valid_length;
                if (copy > prefer_length)
                    copy = prefer_length;
                if (copy > 0) {
                    void *new_buffer = Callocator::allocate_throw(prefer_length);
                    if (length_ <= internal_length) // internal -> new buffer
                        ::memcpy(new_buffer, internal_buffer_, copy);
                    else // alloc -> new buffer
                        ::memcpy(new_buffer, alloc_ptr_, copy);
                    Callocator::deallocate(alloc_ptr_);
                    length_ = prefer_length;
                    alloc_ptr_ = new_buffer;
                    alloc_length_ = length_;
                } else {
                    Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
                    length_ = prefer_length;
                    alloc_ptr_ = Callocator::allocate_throw(length_);
                    alloc_length_ = length_;
                }
            }
        }

        void free() {
            Callocator::deallocate(alloc_ptr_); // No need to check nullptr.
            length_ = 0;
            alloc_ptr_ = nullptr;
            alloc_length_ = 0;
        }

        void *data() const {
            if (length_ <= internal_length)
                return const_cast<unsigned char *>(internal_buffer_); // Remove const.
            else
                return alloc_ptr_;
        }

        size_t length() const {
            return length_;
        }

        explicit operator bool() const {
            return length_ != 0;
        }

        Cslice slice() const {
            return std::move(Cslice(data(), length_));
        }

        Cslice slice(size_t offset, size_t length) const {
            if (offset >= length_)
                return std::move(Cslice());

            if (length > length_ - offset)
                length = length_ - offset;

            return std::move(Cslice(reinterpret_cast<unsigned char *>(data()) + offset, length));
        }

        void release(void *&ptr, size_t &length, size_t &alloc_length) {
            if (length_ <= internal_length) {
                if (0 == length_)
                    ptr = nullptr;
                else {
                    ptr = Callocator::allocate_throw(length_);
                    ::memcpy(ptr, internal_buffer_, length_);
                }
                length = length_;
                alloc_length = length_;
                // Leave alloc_ptr and alloc_length as former state.
            } else {
                ptr = alloc_ptr_;
                length = length_;
                alloc_length = alloc_length_;
                alloc_ptr_ = nullptr;
                alloc_length_ = 0;
            }
            length_ = 0;
        }

    };

    typedef CbufferImpl<0x100> Cbuffer;

}
