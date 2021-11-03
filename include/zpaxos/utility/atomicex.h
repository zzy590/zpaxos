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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <atomic>
#include <thread>
#include <memory>

#include "common_define.h"

namespace utility {

    static const size_t LOCK_SPIN_COUNT = 2000;

    template<class T>
    class CatomicLatchCounter final {

    NO_COPY_MOVE(CatomicLatchCounter);

    private:

        std::atomic<T> &counter_;
        const T my_count_;

    public:

        explicit CatomicLatchCounter(std::atomic<T> &counter)
                : counter_(counter), my_count_(counter_++) {}

        ~CatomicLatchCounter() {
            --counter_;
        }

        inline explicit operator T() {
            return my_count_;
        }

    };

    template<class T>
    class CatomicExtreme final {

    NO_COPY_MOVE(CatomicExtreme);

    private:

        std::atomic<T> value_;

    public:

        explicit CatomicExtreme(T value)
                : value_(value) {}

        inline bool record_max(T new_value) {
            auto old_value = value_.load(std::memory_order_relaxed);
            if (UNLIKELY(new_value > old_value)) {
                while (UNLIKELY(!value_.compare_exchange_weak(old_value, new_value))) {
                    if (UNLIKELY(old_value >= new_value))
                        return false;
                }
                return true;
            }
            return false;
        }

        inline bool record_min(T new_value) {
            auto old_value = value_.load(std::memory_order_relaxed);
            if (UNLIKELY(new_value < old_value)) {
                while (UNLIKELY(!value_.compare_exchange_weak(old_value, new_value))) {
                    if (UNLIKELY(old_value <= new_value))
                        return false;
                }
                return true;
            }
            return false;
        }

        inline explicit operator std::atomic<T> &() {
            return value_;
        }

    };

    class CspinLock final {

    NO_COPY_MOVE(CspinLock)

    private:

        std::atomic_flag lock_;

    public:

        inline void lock() {
            size_t spin = 0;
            while (UNLIKELY(lock_.test_and_set(std::memory_order_acquire))) {
                if (UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
                    std::this_thread::yield();
                    spin = 0;
                }
            }
        }

        inline bool trylock() {
            return !lock_.test_and_set(std::memory_order_acquire);
        }

        inline void unlock() {
            lock_.clear(std::memory_order_release);
        }

        CspinLock() {
            unlock();
        }

    };

    class CautoSpinLock final {

    NO_COPY(CautoSpinLock)

    private:

        CspinLock *lock_;

    public:

        explicit CautoSpinLock(CspinLock &lock)
                : lock_(&lock) {
            lock_->lock();
        }

        CautoSpinLock(CautoSpinLock &&another) noexcept
                : lock_(another.lock_) {
            another.lock_ = nullptr;
        }

        CautoSpinLock &operator=(CautoSpinLock &&another) = delete;

        ~CautoSpinLock() {
            if (LIKELY(lock_ != nullptr))
                lock_->unlock();
        }

        inline void unlock() {
            if (LIKELY(lock_ != nullptr)) {
                lock_->unlock();
                lock_ = nullptr;
            }
        }

    };

    template<size_t lock_count>
    class CspinLockArray final {

    NO_COPY_MOVE(CspinLockArray)

    private:

        ALIGNED_TYPE(struct, 64) tag_lock_array {
            CspinLock locks[lock_count];
        } lock_array_t;

        static_assert(0 == sizeof(lock_array_t) % 64, "Error align of utility::CspinLockArray::lock_array_t.");

        lock_array_t lock_array_;

    public:

        CspinLockArray() = default;

        inline CautoSpinLock obtain(size_t id) {
            assert(id < lock_count);

            return std::move(CautoSpinLock(lock_array_.locks[id]));
        }

    };

    // Nonfair writer-first spin read-write-lock.
    class CspinRWLock final {

    NO_COPY_MOVE(CspinRWLock);

    private:

        static const uintptr_t RWLOCK_PART_BITS = sizeof(uintptr_t) * 4;
        static const uintptr_t RWLOCK_PART_OVERFLOW = static_cast<uintptr_t>(1) << RWLOCK_PART_BITS;
        static const uintptr_t RWLOCK_PART_MASK = RWLOCK_PART_OVERFLOW - 1;
        static const uintptr_t RWLOCK_PART_MAX = RWLOCK_PART_OVERFLOW - 1;
        static const uintptr_t RWLOCK_WRITE_ADD = 1;
        static const uintptr_t RWLOCK_READ_ADD = RWLOCK_PART_OVERFLOW;
#define RWLOCK_READ_COUNT(_x) ((_x) >> RWLOCK_PART_BITS)
#define RWLOCK_WRITE_COUNT(_x) ((_x) & RWLOCK_PART_MASK)

        std::atomic<uintptr_t> lock_counter_; // High part read lock, low part write lock.
        std::atomic<std::thread::id> writer_;

    public:

        CspinRWLock()
                : lock_counter_(0), writer_(std::thread::id()) {}

        bool read_lock(bool wait = true) {
            auto old_cnt = lock_counter_.load(std::memory_order_acquire);
            // CAS or spin wait.
            while (true) {
                if (UNLIKELY(RWLOCK_WRITE_COUNT(old_cnt) > 0)) {
                    // Someone hold the write lock, or wait for write.
                    // Check self reenter first.
                    if (UNLIKELY(writer_.load(std::memory_order_acquire) == std::this_thread::get_id())) {
                        // Reenter(Single thread scope).
                        assert(RWLOCK_READ_COUNT(old_cnt) < RWLOCK_PART_MAX);
                        if (UNLIKELY(!lock_counter_.compare_exchange_strong(old_cnt, old_cnt + RWLOCK_READ_ADD))) {
                            assert(false);
                            lock_counter_ += RWLOCK_READ_ADD;
                        }
                        return true;
                    }
                    // Other thread hold the write lock, or wait for write.
                    if (UNLIKELY(!wait))
                        return false;
                    size_t spin = 0;
                    do {
                        if (UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
                            std::this_thread::yield();
                            spin = 0;
                        }
                    } while (LIKELY(RWLOCK_WRITE_COUNT(old_cnt = lock_counter_.load(std::memory_order_acquire)) > 0));
                }
                assert(RWLOCK_READ_COUNT(old_cnt) < RWLOCK_PART_MAX);
                if (LIKELY(lock_counter_.compare_exchange_weak(old_cnt, old_cnt + RWLOCK_READ_ADD)))
                    return true;
            }
        }

        void read_unlock() {
            lock_counter_ -= RWLOCK_READ_ADD;
        }

        bool write_lock(bool wait = true) {
            auto old_cnt = lock_counter_.load(std::memory_order_acquire);
            // CAS or spin wait.
            while (true) {
                if (LIKELY(0 == old_cnt)) { // No holder.
                    if (LIKELY(lock_counter_.compare_exchange_weak(old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
                        writer_.store(std::this_thread::get_id(), std::memory_order_release);
                        return true;
                    }
                } else if (LIKELY(0 == RWLOCK_WRITE_COUNT(old_cnt))) {
                    // No zero, zero writer, so someone hold read lock.
                    if (UNLIKELY(!wait))
                        return false;
                    // Add write mark and wait reader exit.
                    if (LIKELY(lock_counter_.compare_exchange_weak(old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
                        writer_.store(std::this_thread::get_id(), std::memory_order_release);
                        size_t spin = 0;
                        do {
                            if (UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
                                std::this_thread::yield();
                                spin = 0;
                            }
                        } while (LIKELY(
                                RWLOCK_READ_COUNT(old_cnt = lock_counter_.load(std::memory_order_acquire)) > 0));
                        return true;
                    }
                } else {
                    // Someone hold write lock, or wait for write.
                    // Check self reenter first.
                    if (UNLIKELY(writer_.load(std::memory_order_acquire) == std::this_thread::get_id())) {
                        // Reenter(Single thread scope).
                        if (UNLIKELY(!lock_counter_.compare_exchange_strong(old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
                            assert(false);
                            lock_counter_ += RWLOCK_WRITE_ADD;
                        }
                        return true;
                    }
                    // Other thread hold the write lock, or wait for write.
                    if (UNLIKELY(!wait))
                        return false;
                    size_t spin = 0;
                    do {
                        if (UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
                            std::this_thread::yield();
                            spin = 0;
                        }
                    } while (LIKELY(RWLOCK_WRITE_COUNT(old_cnt = lock_counter_.load(std::memory_order_acquire)) > 0));
                }
            }
        }

        void write_unlock() {
            // Single thread scope.
            auto old_cnt = lock_counter_.load(std::memory_order_acquire);
            if (LIKELY(1 == RWLOCK_WRITE_COUNT(old_cnt))) // Last one.
                writer_.store(std::thread::id(), std::memory_order_release);
            if (UNLIKELY(!lock_counter_.compare_exchange_strong(old_cnt, old_cnt - RWLOCK_WRITE_ADD))) {
                assert(false);
                lock_counter_ -= RWLOCK_WRITE_ADD;
            }
        }

    };

    class CautoSpinRWLock final {

    NO_COPY_MOVE(CautoSpinRWLock);

    private:

        CspinRWLock &lock_;
        bool write_;

    public:

        explicit CautoSpinRWLock(CspinRWLock &lock, bool write = false)
                : lock_(lock), write_(write) {
            if (UNLIKELY(write_))
                lock_.write_lock(true);
            else
                lock_.read_lock(true);
        }

        ~CautoSpinRWLock() {
            if (UNLIKELY(write_))
                lock_.write_unlock();
            else
                lock_.read_unlock();
        }

        bool downgrade() {
            if (UNLIKELY(!write_))
                return false;

            if (UNLIKELY(!lock_.read_lock(false)))
                return false;
            lock_.write_unlock();

            write_ = false;
            return true;
        }

    };

    class CautoCrabbingRWLock final {

    NO_COPY_MOVE(CautoCrabbingRWLock);

    private:

        CspinRWLock *lock_;
        bool write_;

    public:

        CautoCrabbingRWLock()
                : lock_(nullptr), write_(false) {}

        void unlock() {
            if (LIKELY(lock_ != nullptr)) {
                if (UNLIKELY(write_))
                    lock_->write_unlock();
                else
                    lock_->read_unlock();
                lock_ = nullptr;
            }
        }

        ~CautoCrabbingRWLock() {
            unlock();
        }

        void crabbing(CspinRWLock *lock, bool write = false) {
            if (UNLIKELY(nullptr == lock))
                unlock();
            else {
                if (UNLIKELY(write))
                    lock->write_lock(true);
                else
                    lock->read_lock(true);
                unlock();
                lock_ = lock;
                write_ = write;
            }
        }

    };


    //
    // Fast array based lock-free queue.
    // Note: 'T' should have default constructor and be small for performance.
    //

    template<class T>
    class CatomicQueue final {

    NO_COPY_MOVE(CatomicQueue);

    private:

        static const size_t ATOMIC_QUEUE_SPIN_COUNT = 200; // CAS of queue is heavy so spin count is small.

        const uintptr_t slot_number_;
        const std::unique_ptr<T[]> slots_;
        const std::unique_ptr<std::atomic<bool>[]> flags_;

        std::atomic<uintptr_t> head_;
        std::atomic<uintptr_t> tail_;

        inline uintptr_t constrain(uintptr_t idx) {
            return !(slot_number_ & (slot_number_ - 1)) ?
                   idx & (slot_number_ - 1) : (idx < slot_number_ ? idx : idx % slot_number_);
        }

    public:

        // Note: Real queue depth is 'slot_number'-1.
        explicit CatomicQueue(size_t slot_number)
                : slot_number_(slot_number), slots_(new T[slot_number]), flags_(new std::atomic<bool>[slot_number]),
                  head_(0), tail_(0) {
            for (size_t i = 0; i < slot_number_; ++i)
                flags_[i].store(false, std::memory_order_release);
        }

        bool enqueue(const T &value) {
            size_t spin = 0;

            auto now_tail = tail_.load(std::memory_order_acquire);
            while (true) {
                // Load head every time.
                auto now_head = head_.load(std::memory_order_acquire);

                // Check full(Use signed compare to deal with overflow).
                if (UNLIKELY(static_cast<intptr_t>(now_tail - now_head) >= static_cast<intptr_t>(slot_number_ - 1)))
                    return false;

                // Wait read done.
                auto idx = constrain(now_tail);
                if (UNLIKELY(flags_[idx].load(std::memory_order_acquire)))
                    now_tail = tail_.load(std::memory_order_acquire);
                else if (LIKELY(tail_.compare_exchange_weak(now_tail, now_tail + 1))) {
                    slots_[idx] = value;
                    flags_[idx].store(true, std::memory_order_release);
                    return true;
                }

                // Spin.
                if (UNLIKELY(++spin == ATOMIC_QUEUE_SPIN_COUNT)) {
                    std::this_thread::yield();
                    spin = 0;
                }
            }
        }

        bool dequeue(T &value) {
            size_t spin = 0;

            auto now_head = head_.load(std::memory_order_acquire);
            while (true) {
                // Load tail every time.
                auto now_tail = tail_.load(std::memory_order_acquire);

                // Check empty.
                if (UNLIKELY(static_cast<intptr_t>(now_head - now_tail) >= 0))
                    return false;

                // Wait write done.
                auto idx = constrain(now_head);
                if (UNLIKELY(!flags_[idx].load(std::memory_order_acquire)))
                    now_head = head_.load(std::memory_order_acquire);
                else if (LIKELY(head_.compare_exchange_weak(now_head, now_head + 1))) {
                    value = std::move(slots_[idx]);
                    flags_[idx].store(false, std::memory_order_release);
                    return true;
                }

                // Spin.
                if (UNLIKELY(++spin == ATOMIC_QUEUE_SPIN_COUNT)) {
                    std::this_thread::yield();
                    spin = 0;
                }
            }
        }

    };

}
