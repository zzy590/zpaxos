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
// Created by zzy on 2019-01-01.
//

#pragma once

#include <cstdint>
#include <atomic>
#include <mutex>
#include <utility>
#include <memory>

#include "common_define.h"
#include "allocator.h"
#include "timer_work_queue.h"
#include "time.h"

namespace utility {

    template<class T>
    class CresetableTimeoutNotify {

    NO_COPY_MOVE(CresetableTimeoutNotify);

    private:

        inline void timeout_notify() {
            static_cast<T *>(this)->timeout_notify();
        }

        class Cinternal : public Callocator {

        NO_COPY_MOVE(Cinternal);

        private:

            CtimerWorkQueue &queue_;
            std::atomic<int> reference_count_;

            std::mutex timer_lock_;
            int64_t trigger_time_;
            bool timer_set_;

            std::recursive_mutex notify_lock_;
            CresetableTimeoutNotify *notify_;

            static void del(void *ctx) {
                auto internal = reinterpret_cast<Cinternal *>(ctx);

                if (UNLIKELY(0 == --internal->reference_count_))
                    delete internal;
            }

            static void run(void *ctx) {
                auto internal = reinterpret_cast<Cinternal *>(ctx);

                bool trigger = false;
                {
                    std::lock_guard<std::mutex> lck(internal->timer_lock_);
                    if (LIKELY(internal->reference_count_.load(std::memory_order_relaxed) != 1)) {
                        // Call or reset.
                        auto now = Ctime::steady_ms();
                        if (UNLIKELY(now - internal->trigger_time_ >= 0)) { // Usually reset before timeout.
                            // Trigger.
                            internal->timer_set_ = false;
                            trigger = true;
                        } else {
                            ++internal->reference_count_;
                            internal->queue_.push_trigger(
                                    std::move(CtimerWorkQueue::task_t(
                                            internal, Cinternal::run, internal, Cinternal::del)),
                                    internal->trigger_time_);
                        }
                    }
                }

                if (trigger) {
                    std::lock_guard<std::recursive_mutex> lck(internal->notify_lock_);
                    if (LIKELY(internal->notify_ != nullptr))
                        internal->notify_->timeout_notify();
                }
            }

            friend class CresetableTimeoutNotify;

        public:

            Cinternal(CtimerWorkQueue &queue, CresetableTimeoutNotify *notify)
                    : queue_(queue), reference_count_(1), trigger_time_(0), timer_set_(false), notify_(notify) {}

            // Only equal or more timeout_ms is allowed. Or may cause delay.
            void update(uint32_t timeout_ms) {
                std::lock_guard<std::mutex> lck(timer_lock_);
                trigger_time_ = Ctime::steady_ms() + timeout_ms;
                if (UNLIKELY(!timer_set_)) {
                    ++reference_count_;
                    queue_.push_trigger(
                            std::move(CtimerWorkQueue::task_t(this, Cinternal::run, this, Cinternal::del)),
                            trigger_time_);
                    timer_set_ = true;
                }
            }

            // Caution: Never hold the lock which notify routine may acquire
            //          while calling this function!
            void terminate() {
                std::lock_guard<std::recursive_mutex> lck(notify_lock_);
                notify_ = nullptr;
            }

        };

        const uint32_t timeout_ms_;
        Cinternal *internal_;

    public:

        CresetableTimeoutNotify(CtimerWorkQueue &queue, uint32_t timeout_ms)
                : timeout_ms_(timeout_ms) {
            internal_ = new Cinternal(queue, this);
        }

        // Caution: Never hold the lock which notify routine may acquire
        //          while destructing!
        virtual ~CresetableTimeoutNotify() {
            internal_->terminate();
            if (UNLIKELY(0 == --internal_->reference_count_))
                delete internal_;
        }

        // Caution: Never hold the lock which notify routine may acquire
        //          while calling this function!
        //          Call this function before destructing to prevent pure virtual function calling.
        virtual void terminate() {
            internal_->terminate();
        }

        // Set max number of timeout notifier to cache.
        static void advise_cache_number(size_t max_count) {
            Callocator::reserve(sizeof(Cinternal), max_count);
        }

        uint32_t get_timeout_ms() const {
            return timeout_ms_;
        }

        void reset() {
            internal_->update(timeout_ms_);
        }

    };

}
