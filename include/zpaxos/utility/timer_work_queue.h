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

#include <cassert>
#include <cstddef>
#include <vector>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <utility>
#include <functional>
#include <memory>
#include <stdexcept>

#include "common_define.h"
#include "timer_heap.h"
#include "time.h"

namespace utility {

    class CtimerWorkQueue final {

    NO_COPY_MOVE(CtimerWorkQueue);

    public:

#pragma pack(push, 8)

        ALIGNED_TYPE(struct, 16) tag_task final {

        private:

            void *run_ctx_;

            void (*run_)(void *);

            void *del_ctx_;

            void (*del_)(void *);

        public:

            tag_task()
                    : run_ctx_(nullptr), run_(nullptr), del_ctx_(nullptr), del_(nullptr) {}

            tag_task(void *run_ctx, void (*run)(void *), void *del_ctx, void (*del)(void *))
                    : run_ctx_(run_ctx), run_(run), del_ctx_(del_ctx), del_(del) {}

            tag_task(const tag_task &another) = default;

            tag_task(tag_task &&another) noexcept
                    : run_ctx_(another.run_ctx_), run_(another.run_),
                      del_ctx_(another.del_ctx_), del_(another.del_) {
                another.run_ctx_ = nullptr;
                another.run_ = nullptr;
                another.del_ctx_ = nullptr;
                another.del_ = nullptr;
            }

            ~tag_task() = default;

            tag_task &operator=(const tag_task &another) = default;

            tag_task &operator=(tag_task &&another) noexcept {
                run_ctx_ = another.run_ctx_;
                run_ = another.run_;
                del_ctx_ = another.del_ctx_;
                del_ = another.del_;
                another.run_ctx_ = nullptr;
                another.run_ = nullptr;
                another.del_ctx_ = nullptr;
                another.del_ = nullptr;
                return *this;
            }

            explicit operator bool() const {
                return run_ != nullptr;
            }

            void call() {
                if (run_ != nullptr)
                    run_(run_ctx_);
            }

            void fin() {
                if (del_ != nullptr)
                    del_(del_ctx_);
            }

        } task_t;

#pragma pack(pop)

        static_assert(32 == sizeof(task_t), "Error align/size of task_t.");

        // The inherited class should has private destructor to prevent alloc on stack.
        template<class T>
        class Ctask : public Callocator {

        NO_COPY_MOVE(Ctask);

        protected:

            Ctask() = default;
            ~Ctask() override = default;

        private:

            static void run_routine(void *ctx) {
                auto task = reinterpret_cast<T *>(ctx);
                task->run();
            }

            static void del_routine(void *ctx) {
                auto task = reinterpret_cast<T *>(ctx);
                delete task;
            }

        public:

            // Caution: Must call this function with object by new.
            CtimerWorkQueue::task_t gen_task() {
                return std::move(CtimerWorkQueue::task_t(this, Ctask::run_routine, this, Ctask::del_routine));
            }

        };

        class Cfunction final : public Ctask<Cfunction> {

        NO_COPY_MOVE(Cfunction);

        private:

            std::function<void()> func_;
            std::function<void()> cleanup_;

            void run() {
                func_();
            }

            ~Cfunction() final {
                cleanup_();
            }

            friend class Ctask;

        public:

            Cfunction(std::function<void()> &&func, std::function<void()> &&cleanup)
                    : func_(std::forward<std::function<void()>>(func)),
                      cleanup_(std::forward<std::function<void()>>(cleanup)) {}

        };

    private:

        const size_t worker_thread_number_;
        const size_t timer_thread_number_;
        const bool timer_direct_run_;

        std::vector<std::thread> threads_;
        volatile bool exit_;

        std::mutex work_lock_;
        std::condition_variable work_cv_;
        std::deque<task_t> work_tasks_;

        bool get_next_work_task(task_t &task) {
            std::unique_lock<std::mutex> lck(work_lock_);
            while (LIKELY(!exit_)) {
                if (UNLIKELY(work_tasks_.empty()))
                    work_cv_.wait(lck);
                else {
                    task = std::move(work_tasks_.front());
                    work_tasks_.pop_front();
                    return true;
                }
            }
            work_cv_.notify_all(); // Exit notify.
            return false;
        }

        void worker() {
            while (true) {
                task_t task;
                if (UNLIKELY(!get_next_work_task(task)))
                    break;
                task.call();
                task.fin();
            }
        }

        void push_work(task_t &&task) {
            std::unique_lock<std::mutex> lck(work_lock_);
            work_tasks_.push_back(std::forward<task_t>(task));
            work_cv_.notify_one();
        }

        std::mutex timer_lock_;
        std::condition_variable timer_cv_;
        CtimerHeap<task_t> timer_heap_;

        bool get_next_timer_task(task_t &task) {
            std::unique_lock<std::mutex> lck(timer_lock_);
            while (LIKELY(!exit_)) {
                int64_t next_trigger;
                if (UNLIKELY(!timer_heap_.peak(next_trigger)))
                    timer_cv_.wait(lck);
                else {
                    auto now_time = Ctime::steady_ms();
                    // Half and half, but wait will more often if interrupted by insert new timer.
                    if (UNLIKELY(now_time - next_trigger >= 0)) {
                        int32_t id;
                        uint32_t type;
                        bool bret = timer_heap_.pop(now_time, task, id, type);
                        assert(bret);
                        return true;
                    } else
                        timer_cv_.wait_for(lck, std::chrono::milliseconds(next_trigger - now_time));
                }
            }
            timer_cv_.notify_all(); // Exit notify.
            return false;
        }

        void timer() {
            while (true) {
                task_t task;
                if (UNLIKELY(!get_next_timer_task(task)))
                    break;
                if (LIKELY(timer_direct_run_)) {
                    task.call();
                    task.fin();
                } else
                    push_work(std::move(task));
            }
        }

        void exit() {
            exit_ = true;
            {
                std::lock_guard<std::mutex> lck(work_lock_);
                work_cv_.notify_all();
            }
            {
                std::lock_guard<std::mutex> lck(timer_lock_);
                timer_cv_.notify_all();
            }
        }

    public:

        explicit CtimerWorkQueue(size_t worker_thread_number = 1,
                                 size_t timer_thread_number = 1,
                                 bool timer_direct_run = true)
                : worker_thread_number_(worker_thread_number),
                  timer_thread_number_(timer_thread_number),
                  timer_direct_run_(timer_direct_run),
                  exit_(false) {
            if (UNLIKELY(!timer_direct_run_ && 0 == worker_thread_number_))
                throw std::invalid_argument("No worker while no direct timer run.");
            if (UNLIKELY(0 == worker_thread_number_ && 0 == timer_thread_number_))
                throw std::invalid_argument("No timer thread or worker.");
            threads_.reserve(worker_thread_number_ + timer_thread_number_);
            try {
                for (size_t i = 0; i < worker_thread_number_; ++i)
                    threads_.emplace_back(&CtimerWorkQueue::worker, this);
                for (size_t i = 0; i < timer_thread_number_; ++i)
                    threads_.emplace_back(&CtimerWorkQueue::timer, this);
            }
            catch (...) {
                exit();
                for (auto &thread : threads_)
                    thread.join();
                throw;
            }
        }

        ~CtimerWorkQueue() {
            exit();
            for (auto &thread : threads_)
                thread.join();
            // Fin all task.
            for (auto &task : work_tasks_)
                task.fin();
            work_tasks_.clear();
            task_t task;
            int32_t id;
            uint32_t type;
            while (LIKELY(timer_heap_.pop(task, id, type)))
                task.fin();
        }

        void push_trigger(task_t &&task, int64_t trigger_time) {
            if (UNLIKELY(0 == timer_thread_number_))
                throw std::runtime_error("No timer thread for timer/work task.");

            int32_t id;
            int64_t last_time;

            std::unique_lock<std::mutex> lck(timer_lock_);
            if (UNLIKELY(!timer_heap_.peak(last_time)))
                last_time = trigger_time + 1;
            timer_heap_.push(std::forward<task_t>(task), trigger_time, id);
            if (UNLIKELY(last_time - trigger_time >= 0)) {
                if (LIKELY(1 == timer_thread_number_))
                    timer_cv_.notify_one();
                else
                    timer_cv_.notify_all();
            }
        }

        void push_timeout(task_t &&task, uint32_t timeout_ms = 0) {
            if (LIKELY(0 == timeout_ms && worker_thread_number_ != 0))
                push_work(std::forward<task_t>(task));
            else
                push_trigger(std::forward<task_t>(task), Ctime::steady_ms() + timeout_ms);
        }

        void clear_all_task() {
            // Move out and invoke fin().
            std::vector<task_t> tasks;
            {
                std::lock_guard<std::mutex> lck(work_lock_);
                tasks.reserve(work_tasks_.size());
                for (auto &task : work_tasks_)
                    tasks.emplace_back(task);
                work_tasks_.clear();
            }
            for (auto &task : tasks)
                task.fin();
            tasks.clear();

            {
                std::lock_guard<std::mutex> lck(timer_lock_);
                tasks.reserve(timer_heap_.size());
                task_t task;
                int32_t id;
                uint32_t type;
                while (LIKELY(timer_heap_.pop(task, id, type)))
                    tasks.emplace_back(task);
                // No need to notify the CV, because we have no tasks in heap.
            }
            for (auto &task : tasks)
                task.fin();
            tasks.clear();
        }

    };

}
