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

#include <iostream>
#include <functional>
#include <random>
#include <thread>
#include <utility>
#include <vector>
#include <map>
#include <unordered_map>
#include <stdexcept>

using namespace std;

#include "zpaxos/utility/time.h"
#include "zpaxos/utility/timer_work_queue.h"
#include "zpaxos/utility/resetable_timeout_notify.h"
#include "zpaxos/utility/buffer.h"
#include "zpaxos/utility/hash.h"

#include "zpaxos/log.h"

namespace utility {

    class CunitTest {

    NO_CONSTRUCTOR(CunitTest);
    NO_COPY_MOVE(CunitTest);

    public:

        static void test_minus() {

            int64_t a, b;
            a = 0x7fffffffffffffff;
            b = a + 1;
            cout << "(" << b << ") - (" << a << ") minus " << b - a << " b " << (b > a) << endl;

        }

        static void test_CtimerWorkQueue() {

            CtimerWorkQueue queue(4, 1, false);

            auto run = [](void *run_ctx) {
                cout << "run " << run_ctx << endl;
            };
            auto del = [](void *del_ctx) {
                cout << "del " << del_ctx << endl;
            };

            queue.push_timeout(CtimerWorkQueue::task_t((void *) 0, run, (void *) 0, del));
            queue.push_timeout(CtimerWorkQueue::task_t((void *) 1, run, (void *) 1, del), 1000);
            queue.push_timeout(CtimerWorkQueue::task_t((void *) 2, run, (void *) 2, del), 1500);
            queue.push_timeout(CtimerWorkQueue::task_t((void *) 3, run, (void *) 3, del), 1000);
            queue.push_timeout(CtimerWorkQueue::task_t((void *) 4, run, (void *) 4, del), 2000);

            class CmyTask : public CtimerWorkQueue::Ctask<CmyTask> {

            private:

                ~CmyTask() {
                    cout << "~CmyTask" << endl;
                }

                void run() {
                    cout << "CmyTask run" << endl;
                }

                friend class Ctask;

            };

            CmyTask *my = new CmyTask;

            queue.push_timeout(my->gen_task(), 2500);

            Ctime::sleep_ms(3 * 1000);

            queue.push_timeout((new CtimerWorkQueue::Cfunction([]() {
                cout << "func run." << endl;
            }, []() {
                cout << "func cleanup." << endl;
            }))->gen_task());

            Ctime::sleep_ms(1000);

            queue.push_timeout((new CtimerWorkQueue::Cfunction([]() {
                cout << "func run. timeout cancel(never appeal)" << endl;
            }, []() {
                cout << "func cleanup. timeout cancel" << endl;
            }))->gen_task(), 2000);

            Ctime::sleep_ms(1000);
            queue.clear_all_task();

            Ctime::sleep_ms(1000);
        }

        static void test_CresetableTimeoutNotify() {

            class CmyTimeout : public CresetableTimeoutNotify<CmyTimeout> {

            public:

                CmyTimeout(CtimerWorkQueue &queue)
                        : CresetableTimeoutNotify(queue, 1000) {}

                ~CmyTimeout() {
                    cout << "~" << endl;
                }

                void timeout_notify() {
                    cout << "timeout" << endl;
                }

            };

            CtimerWorkQueue queue(4, 1, false);
            CmyTimeout my(queue);
            my.reset();

            for (auto i = 0; i < 3; ++i) {
                Ctime::sleep_ms(800);
                my.reset();
                cout << "reset" << endl;
            }

            cout << "wait tle" << endl;

            Ctime::sleep_ms(2 * 1000);

        }

        static void test_rwlock() {

            utility::CspinRWLock lock;

            {
                utility::CautoSpinRWLock lck0(lock);
                {
                    utility::CautoSpinRWLock lck1(lock);
                    cout << "read reenter." << endl;
                }
            }

            {
                utility::CautoSpinRWLock lck0(lock, true);
                {
                    utility::CautoSpinRWLock lck1(lock, true);
                    cout << "write reenter." << endl;
                }
            }

            {
                lock.write_lock();
                lock.read_lock();
                lock.write_unlock();
                cout << "write->read." << endl;
                lock.read_unlock();
            }

            cout << "done" << endl;

        }

        static void test_fast_hash() {

            string data("01234567");
            Cslice slice(const_cast<char *>(data.c_str()), data.length());

            cout << "fast hash: " << Chash::fast_hash(slice) << endl;

            std::hash<std::string> s_hash;

            cout << "std hash: " << s_hash(data) << endl;

            uint64_t sum = 0;

            auto start = utility::Ctime::steady_ms();

            for (int i = 0; i < 100000000; ++i)
                sum += Chash::fast_hash(slice);

            auto end = utility::Ctime::steady_ms();

            cout << "fast time: " << (end - start) << "ms" << endl;

            sum = 0;
            start = utility::Ctime::steady_ms();

            for (int i = 0; i < 100000000; ++i)
                sum += s_hash(data);

            end = utility::Ctime::steady_ms();

            cout << "std time: " << (end - start) << "ms" << endl;

            sum = 0;
            start = utility::Ctime::steady_ms();

            for (int i = 0; i < 100000000; ++i) {
                size_t hash = 0;
                for (const auto &ch : data)
                    hash += hash * 31 + ch;
                sum += hash;
            }

            end = utility::Ctime::steady_ms();

            cout << "java time: " << (end - start) << "ms" << endl;

        }

        static void test_queue() {

            size_t val = 0;
            atomic<size_t> atomic_cnt(0);
            CspinLock lock;
            mutex mu;
            CatomicQueue<int> queue(0x1000);

            vector<thread> threads;

            auto start = utility::Ctime::steady_ms();

            for (auto n = 0; n < 8; ++n) {
                threads.emplace_back([n, &queue, &atomic_cnt, &lock, &mu, &val]() {
                    for (int i = 0; i < 1000000; ++i) {
/*
                            {
                                CautoSpinLock lck(lock);
                                cout << n << " step0 " << i << endl;
                            }
*/
                        auto bret = queue.enqueue(i);
                        if (!bret)
                            cout << "GG0" << endl;
/*
                            {
                                CautoSpinLock lck(lock);
                                cout << n << " step1 " << i << endl;
                            }
*/
                        int val;
                        bret = queue.dequeue(val);
                        if (!bret)
                            cout << "GG1" << endl;
/*
                            {
                                CautoSpinLock lck(lock);
                                cout << n << " step2 " << i << endl;
                            }
*/
                        //CautoSpinLock lck(lock);
                        //unique_lock<mutex> lck(mu);
                        /*
                        ++atomic_cnt;
                        --atomic_cnt;
                        ++atomic_cnt;
                        --atomic_cnt;
                        */
                    }
                });
            }

            for (auto &thread : threads)
                thread.join();

            auto end = utility::Ctime::steady_ms();

            cout << "queue time: " << (end - start) << "ms" << endl;

            threads.clear();

            start = utility::Ctime::steady_ms();

            for (auto n = 0; n < 8; ++n) {
                threads.emplace_back([&atomic_cnt]() {
                    for (int i = 0; i < 1000000; ++i) {
                        atomic_cnt.load(std::memory_order_acquire);
                        ++atomic_cnt;
                        atomic_cnt.load(std::memory_order_acquire);
                        --atomic_cnt;
                    }
                });
            }

            for (auto &thread : threads)
                thread.join();

            end = utility::Ctime::steady_ms();

            cout << "stack time: " << (end - start) << "ms" << endl;

        }

        static void test_correctness_queue() {

            CatomicQueue<uint64_t> queue(0x1000);
            mutex out_lock;

            vector<thread> threads;

            atomic<uint64_t> count[8];
            atomic<uint64_t> coll[8];
            for (auto &c : count)
                c.store(0);
            for (auto &c : coll)
                c.store(0);

            auto start = utility::Ctime::steady_ms();

            for (int id = 0; id < 8; ++id) {
                threads.emplace_back([id, &queue, &out_lock, &coll]() {
                    mt19937_64 gen(id);

                    for (int i = 0; i < 5000000; ++i) {
                        uint64_t val = gen() & 0xFFFFFFFFFFFFFF;
                        while (true) {
                            auto bret = queue.enqueue((static_cast<uint64_t>(id) << (64 - 8)) | val);
                            if (bret)
                                break;
                            else
                                this_thread::yield();
                        }
                        coll[id] += val;
                    }
                });
            }

            for (int id = 0; id < 8; ++id) {
                threads.emplace_back([&queue, &coll, &count]() {
                    while (true) {
                        uint64_t val;
                        auto bret = queue.dequeue(val);
                        if (!bret) {
                            auto end = true;
                            for (auto &c : count) {
                                if (c != 5000000) {
                                    end = false;
                                    break;
                                }
                            }
                            if (end)
                                break;
                            continue;
                        }
                        auto sender_id = val >> (64 - 8);
                        val &= 0xFFFFFFFFFFFFFF;
                        coll[sender_id] -= val;
                        ++count[sender_id];
                    }
                });
            }

            for (auto &th : threads)
                th.join();

            auto end = utility::Ctime::steady_ms();

            cout << "queue time: " << (end - start) << "ms" << endl;

            for (auto &c : coll)
                cout << "coll: " << c << endl;

        }

        static void test_allocator() {
            auto ptr = new Cbuffer;
            auto ptr2 = new Cbuffer[10];
            delete ptr;
            delete[] ptr2;
        }

    };

}
