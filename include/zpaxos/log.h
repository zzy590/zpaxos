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
// Created by zzy on 2019-02-08.
//

#pragma once

#include "config.h"

#if LOG_ON

#  include <sstream>

#  include "spdlog/spdlog.h"
#  include "spdlog/sinks/stdout_color_sinks.h"
#  include "spdlog/sinks/basic_file_sink.h"

#endif

#include "utility/common_define.h"
#include "utility/time.h"

namespace zpaxos {

    class Clog final {

    NO_CONSTRUCTOR(Clog);
    NO_COPY_MOVE(Clog);

    public:

#if LOG_ON

        static spdlog::logger &logger() {
            static std::once_flag of;
            static std::shared_ptr<spdlog::logger> logger;

            std::call_once(of, [&]() {
#if LOG_CONSOLE
                logger = spdlog::stdout_color_mt("zpaxos");
#else
                std::ostringstream oss;
                oss << LOG_DIR<< "/zpaxos-utc-" << utility::Ctime::system_ms() << ".log";
                logger = spdlog::basic_logger_mt("zpaxos", oss.str());
#endif
                logger->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%n] [%l] %v");
                logger->set_level(LOG_LEVEL);
                logger->flush_on(spdlog::level::err);
            });

            return *logger;
        }

#endif

    };

#if LOG_ON

#  define LOG_TRACE(_x_) zpaxos::Clog::logger().trace _x_
#  define LOG_DEBUG(_x_) zpaxos::Clog::logger().debug _x_
#  define LOG_INFO(_x_) zpaxos::Clog::logger().info _x_
#  define LOG_WARN(_x_) zpaxos::Clog::logger().warn _x_
#  define LOG_ERROR(_x_) zpaxos::Clog::logger().error _x_
#  define LOG_CRITICAL(_x_) zpaxos::Clog::logger().critical _x_

#else

#  define LOG_TRACE(_x_)
#  define LOG_DEBUG(_x_)
#  define LOG_INFO(_x_)
#  define LOG_WARN(_x_)
#  define LOG_ERROR(_x_)
#  define LOG_CRITICAL(_x_)

#endif

}
