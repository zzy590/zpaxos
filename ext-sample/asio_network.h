//
// Created by zzy on 2020/4/2.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <thread>
#include <functional>
#include <stdexcept>

#include <boost/asio.hpp>

#include "zpaxos/utility/common_define.h"
#include "zpaxos/utility/atomicex.h"
#include "zpaxos/utility/hash.h"

#include "utility/serialize_util.h"

#include "zpaxos.h"
#include "storage_ext.h"
#include "peer_manager.h"

namespace network {

    static const size_t UDP_MAX_SIZE = 0x10000; // 64KB
    static const size_t TCP_BUF_SIZE = 0x10000; // 64KB
    static const int TCP_CONNECT_TIMEOUT = 5;
    static const int TCP_IDLE_TIMEOUT = 30;
    static const size_t MAX_UDP_PACK = 1472;

    class CudpCtx final {

    NO_COPY_MOVE(CudpCtx);

    private:

        boost::asio::mutable_buffer buf_;
        boost::asio::ip::udp::endpoint sender_;

        uint8_t internal_buf_[UDP_MAX_SIZE]{};

    public:

        typedef std::shared_ptr<CudpCtx> shared_ptr;

        CudpCtx()
                : buf_(&internal_buf_, sizeof(internal_buf_)) {}

        const boost::asio::mutable_buffer &buf() const {
            return buf_;
        }

        boost::asio::ip::udp::endpoint &sender() {
            return sender_;
        }

    };

    class CtcpCtx final {

    NO_COPY_MOVE(CtcpCtx);

    private:

        const uint32_t crc_base_;

        boost::asio::ip::tcp::socket sock_;
        boost::asio::steady_timer timer_;
        boost::asio::mutable_buffer buf_;

        // Session state.
        zpaxos::node_id_t peer_node_id_;
        size_t valid_length_;
        utility::Cbuffer decoder_buffer_;

        uint8_t internal_buf_[TCP_BUF_SIZE]{};

    public:

        typedef std::shared_ptr<CtcpCtx> shared_ptr;

        explicit CtcpCtx(uint32_t crc_base, boost::asio::io_context &context)
                : crc_base_(crc_base), sock_(context), timer_(context), buf_(&internal_buf_, sizeof(internal_buf_)),
                  peer_node_id_(0), valid_length_(0), decoder_buffer_(0x100) {}

        explicit CtcpCtx(uint32_t crc_base, boost::asio::ip::tcp::socket &&sock)
                : crc_base_(crc_base), sock_(std::forward<boost::asio::ip::tcp::socket>(sock)),
                  timer_(sock_.get_executor()), buf_(&internal_buf_, sizeof(internal_buf_)),
                  peer_node_id_(0), valid_length_(0), decoder_buffer_(0x100) {}

        boost::asio::ip::tcp::socket &sock() {
            return sock_;
        }

        boost::asio::steady_timer &timer() {
            return timer_;
        }

        const boost::asio::mutable_buffer &buf() const {
            return buf_;
        }

        zpaxos::node_id_t &peer_node_id() {
            return peer_node_id_;
        }

        bool stopped() const {
            return !sock_.is_open();
        }

        void push(size_t transferred) {
            auto prefer_size = decoder_buffer_.length();
            while (valid_length_ + transferred > prefer_size)
                prefer_size *= 2;
            decoder_buffer_.resize(prefer_size, valid_length_);
            ::memcpy(reinterpret_cast<uint8_t *>(decoder_buffer_.data()) + valid_length_,
                     internal_buf_, transferred);
            valid_length_ += transferred;
        }

        void decode(const std::unordered_map<zpaxos::state_machine_id_t,
                std::function<bool(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst)>> &deserialize_func,
                    std::vector<zpaxos::message_t> &messages) {
            auto decode_ptr = 0;
            while (valid_length_ >= decode_ptr + sizeof(uint32_t)) {
                auto pkt_hdr = reinterpret_cast<const uint8_t *>(decoder_buffer_.data()) + decode_ptr;
                auto pkt_len = *reinterpret_cast<const uint32_t *>(pkt_hdr);
                if (valid_length_ >= decode_ptr + sizeof(uint32_t) + pkt_len) {
                    if (pkt_len > sizeof(uint32_t)) {
                        auto pkt_data = pkt_hdr + sizeof(uint32_t);
                        auto msg_data = reinterpret_cast<const char *>(pkt_data + sizeof(uint32_t));
                        auto msg_len = pkt_len - sizeof(uint32_t);
                        if (utility::Chash::crc32(msg_data, msg_len, crc_base_) ==
                            *reinterpret_cast<const uint32_t *>(pkt_data)) {
                            zpaxos::message_t msg;
                            if (utility::Cserialize::deserialize(std::string(msg_data, msg_len), msg, deserialize_func))
                                messages.emplace_back(std::move(msg));
                        } else {
                            // TODO: log bad packet.
                        }
                    }
                    decode_ptr += sizeof(uint32_t) + pkt_len;
                }
            }

            // Move and cleanup.
            if (decode_ptr != 0) {
                if (valid_length_ > decode_ptr)
                    ::memmove(decoder_buffer_.data(),
                              reinterpret_cast<uint8_t *>(decoder_buffer_.data()) + decode_ptr,
                              valid_length_ - decode_ptr);
                valid_length_ -= decode_ptr;
            }
        }

    };

    class CasioNetwork final : public zpaxos::Ccommunication {

    NO_COPY_MOVE(CasioNetwork);

    private:

        // Peer info.
        peer_manager::CdynamicPeerManager &peer_mgr_;

        // Asio server.
        std::unique_ptr<boost::asio::io_context> context_;
        std::unique_ptr<boost::asio::ip::udp::socket> udp_;
        std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;

        // Worker threads.
        std::vector<std::thread> workers_;

        // TCP info.
        utility::CspinRWLock session_lock_;
        std::unordered_multimap<zpaxos::node_id_t, CtcpCtx::shared_ptr> session_map_;
        std::unordered_map<zpaxos::node_id_t, std::vector<std::pair<std::shared_ptr<uint8_t>, size_t>>> pending_send_;

        // Sending info.
        std::atomic<size_t> udp_sending_count_;
        std::atomic<size_t> tcp_sending_count_;

        // Peer info cache.
        utility::CspinRWLock peers_cache_lock_;
        zpaxos::instance_id_t peers_cache_version_;
        std::unordered_map<zpaxos::node_id_t, peer_manager::CdynamicPeerManager::peer_t> peers_cache_;

        // Snapshot serialize/deserialize function.
        utility::CspinRWLock snapshot_func_lock_;
        std::unordered_map<zpaxos::state_machine_id_t,
                std::function<bool(const zpaxos::Csnapshot::shared_ptr &src, std::string &dst)>> serialize_func_;
        std::unordered_map<zpaxos::state_machine_id_t,
                std::function<bool(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst)>> deserialize_func_;

        void worker() {
            context_->run();
        }

        /**
         * UDP routines.
         */

        void udp_recv(const CudpCtx::shared_ptr &ctx) {
            udp_->async_receive_from(ctx->buf(), ctx->sender(), [this, ctx]
                    (const boost::system::error_code &error, std::size_t bytes_transferred) {
                if (!error) {
                    // One complete message a packet.
                    if (bytes_transferred > sizeof(uint32_t)) {
                        auto msg_data = reinterpret_cast<const char *>(ctx->buf().data()) + sizeof(uint32_t);
                        auto msg_len = bytes_transferred - sizeof(uint32_t);
                        if (utility::Chash::crc32(msg_data, msg_len, peer_mgr_.get_crc_base()) ==
                            *reinterpret_cast<const uint32_t *>(ctx->buf().data())) {
                            bool good;
                            zpaxos::message_t msg;
                            {
                                utility::CautoSpinRWLock lck(snapshot_func_lock_);
                                good = utility::Cserialize::deserialize(
                                        std::string(msg_data, msg_len), msg, deserialize_func_);
                            }
                            if (good)
                                zpaxos::Ccommunication::on_receive(std::move(msg));
                        } else {
                            // TODO: log bad packet.
                        }
                    }
                }
                // Always receive UDP.
                udp_recv(ctx);
            });
        }

        void udp_send(const std::string &msg_buf, const uint32_t &crc, const boost::asio::ip::udp::endpoint &target) {
            // Copy to private buffer.
            auto len = sizeof(crc) + msg_buf.length();
            std::shared_ptr<uint8_t> buf(new uint8_t[len], std::default_delete<uint8_t[]>());
            *reinterpret_cast<uint32_t *>(buf.get()) = crc;
            ::memcpy(buf.get() + sizeof(crc), msg_buf.data(), msg_buf.length());

            // Async send.
            ++udp_sending_count_;
            udp_->async_send_to(boost::asio::buffer(buf.get(), len), target, [this, buf]
                    (const boost::system::error_code &error, std::size_t bytes_transferred) {
                // Auto free the shared buf.
                --udp_sending_count_;
            });
        }

        /**
         * TCP server routines.
         */

        void tcp_shutdown(const CtcpCtx::shared_ptr &ctx) {
            if (ctx->peer_node_id() != 0) {
                // Deregister if needed.
                utility::CautoSpinRWLock lck(session_lock_, true);
                auto range = session_map_.equal_range(ctx->peer_node_id());
                for (auto it = range.first; it != range.second; ++it) {
                    if (it->second.get() == ctx.get()) {
                        session_map_.erase(it); // Remove this session.
                        break;
                    }
                }
            }
            // Shutdown and close socket.
            boost::system::error_code ignored_error;
            ctx->sock().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored_error);
            ctx->sock().close(ignored_error);
            // Cancel timer.
            ctx->timer().cancel();
        }

        void tcp_send(const CtcpCtx::shared_ptr &ctx, const std::shared_ptr<uint8_t> &buf, size_t size) {
            ctx->timer().expires_after(std::chrono::seconds(TCP_IDLE_TIMEOUT));

            ++tcp_sending_count_;
            boost::asio::async_write(ctx->sock(), boost::asio::buffer(buf.get(), size), [this, ctx, buf, size]
                    (const boost::system::error_code &error, std::size_t bytes_transferred) {
                // Auto free the shared buf.
                --tcp_sending_count_;

                if (ctx->stopped())
                    return;
                if (error || bytes_transferred != size)
                    return tcp_shutdown(ctx);

                // Reset timer when send complete.
                ctx->timer().expires_after(std::chrono::seconds(TCP_IDLE_TIMEOUT));
            });
        }

        void tcp_startup(const CtcpCtx::shared_ptr &ctx) {
            // Register and send pending msg.
            std::vector<std::pair<std::shared_ptr<uint8_t>, size_t>> pending;
            if (ctx->peer_node_id() != 0) {
                utility::CautoSpinRWLock lck(session_lock_, true);
                // Record the session map.
                session_map_.emplace(ctx->peer_node_id(), ctx);
                // Move pending messages and send.
                auto it = pending_send_.find(ctx->peer_node_id());
                if (it != pending_send_.end()) {
                    pending = std::move(it->second);
                    pending_send_.erase(it);
                }
            }
            for (auto &item : pending)
                tcp_send(ctx, item.first, item.second);
        }

        void tcp_recv(const CtcpCtx::shared_ptr &ctx) {
            ctx->timer().expires_after(std::chrono::seconds(TCP_IDLE_TIMEOUT));

            ctx->sock().async_read_some(ctx->buf(), [this, ctx]
                    (const boost::system::error_code &error, std::size_t bytes_transferred) {
                if (ctx->stopped())
                    return;
                if (error)
                    return tcp_shutdown(ctx);

                ctx->push(bytes_transferred);
                std::vector<zpaxos::message_t> messages;
                {
                    utility::CautoSpinRWLock lck(snapshot_func_lock_);
                    ctx->decode(deserialize_func_, messages);
                }
                if (0 == ctx->peer_node_id() && !messages.empty()) {
                    const auto &first_msg = messages[0];
                    // Dealing first msg.
                    // TODO: auth?
                    ctx->peer_node_id() = first_msg.node_id;
                    tcp_startup(ctx);
                }
                for (auto &msg : messages)
                    zpaxos::Ccommunication::on_receive(std::move(msg));
                messages.clear();

                tcp_recv(ctx);
            });
        }

        void tcp_check_timeout(const CtcpCtx::shared_ptr &ctx) {
            ctx->timer().async_wait([this, ctx](const boost::system::error_code &error) {
                if (ctx->stopped())
                    return;
                if (ctx->timer().expiry() <= boost::asio::steady_timer::clock_type::now())
                    tcp_shutdown(ctx);
                else
                    tcp_check_timeout(ctx);
            });
        }

        void accept() {
            acceptor_->async_accept([this]
                                            (const boost::system::error_code &error,
                                             boost::asio::ip::tcp::socket peer) {
                if (!error) {
                    auto ctx = std::make_shared<CtcpCtx>(peer_mgr_.get_crc_base(), std::move(peer));
                    tcp_recv(ctx);
                    tcp_check_timeout(ctx);
                }

                accept();
            });
        }

        /**
         * TCP client routines.
         */

        void tcp_connect(const zpaxos::node_id_t &node_id, const boost::asio::ip::tcp::endpoint &address) {
            auto ctx = std::make_shared<CtcpCtx>(peer_mgr_.get_crc_base(), *context_);

            // Set timeout.
            ctx->timer().expires_after(std::chrono::seconds(TCP_CONNECT_TIMEOUT));

            // Connect.
            ctx->sock().async_connect(address, [this, ctx, node_id]
                    (const boost::system::error_code &error) {
                if (ctx->stopped()) {
                    // Connect timeout.
                    utility::CautoSpinRWLock lck(session_lock_, true);
                    pending_send_.erase(node_id); // Remove all pending send.
                    return;
                }
                if (error)
                    return tcp_shutdown(ctx);

                ctx->peer_node_id() = node_id;
                tcp_startup(ctx);
                tcp_recv(ctx);
            });

            // Start timeout.
            tcp_check_timeout(ctx);
        }

        inline bool pack(const zpaxos::message_t &msg, std::string &msg_buf, uint32_t &crc) {
            // Serialize.
            utility::CautoSpinRWLock lck(snapshot_func_lock_);
            if (!utility::Cserialize::serialize(msg, msg_buf, serialize_func_))
                return false;
            // Calc customised CRC.
            crc = utility::Chash::crc32(msg_buf.data(), msg_buf.length(), peer_mgr_.get_crc_base());
            return true;
        }

        inline void send(const zpaxos::node_id_t &node_id,
                         const boost::asio::ip::udp::endpoint &udp_address,
                         const boost::asio::ip::tcp::endpoint &tcp_address,
                         const std::string &msg_buf, const uint32_t &crc) {
            if (msg_buf.size() + sizeof(crc) <= MAX_UDP_PACK)
                return udp_send(msg_buf, crc, udp_address);// Send via UDP.

            // Generate TCP packet.
            auto len = sizeof(uint32_t) + sizeof(crc) + msg_buf.length();
            std::shared_ptr<uint8_t> buf(new uint8_t[len], std::default_delete<uint8_t[]>());
            *reinterpret_cast<uint32_t *>(buf.get()) = len - sizeof(uint32_t);
            *reinterpret_cast<uint32_t *>(buf.get() + sizeof(uint32_t)) = crc;
            ::memcpy(buf.get() + sizeof(uint32_t) + sizeof(crc), msg_buf.data(), msg_buf.length());

            bool need_connect = false;
            {
                utility::CautoSpinRWLock lck(session_lock_);
                auto it = session_map_.find(node_id); // Find one is ok.
                if (it != session_map_.end())
                    tcp_send(it->second, buf, len);
                else {
                    auto pair = std::make_pair(buf, len);
                    auto ib = pending_send_.emplace(node_id,
                                                    std::vector<std::pair<std::shared_ptr<uint8_t>, size_t>>(1, pair));
                    if (ib.second)
                        need_connect = true;
                    else
                        ib.first->second.emplace_back(pair);
                }
            }

            if (need_connect)
                tcp_connect(node_id, tcp_address);
        }

    public:

        CasioNetwork(peer_manager::CdynamicPeerManager &peer_mgr,
                     const boost::asio::ip::udp::endpoint &udp_address,
                     const boost::asio::ip::tcp::endpoint &tcp_address,
                     size_t worker_number = 2)
                : peer_mgr_(peer_mgr), udp_sending_count_(0), tcp_sending_count_(0),
                  peers_cache_version_(zpaxos::INSTANCE_ID_MAX) {
            // Check protobuf.
            GOOGLE_PROTOBUF_VERIFY_VERSION;

            // Init asio.
            context_.reset(new boost::asio::io_context);
            udp_.reset(new boost::asio::ip::udp::socket(*context_, udp_address));
            acceptor_.reset(new boost::asio::ip::tcp::acceptor(*context_, tcp_address));

            // Queue UDP read and acceptor with thread number.
            for (auto i = 0; i < worker_number; ++i) {
                udp_recv(std::make_shared<CudpCtx>());
                accept();
            }

            // Start threads.
            for (auto i = 0; i < worker_number; ++i)
                workers_.emplace_back(&CasioNetwork::worker, this);
        }

        ~CasioNetwork() final {
            context_->stop();
            for (auto &thread : workers_)
                thread.join();
        }

        bool register_snapshot_function(
                const zpaxos::state_machine_id_t id,
                std::function<bool(const zpaxos::Csnapshot::shared_ptr &src, std::string &dst)> &&serialize,
                std::function<bool(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst)> &&deserialize) {
            utility::CautoSpinRWLock lck(snapshot_func_lock_, true);
            auto ib0 = serialize_func_.emplace(id, std::forward<
                    std::function<bool(const zpaxos::Csnapshot::shared_ptr &src, std::string &dst)>>(serialize));
            if (!ib0.second)
                return false;
            auto ib1 = deserialize_func_.emplace(id, std::forward<
                    std::function<bool(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst)>>(deserialize));
            if (!ib1.second) {
                serialize_func_.erase(ib0.first);
                return false;
            }
            return true;
        }

        /**
         * Communication interface.
         */

        int send(const zpaxos::node_id_t &target_id, const zpaxos::message_t &message) final {
            std::string msg_buf;
            uint32_t crc;
            if (!pack(message, msg_buf, crc))
                return -1;

            zpaxos::instance_id_t target_version;
            if (!peer_mgr_.get_version(message.group_id, message.instance_id, target_version))
                return -1;

            bool need_update = false;
            // Check good and send.
            {
                utility::CautoSpinRWLock lck(peers_cache_lock_);
                if (peers_cache_version_ == target_version) {
                    // Good.
                    auto it = peers_cache_.find(target_id);
                    if (it != peers_cache_.end())
                        return send(target_id, it->second.udp_address, it->second.tcp_address, msg_buf, crc), 0;
                } else
                    need_update = true;
            }

            if (need_update) {
                // Update.
                utility::CautoSpinRWLock lck(peers_cache_lock_, true);
                if (!peer_mgr_.update_peers(message.group_id, message.instance_id, peers_cache_version_, peers_cache_))
                    return -1;
                lck.downgrade();

                auto it = peers_cache_.find(target_id);
                if (it != peers_cache_.end())
                    return send(target_id, it->second.udp_address, it->second.tcp_address, msg_buf, crc), 0;
            }
            return -1;
        }

        int broadcast(const zpaxos::message_t &message, broadcast_range_e range, broadcast_type_e type) final {
            std::string msg_buf;
            uint32_t crc;
            if (!pack(message, msg_buf, crc))
                return -1;

            bool self_voter = false;
            zpaxos::instance_id_t target_version;
            if (!peer_mgr_.get_version(message.group_id, message.instance_id, target_version, &self_voter))
                return -1;

            if (type == broadcast_self_first &&
                (broadcast_all == range ||
                 (self_voter && broadcast_voter == range) ||
                 (!self_voter && broadcast_follower == range))) {
                zpaxos::message_t copy = message;
                on_receive(std::move(copy));
            }

            bool need_update = false;
            // Check good and send.
            {
                utility::CautoSpinRWLock lck(peers_cache_lock_);
                if (peers_cache_version_ == target_version) {
                    // Good.
                    for (const auto &pair : peers_cache_) {
                        if (pair.first != peer_mgr_.get_self_node_id() &&
                            (broadcast_all == range ||
                             (pair.second.voter && broadcast_voter == range) ||
                             (!pair.second.voter && broadcast_follower == range)))
                            send(pair.first, pair.second.udp_address, pair.second.tcp_address, msg_buf, crc);
                    }
                } else
                    need_update = true;
            }

            if (need_update) {
                // Update.
                utility::CautoSpinRWLock lck(peers_cache_lock_, true);
                if (!peer_mgr_.update_peers(message.group_id, message.instance_id, peers_cache_version_, peers_cache_))
                    return -1;
                lck.downgrade();

                for (const auto &pair : peers_cache_) {
                    if (pair.first != peer_mgr_.get_self_node_id() &&
                        (broadcast_all == range ||
                         (pair.second.voter && broadcast_voter == range) ||
                         (!pair.second.voter && broadcast_follower == range)))
                        send(pair.first, pair.second.udp_address, pair.second.tcp_address, msg_buf, crc);
                }
            }

            if (type == broadcast_self_last &&
                (broadcast_all == range ||
                 (self_voter && broadcast_voter == range) ||
                 (!self_voter && broadcast_follower == range))) {
                zpaxos::message_t copy = message;
                on_receive(std::move(copy));
            }
            return 0;
        }

    };

}
