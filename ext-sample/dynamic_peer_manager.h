//
// Created by zzy on 2020/4/28.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <map>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <functional>
#include <stdexcept>

#include <boost/asio.hpp>

#include "rapidjson/document.h"

#include "proto/peer_manager.pb.h"

#include "zpaxos/utility/common_define.h"
#include "zpaxos/utility/atomicex.h"
#include "zpaxos/utility/hash.h"

#include "utility/md5.h"

#include "zpaxos.h"
#include "storage_ext.h"
#include "sm_id.h"

namespace peer_manager {

    static const char *DYNAMIC_PEER_MANAGER_KEY = "PEER-GROUP-";
    static const char *DYNAMIC_PEER_MANAGER_STATE_KEY = "PEER-SM-GROUP-";
    static const int LAZY_PERSIST_GAP = 64;

    class CdynamicPeerManager final : public zpaxos::CpeerManager {

    NO_COPY_MOVE(CdynamicPeerManager);

    public:

        typedef std::shared_ptr<CdynamicPeerManager> shared_ptr;

        struct peer_t final {
            zpaxos::node_id_t node_id;
            bool voter;
            boost::asio::ip::udp::endpoint udp_address;
            boost::asio::ip::tcp::endpoint tcp_address;

            explicit peer_t(const zpaxos::Node &node)
                    : node_id(node.node_id()), voter(node.voter()),
                      udp_address(boost::asio::ip::address::from_string(node.udp_address().ip()),
                                  node.udp_address().port()),
                      tcp_address(boost::asio::ip::address::from_string(node.tcp_address().ip()),
                                  node.tcp_address().port()) {}

            // {"id":1, "voter":true, "upd":{"ip":"127.0.0.1", "port":80}, "tcp":{"ip":"127.0.0.1", "port":80}}
            explicit peer_t(const rapidjson::Value &peer) {
                // Id.
                auto it = peer.FindMember("id");
                if (it == peer.MemberEnd() || !it->value.IsNumber())
                    throw std::runtime_error("Peer info without valid 'id'.");
                node_id = it->value.GetUint64();

                // Voter.
                it = peer.FindMember("voter");
                if (it == peer.MemberEnd() || !it->value.IsBool())
                    throw std::runtime_error("Peer info without valid 'voter'.");
                voter = it->value.GetBool();

                // UDP address.
                it = peer.FindMember("upd");
                if (it == peer.MemberEnd() || !it->value.IsObject())
                    throw std::runtime_error("Peer info without valid 'upd'.");
                auto ip_it = it->value.FindMember("ip");
                if (ip_it == it->value.MemberEnd() || !ip_it->value.IsString())
                    throw std::runtime_error("Peer info without valid 'upd'.'ip'.");
                auto port_it = it->value.FindMember("port");
                if (port_it == it->value.MemberEnd() || !port_it->value.IsNumber())
                    throw std::runtime_error("Peer info without valid 'upd'.'port'.");
                udp_address.address(boost::asio::ip::address::from_string(ip_it->value.GetString()));
                udp_address.port(port_it->value.GetUint());

                // TCP address.
                it = peer.FindMember("tcp");
                if (it == peer.MemberEnd() || !it->value.IsObject())
                    throw std::runtime_error("Peer info without valid 'tcp'.");
                ip_it = it->value.FindMember("ip");
                if (ip_it == it->value.MemberEnd() || !ip_it->value.IsString())
                    throw std::runtime_error("Peer info without valid 'tcp'.'ip'.");
                port_it = it->value.FindMember("port");
                if (port_it == it->value.MemberEnd() || !port_it->value.IsNumber())
                    throw std::runtime_error("Peer info without valid 'tcp'.'port'.");
                tcp_address.address(boost::asio::ip::address::from_string(ip_it->value.GetString()));
                tcp_address.port(port_it->value.GetUint());
            }

            void get(zpaxos::Node *node) const {
                node->set_node_id(node_id);
                auto udp = node->mutable_udp_address();
                udp->set_ip(udp_address.address().to_string());
                udp->set_port(udp_address.port());
                auto tcp = node->mutable_tcp_address();
                tcp->set_ip(tcp_address.address().to_string());
                tcp->set_port(tcp_address.port());
                node->set_voter(voter);
            }
        };

    private:

        // Peer info.
        struct peer_info_t final {
            bool self_voter;
            size_t voter_cnt;
            std::unordered_map<zpaxos::node_id_t, peer_t> peers;

            peer_info_t()
                    : self_voter(false), voter_cnt(0) {}
        };

        struct group_peer_info_t final {
            typedef std::unique_ptr<group_peer_info_t> ptr;

            struct reverse_cmp_t final {
                bool operator()(const zpaxos::instance_id_t &left, const zpaxos::instance_id_t &right) const {
                    return left > right;
                }
            };

            std::atomic<zpaxos::instance_id_t> next_instance_id;
            std::atomic<zpaxos::instance_id_t> next_persist_id;

            utility::CspinRWLock peer_lock;
            bool purged;
            std::map<zpaxos::instance_id_t, peer_info_t, reverse_cmp_t> mvcc; // N2O

            group_peer_info_t()
                    : next_instance_id(0), next_persist_id(0), purged(false) {}
        };

        // Const data.
        zpaxos::CstorageExt &storage_;
        const zpaxos::write_options_t default_write_options_;
        const zpaxos::node_id_t self_node_id_;
        std::string digest_;
        uint32_t crc_base_;
        peer_info_t initial_peers_;

        // Peer info per group.
        std::vector<group_peer_info_t::ptr> group_peer_infos_;

        // State machine.
        class CpeerManagementStateMachine final : public zpaxos::CstateMachine {

        NO_COPY_MOVE(CpeerManagementStateMachine);

        private:

            const CdynamicPeerManager::shared_ptr manager_;
            const zpaxos::group_id_t group_id_;

            // Should hold the write lock.
            bool internal_restore_mvcc(const zpaxos::PeerPersist &persist, bool check_node_id = true) {
                if (persist.cluster_digest() != manager_->digest_ ||
                    persist.cluster_crc_base() != manager_->crc_base_ ||
                    persist.group_id() != group_id_ ||
                    (check_node_id && persist.my_node_id() != manager_->self_node_id_))
                    return false;

                auto &group = *manager_->group_peer_infos_[group_id_];

                // Load purged flag.
                group.purged = persist.purged();

                // Load mvcc.
                group.mvcc.clear();
                for (const auto &item : persist.mvcc()) {
                    peer_info_t info;
                    for (const auto &node : item.nodes()) {
                        auto ib = info.peers.emplace(node.node_id(), peer_t(node));
                        if (ib.second && node.voter()) {
                            ++info.voter_cnt;
                            if (node.node_id() == manager_->self_node_id_)
                                info.self_voter = true;
                        }
                    }
                    group.mvcc.emplace(item.instance_id(), std::move(info));
                }
                return true;
            }

            // Should hold the read lock.
            void internal_store_mvcc(zpaxos::PeerPersist &persist, bool store_node_id = true) {
                persist.set_cluster_digest(manager_->digest_);
                persist.set_cluster_crc_base(manager_->crc_base_);
                persist.set_group_id(group_id_);
                if (store_node_id)
                    persist.set_my_node_id(manager_->self_node_id_);

                auto &group = *manager_->group_peer_infos_[group_id_];

                // Store purged flag.
                persist.set_purged(group.purged);

                // Store mvcc.
                if (!group.mvcc.empty()) {
                    auto mutable_mvcc = persist.mutable_mvcc();
                    for (const auto &pair : group.mvcc) {
                        auto ptr = mutable_mvcc->Add();
                        ptr->set_instance_id(pair.first);
                        if (!pair.second.peers.empty()) {
                            auto mutable_modes = ptr->mutable_nodes();
                            for (const auto &peer_pair : pair.second.peers)
                                peer_pair.second.get(mutable_modes->Add());
                        }
                    }
                }
            }

            void restore() {
                auto &group = *manager_->group_peer_infos_[group_id_];

                utility::CautoSpinRWLock lck(group.peer_lock, true);

                zpaxos::instance_id_t next_inst = 0;

                // Load from PeerStatePersist.
                std::string val;
                auto iret = manager_->storage_.get(group_id_, DYNAMIC_PEER_MANAGER_STATE_KEY, val);
                if (iret < 0)
                    throw std::runtime_error("CpeerManagementStateMachine::restore error get.");
                else if (0 == iret) {
                    // Deserialize.
                    zpaxos::PeerStatePersist statePersist;
                    auto bret = statePersist.ParseFromString(val);
                    if (!bret)
                        throw std::runtime_error("CpeerManagementStateMachine::restore error deserializing.");
                    next_inst = statePersist.instance_id();
                }

                // Load MVCC.
                val.clear();
                iret = manager_->storage_.get(group_id_, DYNAMIC_PEER_MANAGER_KEY, val);
                if (iret < 0)
                    throw std::runtime_error("CpeerManagementStateMachine::restore error get.");
                else if (0 == iret) {
                    // Deserialize.
                    zpaxos::PeerPersist persist;
                    auto bret = persist.ParseFromString(val);
                    if (!bret)
                        throw std::runtime_error("CpeerManagementStateMachine::restore error deserializing.");
                    if (!internal_restore_mvcc(persist))
                        throw std::runtime_error("CpeerManagementStateMachine::restore bad PeerPersist.");
                }

                // Set next inst.
                if (!group.mvcc.empty()) // Peer operation use one inst, so plus one.
                    next_inst = std::max(next_inst, group.mvcc.begin()->first + 1);
                group.next_instance_id.store(next_inst, std::memory_order_release);
                group.next_persist_id.store(next_inst, std::memory_order_release);
            }

            // Should hold the read lock.
            void persist(group_peer_info_t &group, bool store_mvcc = false, bool must_success = true) {
                std::string key, val;
                zpaxos::instance_id_t persist_id = 0;
                if (store_mvcc) {
                    // Write PeerPersist.
                    zpaxos::PeerPersist persist;
                    internal_store_mvcc(persist);

                    if (!group.mvcc.empty())
                        persist_id = group.mvcc.begin()->first + 1;

                    key = DYNAMIC_PEER_MANAGER_KEY;
                    if (!persist.SerializePartialToString(&val))
                        throw std::runtime_error("CpeerManagementStateMachine::persist error serializing.");
                } else {
                    // Write PeerStatePersist.
                    zpaxos::PeerStatePersist statePersist;
                    persist_id = group.next_instance_id.load(std::memory_order_acquire);
                    statePersist.set_instance_id(persist_id);

                    key = DYNAMIC_PEER_MANAGER_STATE_KEY;
                    if (!statePersist.SerializePartialToString(&val))
                        throw std::runtime_error("CpeerManagementStateMachine::persist error serializing.");
                }

                if (val.empty())
                    throw std::runtime_error("CpeerManagementStateMachine::persist error empty val.");
                auto iret = manager_->storage_.put(group_id_, key, val, manager_->default_write_options_);
                if (0 == iret) {
                    if (persist_id > group.next_persist_id.load(std::memory_order_acquire))
                        group.next_persist_id.store(persist_id, std::memory_order_release);
                } else if (must_success)
                    throw std::runtime_error("CpeerManagementStateMachine::persist error persist state.");
            }

        public:

            CpeerManagementStateMachine(CdynamicPeerManager::shared_ptr manager, const zpaxos::group_id_t &group_id)
                    : manager_(std::move(manager)), group_id_(group_id) {
                // Check group.
                if (group_id_ >= manager_->group_peer_infos_.size())
                    throw std::runtime_error("CpeerManagementStateMachine error group id.");
                // Restore.
                restore();
            }

            zpaxos::state_machine_id_t get_id() const final {
                return zpaxos::SMID_DYNAMIC_PEER_MANAGER;
            }

            void consume_sequentially(const zpaxos::instance_id_t &instance_id, const utility::Cslice &value) final {
                auto &group = *manager_->group_peer_infos_[group_id_];

                utility::CautoSpinRWLock lck(group.peer_lock, true);

                // Ignore duplicated replay.
                auto permitted_instance_id = group.next_instance_id.load(std::memory_order_acquire);
                if (instance_id < permitted_instance_id)
                    return;
                else if (instance_id > permitted_instance_id) // Fatal error when jump instance.
                    throw std::runtime_error("CpeerManagementStateMachine::consume_sequentially jump instance.");

                // Decode.
                zpaxos::PeerOperation operation;
                if (0 == value.length() || !operation.ParseFromString(
                        std::string(reinterpret_cast<const char *>(value.data()), value.length()))) {
                    ++group.next_instance_id;
                    if (group.next_instance_id.load(std::memory_order_acquire) >=
                        group.next_persist_id.load(std::memory_order_acquire) + LAZY_PERSIST_GAP)
                        persist(group, false, false); // Lazy persist the state.
                    return;
                }

                // Check digest.
                if (operation.cluster_digest() != manager_->digest_ ||
                    operation.cluster_crc_base() != manager_->crc_base_)
                    throw std::runtime_error(
                            "CpeerManagementStateMachine::consume_sequentially cluster digest & crc mismatch.");

                //
                // Dealing operation.
                //

                // Copy latest.
                peer_info_t new_peer_info;
                if (!group.mvcc.empty())
                    new_peer_info = group.mvcc.begin()->second;
                else if (group.purged)
                    throw std::runtime_error("CpeerManagementStateMachine::consume_sequentially bad purge result.");
                else
                    new_peer_info = manager_->initial_peers_;

                for (const auto &removing : operation.removing()) {
                    auto sz = new_peer_info.peers.erase(removing);
                    if (sz != 1)
                        throw std::runtime_error(
                                "CpeerManagementStateMachine::consume_sequentially removing not existing.");
                }

                for (const auto &adding : operation.adding()) {
                    auto it = new_peer_info.peers.find(adding.node_id());
                    if (it != new_peer_info.peers.end())
                        throw std::runtime_error("CpeerManagementStateMachine::consume_sequentially adding duplicate.");
                    new_peer_info.peers.emplace(adding.node_id(), std::move(peer_t(adding)));
                }

                new_peer_info.self_voter = false;
                new_peer_info.voter_cnt = 0;
                for (const auto &pair : new_peer_info.peers) {
                    if (pair.second.voter) {
                        ++new_peer_info.voter_cnt;
                        if (pair.first == manager_->self_node_id_)
                            new_peer_info.self_voter = true;
                    }
                }

                group.mvcc.emplace(instance_id, new_peer_info);
                ++group.next_instance_id;
                persist(group, true);
            }

            zpaxos::instance_id_t get_next_execute_id() final {
                return manager_->group_peer_infos_[group_id_]->next_instance_id.load(std::memory_order_acquire);
            }

            zpaxos::instance_id_t get_next_persist_id() final {
                return manager_->group_peer_infos_[group_id_]->next_persist_id.load(std::memory_order_acquire);
            }

            class CdynamicPeerManagerSnapshot final : public zpaxos::Csnapshot {

            NO_COPY_MOVE(CdynamicPeerManagerSnapshot);

            private:

                const zpaxos::instance_id_t next_instance_id_;
                zpaxos::PeerSnapshot snapshot_;

            public:

                // Should hold the read lock.
                explicit CdynamicPeerManagerSnapshot(CpeerManagementStateMachine &sm)
                        : next_instance_id_(sm.manager_->group_peer_infos_[sm.group_id_]
                                                    ->next_instance_id.load(std::memory_order_acquire)) {
                    snapshot_.set_next_instance_id(next_instance_id_);
                    sm.internal_store_mvcc(*snapshot_.mutable_persist(), false);
                }

                explicit CdynamicPeerManagerSnapshot(zpaxos::PeerSnapshot &&snapshot)
                        : next_instance_id_(snapshot.next_instance_id()),
                          snapshot_(std::forward<zpaxos::PeerSnapshot>(snapshot)) {}

                const zpaxos::PeerSnapshot &get_snapshot() const {
                    return snapshot_;
                }

                zpaxos::state_machine_id_t get_id() const final {
                    return zpaxos::SMID_DYNAMIC_PEER_MANAGER;
                }

                const zpaxos::instance_id_t &get_next_instance_id() const final {
                    return next_instance_id_;
                }

                static bool serialize_function(const zpaxos::Csnapshot::shared_ptr &src, std::string &dst) {
                    if (src->get_id() != zpaxos::SMID_DYNAMIC_PEER_MANAGER)
                        return false;
                    return dynamic_cast<CdynamicPeerManagerSnapshot *>(src.get())
                            ->snapshot_.SerializePartialToString(&dst);
                }

                static bool deserialize_function(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst) {
                    zpaxos::PeerSnapshot snapshot;
                    if (!snapshot.ParseFromString(src))
                        return false;
                    dst = std::make_shared<CdynamicPeerManagerSnapshot>(std::move(snapshot));
                    return true;
                }

            };

            int take_snapshot(zpaxos::Csnapshot::shared_ptr &snapshot) final {
                auto &group = *manager_->group_peer_infos_[group_id_];
                utility::CautoSpinRWLock lck(group.peer_lock);
                snapshot = std::make_shared<CdynamicPeerManagerSnapshot>(*this);
                return 0;
            }

            int load_snapshot(const zpaxos::Csnapshot::shared_ptr &snapshot) final {
                if (!snapshot || snapshot->get_id() != zpaxos::SMID_DYNAMIC_PEER_MANAGER)
                    return -1;

                auto &group = *manager_->group_peer_infos_[group_id_];
                utility::CautoSpinRWLock lck(group.peer_lock, true);
                auto ss = dynamic_cast<CdynamicPeerManagerSnapshot *>(snapshot.get());
                if (group.next_instance_id.load(std::memory_order_acquire) >= ss->get_next_instance_id())
                    return 1;

                if (!internal_restore_mvcc(ss->get_snapshot().persist(), false))
                    return -1;
                group.next_instance_id.store(ss->get_next_instance_id(), std::memory_order_release);

                if (!group.mvcc.empty()) {
                    if (group.next_instance_id.load(std::memory_order_acquire) < group.mvcc.begin()->first + 1)
                        throw std::runtime_error("CpeerManagementStateMachine::load_snapshot bad snapshot next inst.");
                }

                lck.downgrade();

                // Persist. First store mvcc then state.
                persist(group, true);
                persist(group);
                return 0;
            }

        };

        const peer_info_t *get_info_mvcc(const group_peer_info_t &group, const zpaxos::instance_id_t &instance_id) {
            if (group.mvcc.empty()) {
                if (group.purged)
                    throw std::runtime_error("CpeerManagementStateMachine::get_info_mvcc bad purge result.");
                return &initial_peers_;
            }
            auto it = group.mvcc.upper_bound(instance_id);
            if (it == group.mvcc.end()) {
                if (group.purged)
                    return nullptr; // Just return nullptr if we have lost the version.
                return &initial_peers_;
            } else
                return &it->second;
        }

    public:

        CdynamicPeerManager(zpaxos::CstorageExt &storage,
                            const zpaxos::write_options_t &default_write_options,
                            const std::string &conf, // {"group":2, "peers":[{peer},{peer}]}
                            const zpaxos::node_id_t &my_node)
                : storage_(storage), default_write_options_(default_write_options), self_node_id_(my_node) {
            // Calc cluster digest & CRC base.
            utility::Cmd5::digest(conf, digest_);
            crc_base_ = utility::Chash::crc32(conf.data(), conf.length());

            // Form initial peer info from conf string.
            rapidjson::Document document;
            document.Parse(conf.c_str());
            auto it = document.FindMember("group");
            if (it == document.MemberEnd() || !it->value.IsNumber())
                throw std::runtime_error("CpeerManager conf without valid 'group'.");
            if (it->value.GetUint() != storage_.group_number())
                throw std::runtime_error("CpeerManager conf group number mismatch.");
            it = document.FindMember("peers");
            if (it == document.MemberEnd() || !it->value.IsArray())
                throw std::runtime_error("CpeerManager conf without valid 'peers'.");

            const peer_t *myself = nullptr;
            initial_peers_.voter_cnt = 0;
            for (const auto &peer_val : it->value.GetArray()) {
                peer_t peer(peer_val);
                auto id = peer.node_id;
                auto voter = peer.voter;
                auto ib = initial_peers_.peers.emplace(id, std::move(peer));
                if (!ib.second)
                    throw std::runtime_error("CpeerManager duplicate peer.");
                if (id == my_node) {
                    if (myself != nullptr)
                        throw std::runtime_error("CpeerManager duplicate self.");
                    myself = &ib.first->second; // Pointer on map.
                }
                if (voter)
                    ++initial_peers_.voter_cnt;
            }
            if (nullptr == myself) // No myself in initial peers is ok.
                initial_peers_.self_voter = false;
            else
                initial_peers_.self_voter = myself->voter;

            // Init group vector.
            group_peer_infos_.resize(storage_.group_number());
            for (auto &ptr : group_peer_infos_)
                ptr.reset(new group_peer_info_t());
        }

        const std::string &get_digest() const {
            return digest_;
        }

        uint32_t get_crc_base() const {
            return crc_base_;
        }

        const zpaxos::node_id_t &get_self_node_id() const {
            return self_node_id_;
        }

        static zpaxos::CstateMachine::ptr get_sm(
                const CdynamicPeerManager::shared_ptr &ptr, const zpaxos::group_id_t &group_id) {
            if (group_id >= ptr->group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            return std::move(zpaxos::CstateMachine::ptr(new CpeerManagementStateMachine(ptr, group_id)));
        }

        static std::function<bool(const zpaxos::Csnapshot::shared_ptr &src, std::string &dst)>
        get_serialize_function() {
            return CpeerManagementStateMachine::CdynamicPeerManagerSnapshot::serialize_function;
        }

        static std::function<bool(const std::string &src, zpaxos::Csnapshot::shared_ptr &dst)>
        get_deserialize_function() {
            return CpeerManagementStateMachine::CdynamicPeerManagerSnapshot::deserialize_function;
        }

        bool is_voter(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                      const zpaxos::node_id_t &node_id) final {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            auto info = get_info_mvcc(group, instance_id);
            if (nullptr == info)
                return false; // Return false if we have lost the version.
            if (node_id == self_node_id_)
                return info->self_voter;
            auto it = info->peers.find(node_id);
            if (it == info->peers.end())
                return false;
            return it->second.voter;
        }

        bool is_all_peer(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                         const std::set<zpaxos::node_id_t> &node_set) final {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            auto info = get_info_mvcc(group, instance_id);
            if (nullptr == info || node_set.size() < info->peers.size())
                return false; // Return false if we have lost the version.
            for (const auto &pair : info->peers) {
                if (node_set.find(pair.first) == node_set.end())
                    return false;
            }
            return true;
        }

        bool is_all_voter(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                          const std::set<zpaxos::node_id_t> &node_set) final {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            auto info = get_info_mvcc(group, instance_id);
            if (nullptr == info || node_set.size() < info->voter_cnt)
                return false; // Return false if we have lost the version.
            for (const auto &pair : info->peers) {
                if (pair.second.voter && node_set.find(pair.first) == node_set.end())
                    return false;
            }
            return true;
        }

        bool is_quorum(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                       const std::set<zpaxos::node_id_t> &node_set) final {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            auto info = get_info_mvcc(group, instance_id);
            if (nullptr == info || node_set.size() < info->voter_cnt / 2 + 1)
                return false; // Return false if we have lost the version.
            auto cnt = 0;
            for (const auto &pair : info->peers) {
                if (pair.second.voter && node_set.find(pair.first) != node_set.end())
                    ++cnt;
            }
            return cnt >= info->voter_cnt / 2 + 1;
        }

        bool get_version(
                const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                zpaxos::instance_id_t &version,
                bool *self_voter = nullptr) {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            if (!group.mvcc.empty()) {
                auto it = group.mvcc.upper_bound(instance_id);
                if (it != group.mvcc.end()) {
                    if (self_voter != nullptr)
                        *self_voter = it->second.self_voter;
                    version = it->first;
                    return true;
                } else if (group.purged)
                    return false; // Fail if we have lost the version.
            } else if (group.purged)
                throw std::runtime_error("CpeerManagementStateMachine::get_version bad purge result.");
            if (self_voter != nullptr)
                *self_voter = initial_peers_.self_voter;
            version = zpaxos::INSTANCE_ID_INVALID;
            return true;
        }

        bool update_peers(const zpaxos::group_id_t &group_id, const zpaxos::instance_id_t &instance_id,
                          zpaxos::instance_id_t &version, std::unordered_map<zpaxos::node_id_t, peer_t> &peers) {
            if (group_id >= group_peer_infos_.size())
                throw std::runtime_error("CdynamicPeerManager error group id.");
            auto &group = *group_peer_infos_[group_id];

            utility::CautoSpinRWLock lck(group.peer_lock);

            auto target_version = zpaxos::INSTANCE_ID_INVALID;
            auto info = &initial_peers_;
            if (!group.mvcc.empty()) {
                auto it = group.mvcc.upper_bound(instance_id);
                if (it != group.mvcc.end())
                    target_version = it->first, info = &it->second;
                else if (group.purged)
                    return false; // Fail if we have lost the version.
            } else if (group.purged)
                throw std::runtime_error("CpeerManagementStateMachine::update_peers bad purge result.");

            // Copy if empty or need update.
            if (peers.empty() || version != target_version) {
                version = target_version;
                peers = info->peers;
            }
            return true;
        }

    };

}
