#ifndef __COMMON_H__
#define __COMMON_H__

#include <atomic>
#include <string>
#include <boost/functional/hash.hpp>
#include <boost/format.hpp>
#include <boost/crc.hpp>
#include <functional>
#include "consistent_hash_map.hpp"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"

#include "spdlog/spdlog.h"

using namespace std;

// Define the default replication factor for the data
#define DEFAULT_GLOBAL_REPLICATION 1

// Define the default local replication factor
#define DEFAULT_LOCAL_REPLICATION 1

// Define the number of memory threads
#define THREAD_NUM 16

// Define the number of proxy worker threads
#define PROXY_THREAD_NUM 16

// Define the number of benchmark threads
#define BENCHMARK_THREAD_NUM 16

// Define the number of virtual thread per each physical thread
#define VIRTUAL_THREAD_NUM 1000

// Define port offset
// used by servers
#define SERVER_PORT 6560
#define NODE_JOIN_BASE_PORT 6660
#define NODE_DEPART_BASE_PORT 6760
#define SELF_DEPART_BASE_PORT 6860
#define REQUEST_PULLING_BASE_PORT 6460
#define GOSSIP_BASE_PORT 7060

// used by proxies
#define SEED_BASE_PORT 6560
#define NOTIFY_BASE_PORT 6660
#define KEY_ADDRESS_BASE_PORT 6760

// used by benchmark threads
#define COMMAND_BASE_PORT 6560

#define SERVER_IP_FILE "conf/server/server_ip.txt"
#define PROXY_IP_FILE "conf/proxy/proxy_ip.txt"
#define USER_IP_FILE "conf/user/user_ip.txt"

// server thread
class server_thread_t {
  string ip_;
  unsigned tid_;
  unsigned virtual_num_;
public:
  server_thread_t() {}
  server_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}
  server_thread_t(string ip, unsigned tid, unsigned virtual_num): ip_(ip), tid_(tid), virtual_num_(virtual_num) {}

  string get_ip() const {
    return ip_;
  }
  unsigned get_tid() const {
    return tid_;
  }
  unsigned get_virtual_num() const {
    return virtual_num_;
  }
  string get_id() const {
    return ip_ + ":" + to_string(SERVER_PORT + tid_);
  }
  string get_virtual_id() const {
    return ip_ + ":" + to_string(SERVER_PORT + tid_) + "_" + to_string(virtual_num_);
  }
  string get_node_join_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  string get_node_join_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NODE_JOIN_BASE_PORT);
  }
  string get_node_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  string get_node_depart_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NODE_DEPART_BASE_PORT);
  }
  string get_self_depart_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  string get_self_depart_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SELF_DEPART_BASE_PORT);
  }
  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_gossip_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
  string get_gossip_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + GOSSIP_BASE_PORT);
  }
};

bool operator==(const server_thread_t& l, const server_thread_t& r) {
  if (l.get_id().compare(r.get_id()) == 0) {
    return true;
  } else {
    return false;
  }
}

struct thread_hash {
  std::size_t operator () (const server_thread_t &st) const {
    return std::hash<string>{}(st.get_id());
  }
};

struct global_hasher {
  uint32_t operator()(const server_thread_t& th) {
    boost::crc_32_type ret;
    ret.process_bytes(th.get_virtual_id().c_str(), th.get_virtual_id().size());
    return ret.checksum();
  }
  uint32_t operator()(const string& key) {
    boost::crc_32_type ret;
    ret.process_bytes(key.c_str(), key.size());
    return ret.checksum();
  }
  typedef uint32_t result_type;
};

struct local_hasher {
  hash<string>::result_type operator()(const server_thread_t& th) {
    return hash<string>{}(to_string(th.get_tid()) + "_" + to_string(th.get_virtual_num()));
  }
  hash<string>::result_type operator()(const string& key) {
    return hash<string>{}(key);
  }
  typedef hash<string>::result_type result_type;
};


// proxy thread
class proxy_thread_t {
  string ip_;
  unsigned tid_;
public:
  proxy_thread_t() {}
  proxy_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}

  string get_ip() const {
    return ip_;
  }
  unsigned get_tid() const {
    return tid_;
  }
  string get_seed_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + SEED_BASE_PORT);
  }
  string get_seed_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + SEED_BASE_PORT);
  }
  string get_notify_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + NOTIFY_BASE_PORT);
  }
  string get_notify_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + NOTIFY_BASE_PORT);
  }
  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }
  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }
};

class user_thread_t {
  string ip_;
  unsigned tid_;
public:
  user_thread_t() {}
  user_thread_t(string ip, unsigned tid): ip_(ip), tid_(tid) {}

  string get_ip() const {
    return ip_;
  }
  unsigned get_tid() const {
    return tid_;
  }
  string get_request_pulling_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_request_pulling_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + REQUEST_PULLING_BASE_PORT);
  }
  string get_key_address_connect_addr() const {
    return "tcp://" + ip_ + ":" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }
  string get_key_address_bind_addr() const {
    return "tcp://*:" + to_string(tid_ + KEY_ADDRESS_BASE_PORT);
  }
};

typedef consistent_hash_map<server_thread_t, global_hasher> global_hash_t;
typedef consistent_hash_map<server_thread_t, local_hasher> local_hash_t;

void split(const string &s, char delim, vector<string> &elems) {
  stringstream ss(s);
  string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
}

string get_ip(string node_type) {
  string server_ip;
  ifstream address;

  if (node_type == "server") {
    address.open(SERVER_IP_FILE);
  } else if (node_type == "proxy") {
    address.open(PROXY_IP_FILE);
  } else if (node_type == "user") {
    address.open(USER_IP_FILE);
  }
  std::getline(address, server_ip);
  address.close();

  return server_ip;
}

// assuming the replication factor will never be greater than the number of nodes in a tier
// return a set of server_thread_t that are responsible for a key
unordered_set<server_thread_t, thread_hash> responsible_global(string key, unsigned global_rep, global_hash_t& global_hash_ring) {
  unordered_set<server_thread_t, thread_hash> threads;
  auto pos = global_hash_ring.find(key);
  if (pos != global_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;
    while (i < global_rep && i != global_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = threads.insert(pos->second).second;
      if (++pos == global_hash_ring.end()) {
        pos = global_hash_ring.begin();
      }
      if (succeed) {
        i += 1;
      }
    }
  }
  return threads;
}

// assuming the replication factor will never be greater than the number of worker threads
// return a set of tids that are responsible for a key
unordered_set<unsigned> responsible_local(string key, unsigned local_rep, local_hash_t& local_hash_ring) {
  unordered_set<unsigned> tids;
  auto pos = local_hash_ring.find(key);
  if (pos != local_hash_ring.end()) {
    // iterate for every value in the replication factor
    unsigned i = 0;
    while (i < local_rep && i != local_hash_ring.size() / VIRTUAL_THREAD_NUM) {
      bool succeed = tids.insert(pos->second.get_tid()).second;
      if (++pos == local_hash_ring.end()) {
        pos = local_hash_ring.begin();
      }
      if (succeed) {
        i += 1;
      }
    }
  }
  return tids;
}

void prepare_get_tuple(communication::Request& req, string key) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
}

void prepare_put_tuple(communication::Request& req, string key, string value, unsigned long long timestamp) {
  communication::Request_Tuple* tp = req.add_tuple();
  tp->set_key(key);
  tp->set_value(value);
  tp->set_timestamp(timestamp);
}

template<typename REQ, typename RES>
bool recursive_receive(zmq::socket_t& receiving_socket, zmq::message_t& message, REQ& req, RES& response, bool& succeed) {
  bool rc = receiving_socket.recv(&message);
  if (rc) {
    //succeed = true;
    auto serialized_resp = zmq_util::message_to_string(message);
    response.ParseFromString(serialized_resp);
    if (req.request_id() == response.response_id()) {
      succeed = true;
      return false;
    } else {
      cerr << "id mismatch!\n";
      return true;
    }
  } else {
    // timeout
    if (errno == EAGAIN) {
      succeed = false;
    } else {
      cerr << "Unexpected error type\n";
      succeed = false;
    }
    return false;
  }
}

template<typename REQ, typename RES>
RES send_request(REQ& req, zmq::socket_t& sending_socket, zmq::socket_t& receiving_socket, bool& succeed) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &sending_socket);
  RES response;
  zmq::message_t message;
  bool recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);
  while (recurse) {
    response.Clear();
    zmq::message_t message;
    recurse = recursive_receive<REQ, RES>(receiving_socket, message, req, response, succeed);
  }
  return response;
}

void push_request(communication::Request& req, zmq::socket_t& socket) {
  string serialized_req;
  req.SerializeToString(&serialized_req);
  zmq_util::send_string(serialized_req, &socket);
}

template<typename H>
bool insert_to_hash_ring(H& hash_ring, string ip, unsigned tid) {
  bool succeed;
  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    succeed = hash_ring.insert(server_thread_t(ip, tid, virtual_num)).second;
  }
  return succeed;
}

template<typename H>
void remove_from_hash_ring(H& hash_ring, string ip, unsigned tid) {
  for (unsigned virtual_num = 0; virtual_num < VIRTUAL_THREAD_NUM; virtual_num++) {
    hash_ring.erase(server_thread_t(ip, tid, virtual_num));
  }
}

// get all threads responsible for a key from the "node_type" tier
// metadata flag = 0 means the key is a metadata. Otherwise, it is a regular data
unordered_set<server_thread_t, thread_hash> get_responsible_threads(
    string key,
    global_hash_t& global_hash_ring,
    local_hash_t& local_hash_ring) {
  unordered_set<server_thread_t, thread_hash> result;
  auto mts = responsible_global(key, DEFAULT_GLOBAL_REPLICATION, global_hash_ring);
  for (auto it = mts.begin(); it != mts.end(); it++) {
    string ip = it->get_ip();
    auto tids = responsible_local(key, DEFAULT_LOCAL_REPLICATION, local_hash_ring);
    for (auto iter = tids.begin(); iter != tids.end(); iter++) {
      result.insert(server_thread_t(ip, *iter));
    }
  }
  return result;
}

#endif
