#include <zmq.hpp>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <ctime>
#include "lww_kvs.h"
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"
#include "server_utility.h"

using namespace std;

pair<LWW_KVS_PairLattice<string>, unsigned> process_get(const string& key, Serializer* serializer) {
  unsigned err_number = 0;
  auto res = serializer->get(key, err_number);
  // check if the value is an empty string
  if (res.reveal().value == "") {
    err_number = 1;
  }
  return pair<LWW_KVS_PairLattice<string>, unsigned>(res, err_number);
}

void process_put(const string& key,
    const unsigned long long& timestamp,
    const string& value,
    Serializer* serializer) {
  serializer->put(key, value, timestamp);
}

communication::Response process_request(
    communication::Request& req,
    unordered_set<string>& local_changeset,
    Serializer* serializer,
    server_thread_t& wt,
    chrono::system_clock::time_point& start_time) {
  communication::Response response;
  string respond_id = "";
  if (req.has_request_id()) {
    respond_id = req.request_id();
    response.set_response_id(respond_id);
  }
  if (req.type() == "GET") {
    for (int i = 0; i < req.tuple_size(); i++) {
      string key = req.tuple(i).key();
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(key);
      auto res = process_get(key, serializer);
      tp->set_value(res.first.reveal().value);
      tp->set_err_number(res.second);
    }
  } else if (req.type() == "PUT") {
    for (int i = 0; i < req.tuple_size(); i++) {
      string key = req.tuple(i).key();
      communication::Response_Tuple* tp = response.add_tuple();
      tp->set_key(key);
      auto current_time = chrono::system_clock::now();
      auto ts = generate_timestamp(chrono::duration_cast<chrono::milliseconds>(current_time-start_time).count(), wt.get_tid());
      process_put(key, ts, req.tuple(i).value(), serializer);
      tp->set_err_number(0);
      local_changeset.insert(key);
    }
  }
  return response;
}

void process_gossip(
    communication::Request& gossip,
    server_thread_t& wt,
    global_hash_t& global_hash_ring,
    local_hash_t& local_hash_ring,
    Serializer* serializer) {
  for (int i = 0; i < gossip.tuple_size(); i++) {
    process_put(gossip.tuple(i).key(), gossip.tuple(i).timestamp(), gossip.tuple(i).value(), serializer);
  }
}

void send_gossip(address_keyset_map& addr_keyset_map, SocketCache& pushers, Serializer* serializer) {
  unordered_map<string, communication::Request> gossip_map;

  for (auto map_it = addr_keyset_map.begin(); map_it != addr_keyset_map.end(); map_it++) {
    gossip_map[map_it->first].set_type("PUT");
    for (auto set_it = map_it->second.begin(); set_it != map_it->second.end(); set_it++) {
      auto res = process_get(*set_it, serializer);
      if (res.second == 0) {
        //cerr << "gossiping key " + *set_it + " to address " + map_it->first + "\n";
        prepare_put_tuple(gossip_map[map_it->first], *set_it, res.first.reveal().value, res.first.reveal().timestamp);
      }
    }
  }
  // send gossip
  for (auto it = gossip_map.begin(); it != gossip_map.end(); it++) {
    push_request(it->second, pushers[it->first]);
  }
}

// thread entry point
void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("server");

  // each thread has a handle to itself
  server_thread_t wt = server_thread_t(ip, thread_id);

  // prepare the zmq context
  zmq::context_t context(1);
  //zmq_ctx_set(&context, ZMQ_IO_THREADS, 2);

  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash rings
  global_hash_t global_hash_ring;
  local_hash_t local_hash_ring;

  vector<string> proxy_address;

  // read address of proxies from conf file
  string ip_line;
  ifstream address;
  address.open("conf/server/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  address.open("conf/server/seed_server.txt");
  getline(address, ip_line);
  address.close();

  logger->info("seed address is {}", ip_line);

  // request server addresses from the seed node
  zmq::socket_t addr_requester(context, ZMQ_REQ);
  addr_requester.connect(proxy_thread_t(ip_line, 0).get_seed_connect_addr());
  zmq_util::send_string("join", &addr_requester);

  // receive and add all the addresses that seed node sent
  string serialized_addresses = zmq_util::recv_string(&addr_requester);
  communication::Address addresses;
  addresses.ParseFromString(serialized_addresses);
  // populate start time
  unsigned long long duration = addresses.start_time();
  chrono::milliseconds dur(duration);
  chrono::system_clock::time_point start_time(dur);
  // populate addresses
  for (int i = 0; i < addresses.tuple_size(); i++) {
    insert_to_hash_ring<global_hash_t>(global_hash_ring, addresses.tuple(i).ip(), 0);
  }

  // add itself to global hash ring
  insert_to_hash_ring<global_hash_t>(global_hash_ring, ip, 0);

  // form local hash rings
  for (unsigned tid = 0; tid < THREAD_NUM; tid++) {
    insert_to_hash_ring<local_hash_t>(local_hash_ring, ip, tid);
  }

  // thread 0 notifies other servers that it has joined
  if (thread_id == 0) {
    unordered_set<string> observed_ip;
    for (auto iter = global_hash_ring.begin(); iter != global_hash_ring.end(); iter++) {
      if (iter->second.get_ip().compare(ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
        zmq_util::send_string(ip, &pushers[(iter->second).get_node_join_connect_addr()]);
        observed_ip.insert(iter->second.get_ip());
      }
    }

    string msg = "join:" + ip;
    // notify proxies that this node has joined
    for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
      zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
    }
  }

  Database* kvs = new Database();
  Serializer* serializer = new Serializer(kvs);

  // keep track of the key set
  unordered_set<string> keys;

  // the set of changes made on this thread since the last round of gossip
  unordered_set<string> local_changeset;

  // listens for a new node joining
  zmq::socket_t join_puller(context, ZMQ_PULL);
  join_puller.bind(wt.get_node_join_bind_addr());
  // listens for a node departing
  zmq::socket_t depart_puller(context, ZMQ_PULL);
  depart_puller.bind(wt.get_node_depart_bind_addr());
  // responsible for listening for a command that this node should leave
  zmq::socket_t self_depart_puller(context, ZMQ_PULL);
  self_depart_puller.bind(wt.get_self_depart_bind_addr());
  // responsible for handling requests
  zmq::socket_t request_puller(context, ZMQ_PULL);
  request_puller.bind(wt.get_request_pulling_bind_addr());
  // responsible for processing gossips
  zmq::socket_t gossip_puller(context, ZMQ_PULL);
  gossip_puller.bind(wt.get_gossip_bind_addr());

  //  Initialize poll set
  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(join_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(self_depart_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(request_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(gossip_puller), 0, ZMQ_POLLIN, 0 },
  };

  auto gossip_start = chrono::system_clock::now();
  auto gossip_end = chrono::system_clock::now();

  // enter event loop
  while (true) {
    zmq_util::poll(0, &pollitems);

    // receives a node join
    if (pollitems[0].revents & ZMQ_POLLIN) {
      string new_server_ip = zmq_util::recv_string(&join_puller);

      // update global hash ring
      bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring, new_server_ip, 0);

      if (inserted) {
        logger->info("Received a node join. New node is {}", new_server_ip);
        // only relevant to thread 0
        if (thread_id == 0) {
          // gossip the new node address between server nodes to ensure consistency
          unordered_set<string> observed_ip;
          for (auto iter = global_hash_ring.begin(); iter != global_hash_ring.end(); iter++) {
            if (iter->second.get_ip().compare(ip) != 0 && iter->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
              // if the node is not myself and not the newly joined node, send the ip of the newly joined node
              zmq_util::send_string(new_server_ip, &pushers[(iter->second).get_node_join_connect_addr()]);
              observed_ip.insert(iter->second.get_ip());
            } else if (iter->second.get_ip().compare(new_server_ip) == 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
              // if the node is the newly joined node, send my ip
              zmq_util::send_string(ip, &pushers[(iter->second).get_node_join_connect_addr()]);
              observed_ip.insert(iter->second.get_ip());
            }
          }
          // tell all worker threads about the new node join
          for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
            zmq_util::send_string(new_server_ip, &pushers[server_thread_t(ip, tid).get_node_join_connect_addr()]);
          }
          logger->info("global hash ring size is {}", to_string(global_hash_ring.size()));
        }
        // map from worker address to a set of keys
        address_keyset_map addr_keyset_map;
        // keep track of which key should be removed
        unordered_set<string> remove_set;
        for (auto it = keys.begin(); it != keys.end(); it++) {
          string key = *it;
          auto threads = get_responsible_threads(key, global_hash_ring, local_hash_ring);
          if (threads.find(wt) == threads.end()) {
            remove_set.insert(key);
            for (auto iter = threads.begin(); iter != threads.end(); iter++) {
              addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
            }
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        // remove keys
        for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
          keys.erase(*it);
          serializer->remove(*it);
        }
      }
    }

    // receives a node departure notice
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string departing_server_ip = zmq_util::recv_string(&depart_puller);

      logger->info("Received departure for node {}", departing_server_ip);
      // update hash ring
      remove_from_hash_ring<global_hash_t>(global_hash_ring, departing_server_ip, 0);
      if (thread_id == 0) {
        // tell all worker threads about the node departure
        for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
          zmq_util::send_string(departing_server_ip, &pushers[server_thread_t(ip, tid).get_node_depart_connect_addr()]);
        }
        logger->info("global hash ring size is {}", to_string(global_hash_ring.size()));
      }
    }

    // receives a node departure request
    if (pollitems[2].revents & ZMQ_POLLIN) {
      string ack_addr = zmq_util::recv_string(&self_depart_puller);
      logger->info("Node is departing");
      remove_from_hash_ring<global_hash_t>(global_hash_ring, ip, 0);
      if (thread_id == 0) {
          unordered_set<string> observed_ip;
          for (auto iter = global_hash_ring.begin(); iter != global_hash_ring.end(); iter++) {
            if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
              zmq_util::send_string(ip, &pushers[(iter->second).get_node_depart_connect_addr()]);
              observed_ip.insert(iter->second.get_ip());
            }
          }
        string msg = "depart:" + ip;
        // notify proxies
        for (auto it = proxy_address.begin(); it != proxy_address.end(); it++) {
          zmq_util::send_string(msg, &pushers[proxy_thread_t(*it, 0).get_notify_connect_addr()]);
        }
        // tell all worker threads about the self departure
        for (unsigned tid = 1; tid < THREAD_NUM; tid++) {
          zmq_util::send_string(ack_addr, &pushers[server_thread_t(ip, tid).get_self_depart_connect_addr()]);
        }
      }

      address_keyset_map addr_keyset_map;
      for (auto it = keys.begin(); it != keys.end(); it++) {
        string key = *it;
        auto threads = get_responsible_threads(key, global_hash_ring, local_hash_ring);
        // since we already removed itself from the hash ring, no need to exclude itself from threads
        for (auto iter = threads.begin(); iter != threads.end(); iter++) {
          addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
        }
      }

      send_gossip(addr_keyset_map, pushers, serializer);
      zmq_util::send_string(ip, &pushers[ack_addr]);
    }

    // receives a request
    if (pollitems[3].revents & ZMQ_POLLIN) {
      string serialized_req = zmq_util::recv_string(&request_puller);
      communication::Request req;
      req.ParseFromString(serialized_req);
      //  process request
      auto response = process_request(req, local_changeset, serializer, wt, start_time);
      if (response.tuple_size() > 0 && req.has_respond_address()) {
        string serialized_response;
        response.SerializeToString(&serialized_response);
        zmq_util::send_string(serialized_response, &pushers[req.respond_address()]);
      }
    }

    // receives a gossip
    if (pollitems[4].revents & ZMQ_POLLIN) {
      string serialized_gossip = zmq_util::recv_string(&gossip_puller);
      communication::Request gossip;
      gossip.ParseFromString(serialized_gossip);
      //  Process distributed gossip
      process_gossip(gossip, wt, global_hash_ring, local_hash_ring, serializer);
    }

    gossip_end = chrono::system_clock::now();
    if (chrono::duration_cast<chrono::microseconds>(gossip_end-gossip_start).count() >= PERIOD) {
      // only gossip if we have changes
      if (local_changeset.size() > 0) {
        address_keyset_map addr_keyset_map;

        for (auto it = local_changeset.begin(); it != local_changeset.end(); it++) {
          string key = *it;
          auto threads = get_responsible_threads(key, global_hash_ring, local_hash_ring);
          for (auto iter = threads.begin(); iter != threads.end(); iter++) {
            if (iter->get_id() != wt.get_id()) {
              addr_keyset_map[iter->get_gossip_connect_addr()].insert(key);
            }
          }
        }

        send_gossip(addr_keyset_map, pushers, serializer);
        local_changeset.clear();
      }
      gossip_start = chrono::system_clock::now();
    }
  }
}

int main(int argc, char* argv[]) {

  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  // debugging
  cerr << "worker thread number is " + to_string(THREAD_NUM) + "\n";

  vector<thread> worker_threads;

  // start the initial threads based on THREAD_NUM
  for (unsigned thread_id = 1; thread_id < THREAD_NUM; thread_id++) {
    worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}