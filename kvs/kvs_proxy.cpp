#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "consistent_hash_map.hpp"
#include "common.h"

using namespace std;

void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("proxy");

  proxy_thread_t pt = proxy_thread_t(ip, thread_id);

  // prepare the zmq context
  zmq::context_t context(1);

  SocketCache pushers(&context, ZMQ_PUSH);

  // initialize hash rings
  global_hash_t global_hash_ring;
  local_hash_t local_hash_ring;

  // form local hash rings
  for (unsigned tid = 0; tid < THREAD_NUM; tid++) {
    insert_to_hash_ring<local_hash_t>(local_hash_ring, ip, tid);
  }

  // responsible for sending existing server addresses to a new node (relevant to seed node)
  zmq::socket_t addr_responder(context, ZMQ_REP);
  addr_responder.bind(pt.get_seed_bind_addr());
  // responsible for both node join and departure
  zmq::socket_t notify_puller(context, ZMQ_PULL);
  notify_puller.bind(pt.get_notify_bind_addr());
  // responsible for handling key address request from users
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.bind(pt.get_key_address_bind_addr());  

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(addr_responder), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(notify_puller), 0, ZMQ_POLLIN, 0 },
    { static_cast<void *>(key_address_puller), 0, ZMQ_POLLIN, 0 }
  };

  auto start_time = chrono::system_clock::now();
  auto start_time_ms = chrono::time_point_cast<std::chrono::milliseconds>(start_time);

  auto value = start_time_ms.time_since_epoch();
  unsigned long long duration = value.count();

  while (true) {
    zmq_util::poll(-1, &pollitems);

    // only relavant for the seed node
    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("Received an address request");
      zmq_util::recv_string(&addr_responder);

      communication::Address address;
      address.set_start_time(duration);
      unordered_set<string> observed_ip;
      for (auto iter = global_hash_ring.begin(); iter != global_hash_ring.end(); iter++) {
        if (observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
          communication::Address_Tuple* tp = address.add_tuple();
          tp->set_ip(iter->second.get_ip());
          observed_ip.insert(iter->second.get_ip());
        }
      }

      string serialized_address;
      address.SerializeToString(&serialized_address);
      zmq_util::send_string(serialized_address, &addr_responder);
    }

    // handle a join or depart event coming from the server side
    if (pollitems[1].revents & ZMQ_POLLIN) {
      string message = zmq_util::recv_string(&notify_puller);

      vector<string> v;
      split(message, ':', v);
      string type = v[0];
      string new_server_ip = v[1];
      if (type == "join") {
        logger->info("received join");
        logger->info("new server ip is {}", new_server_ip);
        // update hash ring
        bool inserted = insert_to_hash_ring<global_hash_t>(global_hash_ring, new_server_ip, 0);
        if (inserted) {
          if (thread_id == 0) {
            // gossip the new node address between server nodes to ensure consistency
            unordered_set<string> observed_ip;
            for (auto iter = global_hash_ring.begin(); iter != global_hash_ring.end(); iter++) {
              if (iter->second.get_ip().compare(new_server_ip) != 0 && observed_ip.find(iter->second.get_ip()) == observed_ip.end()) {
                // if the node is not the newly joined node, send the ip of the newly joined node
                zmq_util::send_string(new_server_ip, &pushers[(iter->second).get_node_join_connect_addr()]);
                observed_ip.insert(iter->second.get_ip());
              }
            }
            // tell all worker threads about the message
            for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
              zmq_util::send_string(message, &pushers[proxy_thread_t(ip, tid).get_notify_connect_addr()]);
            }
          }
        }

        logger->info("global hash ring size is {}", to_string(global_hash_ring.size()));
      } else if (type == "depart") {
        logger->info("received depart");
        logger->info("departing server ip is {}", new_server_ip);
        // update hash ring
        remove_from_hash_ring<global_hash_t>(global_hash_ring, new_server_ip, 0);

        if (thread_id == 0) {
          // tell all worker threads about the message
          for (unsigned tid = 1; tid < PROXY_THREAD_NUM; tid++) {
            zmq_util::send_string(message, &pushers[proxy_thread_t(ip, tid).get_notify_connect_addr()]);
          }
        }

        logger->info("global hash ring size is {}", to_string(global_hash_ring.size()));
      }
    }

    if (pollitems[2].revents & ZMQ_POLLIN) {
      //cerr << "received key address request\n";
      string serialized_key_req = zmq_util::recv_string(&key_address_puller);
      communication::Key_Request key_req;
      key_req.ParseFromString(serialized_key_req);

      communication::Key_Response key_res;
      key_res.set_response_id(key_req.request_id());

      for (int i = 0; i < key_req.keys_size(); i++) {
        string key = key_req.keys(i);
        auto threads = get_responsible_threads(key, global_hash_ring, local_hash_ring);
        communication::Key_Response_Tuple* tp = key_res.add_tuple();
        tp->set_key(key);
        for (auto it = threads.begin(); it != threads.end(); it++) {
          tp->add_addresses(it->get_request_pulling_connect_addr());
        }
      }
      string serialized_key_res;
      key_res.SerializeToString(&serialized_key_res);
      zmq_util::send_string(serialized_key_res, &pushers[key_req.respond_address()]);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  vector<thread> proxy_worker_threads;

  for (unsigned thread_id = 1; thread_id < PROXY_THREAD_NUM; thread_id++) {
    proxy_worker_threads.push_back(thread(run, thread_id));
  }

  run(0);
}