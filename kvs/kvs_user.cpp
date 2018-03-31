#include <zmq.hpp>
#include <string>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <vector>
#include <iostream>
#include <unistd.h>
#include <memory>
#include <unordered_set>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"
#include "common.h"

using namespace std;

unsigned thread_id = 0;

// query the proxy for a key and return all address
vector<string> get_address_from_proxy(
    user_thread_t& ut,
    string key,
    zmq::socket_t& sending_socket,
    zmq::socket_t& receiving_socket,
    bool& succeed,
    string& ip,
    unsigned& thread_id,
    unsigned& rid) {
  communication::Key_Request key_req;
  key_req.set_respond_address(ut.get_key_address_connect_addr());
  key_req.add_keys(key);
  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  key_req.set_request_id(req_id);
  rid += 1;
  // query proxy for addresses on the other tier
  auto key_response = send_request<communication::Key_Request, communication::Key_Response>(key_req, sending_socket, receiving_socket, succeed);
  vector<string> result;
  if (succeed) {
    for (int j = 0; j < key_response.tuple(0).addresses_size(); j++) {
      result.push_back(key_response.tuple(0).addresses(j));
    }
  }
  return result;
}

proxy_thread_t get_random_proxy_thread(vector<string>& proxy_address, unsigned& seed) {
  string proxy_ip = proxy_address[rand_r(&seed) % proxy_address.size()];
  unsigned tid = rand_r(&seed) % PROXY_THREAD_NUM;
  return proxy_thread_t(proxy_ip, tid);
}

void handle_request(
    string key,
    string value,
    SocketCache& pushers,
    vector<string>& proxy_address,
    unordered_map<string, unordered_set<string>>& key_address_cache,
    unsigned& seed,
    shared_ptr<spdlog::logger> logger,
    user_thread_t& ut,
    zmq::socket_t& response_puller,
    zmq::socket_t& key_address_puller,
    string& ip,
    unsigned& thread_id,
    unsigned& rid,
    unsigned& trial) {
  if (trial > 5) {
    logger->info("trial is {} for request for key {}", trial, key);
    cerr << "trial is " + to_string(trial) + " for key " + key + "\n";
    logger->info("Waiting for 5 seconds");
    cerr << "Waiting for 5 seconds\n";
    chrono::seconds dura(5);
    this_thread::sleep_for(dura);
    logger->info("Waited 5s");
    cout << "Waited 5s\n";
  }
  // get worker address
  string worker_address;
  if (key_address_cache.find(key) == key_address_cache.end()) {
    // query the proxy and update the cache
    string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
    bool succeed;
    auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed, ip, thread_id, rid);
    if (succeed) {
      for (auto it = addresses.begin(); it != addresses.end(); it++) {
        key_address_cache[key].insert(*it);
      }
      worker_address = addresses[rand_r(&seed) % addresses.size()];
    } else {
      logger->info("request timed out when querying proxy, this should never happen");
      return;
    }
  } else {
    if (key_address_cache[key].size() == 0) {
      cerr << "address cache for key " + key + " has size 0\n";
    }
    worker_address = *(next(begin(key_address_cache[key]), rand_r(&seed) % key_address_cache[key].size()));
  }
  communication::Request req;
  req.set_respond_address(ut.get_request_pulling_connect_addr());
  string req_id = ip + ":" + to_string(thread_id) + "_" + to_string(rid);
  req.set_request_id(req_id);
  rid += 1;
  if (value == "") {
    // get request
    req.set_type("GET");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_num_address(key_address_cache[key].size());
  } else {
    // put request
    req.set_type("PUT");
    communication::Request_Tuple* tp = req.add_tuple();
    tp->set_key(key);
    tp->set_value(value);
    tp->set_timestamp(0);
    tp->set_num_address(key_address_cache[key].size());
  }
  bool succeed;
  auto res = send_request<communication::Request, communication::Response>(req, pushers[worker_address], response_puller, succeed);
  if (succeed) {
    // initialize the respond string
    if (res.tuple(0).err_number() == 2) {
      trial += 1;
      // update cache and retry
      //logger->info("cache invalidation due to wrong address");
      key_address_cache.erase(key);
      handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
    } else {
      if (req.type() == "GET") {
        cout << "key " + res.tuple(0).key() + " has value " + res.tuple(0).value() + "\n";
      } else {
        cout << "key " + res.tuple(0).key() + " successfully put\n";
      }
    }
  } else {
    logger->info("request timed out when querying worker, clearing cache due to possible node membership change");
    cerr << "request timed out when querying worker, clearing cache due to possible node membership change\n";
    // likely the node has departed. We clear the entries relavant to the worker_address
    vector<string> tokens;
    split(worker_address, ':', tokens);
    string signature = tokens[1];
    unordered_set<string> remove_set;
    for (auto it = key_address_cache.begin(); it != key_address_cache.end(); it++) {
      for (auto iter = it->second.begin(); iter != it->second.end(); iter++) {
        vector<string> v;
        split(*iter, ':', v);
        if (v[1] == signature) {
          remove_set.insert(it->first);
        }
      }
    }
    for (auto it = remove_set.begin(); it != remove_set.end(); it++) {
      key_address_cache.erase(*it);
    }
    trial += 1;
    handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("user");

  hash<string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  logger->info("seed is {}", seed);

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;
  // rep factor map
  unordered_map<string, pair<double, unsigned>> rep_factor_map;

  user_thread_t ut = user_thread_t(ip, 0);

  // read proxy address from the file
  string ip_line;
  ifstream address;
  address.open("conf/user/proxy_address.txt");
  while (getline(address, ip_line)) {
    proxy_address.push_back(ip_line);
  }
  address.close();

  zmq::context_t context(1);
  //zmq_ctx_set(&context, ZMQ_IO_THREADS, 3);

  SocketCache pushers(&context, ZMQ_PUSH);

  int timeout = 10000;
  // responsible for pulling response
  zmq::socket_t response_puller(context, ZMQ_PULL);
  response_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  response_puller.bind(ut.get_request_pulling_bind_addr());
  // responsible for receiving depart done notice
  zmq::socket_t key_address_puller(context, ZMQ_PULL);
  key_address_puller.setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  key_address_puller.bind(ut.get_key_address_bind_addr());


  unsigned rid = 0;

  string input;
  while (true) {
    cout << "kvs> ";
    getline(cin, input);
    vector<string> v;
    split(input, ' ', v);
    if (v[0] == "GET") {
      if (v.size() != 2) {
        cerr << "Invalid argument number\n";
      } else {
        unsigned trial = 1;
        string key = v[1];
        handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
      }
    } else if (v[0] == "PUT") {
      if (v.size() != 3) {
        cerr << "Invalid argument number\n";
      } else {
        unsigned trial = 1;
        string key = v[1];
        string value = v[2];
        handle_request(key, value, pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
      }
    } else {
      cerr << "Invalid operation type\n";
    }
    // reset rid
    if (rid > 10000000) {
      rid = 0;
    }
  }
}