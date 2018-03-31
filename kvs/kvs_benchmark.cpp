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

double get_base(unsigned N, double skew) {
  double base = 0;
  for (unsigned k = 1; k <= N; k++) {
    base += pow(k, -1*skew);
  }
  return (1/ base);
}

double get_zipf_prob(unsigned rank, double skew, double base) {
  return pow(rank, -1*skew) / base;
}

int sample(int n, unsigned& seed, double base, unordered_map<unsigned, double>& sum_probs)
{
  double z;                     // Uniform random number (0 < z < 1)
  int zipf_value;               // Computed exponential value to be returned
  int    i;                     // Loop counter
  int low, high, mid;           // Binary-search bounds

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = rand_r(&seed) / static_cast<double>(RAND_MAX);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  low = 1, high = n;
  do {
    mid = floor((low+high)/2);
    if (sum_probs[mid] >= z && sum_probs[mid-1] < z) {
      zipf_value = mid;
      break;
    } else if (sum_probs[mid] >= z) {
      high = mid-1;
    } else {
      low = mid+1;
    }
  } while (low <= high);

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return(zipf_value);
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

void run(unsigned thread_id) {

  string log_file = "log_" + to_string(thread_id) + ".txt";
  string logger_name = "basic_logger_" + to_string(thread_id);
  auto logger = spdlog::basic_logger_mt(logger_name, log_file, true);
  logger->flush_on(spdlog::level::info);

  string ip = get_ip("user");

  hash<string> hasher;
  unsigned seed = time(NULL);
  seed += hasher(ip);
  seed += thread_id;
  logger->info("seed is {}", seed);

  // read in the proxy addresses
  vector<string> proxy_address;

  // mapping from key to a set of worker addresses
  unordered_map<string, unordered_set<string>> key_address_cache;
  // rep factor map
  unordered_map<string, pair<double, unsigned>> rep_factor_map;

  user_thread_t ut = user_thread_t(ip, thread_id);

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
  // responsible for pulling benchmark command
  zmq::socket_t command_puller(context, ZMQ_PULL);
  command_puller.bind("tcp://*:" + to_string(thread_id + COMMAND_BASE_PORT));

  vector<zmq::pollitem_t> pollitems = {
    { static_cast<void *>(command_puller), 0, ZMQ_POLLIN, 0 }
  };

  unsigned rid = 0;

  while (true) {
    zmq_util::poll(-1, &pollitems);

    if (pollitems[0].revents & ZMQ_POLLIN) {
      logger->info("received benchmark command");
      vector<string> v;
      split(zmq_util::recv_string(&command_puller), ':', v);
      string mode = v[0];

      if (mode == "CACHE") {
        unsigned num_keys = stoi(v[1]);
        // warm up cache
        key_address_cache.clear();
        auto warmup_start = std::chrono::system_clock::now();
        for (unsigned i = 1; i <= num_keys; i++) {
          // key is 8 bytes
          string key = string(8 - to_string(i).length(), '0') + to_string(i);
          if (i % 50000 == 0) {
            logger->info("warming up cache for key {}", key);
          }
          string target_proxy_address = get_random_proxy_thread(proxy_address, seed).get_key_address_connect_addr();
          bool succeed;
          auto addresses = get_address_from_proxy(ut, key, pushers[target_proxy_address], key_address_puller, succeed, ip, thread_id, rid);
          if (succeed) {
            for (auto it = addresses.begin(); it != addresses.end(); it++) {
              key_address_cache[key].insert(*it);
            }
          } else {
            logger->info("timeout during cache warmup");
          }
        }
        auto warmup_time = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-warmup_start).count();
        logger->info("warming up cache took {} seconds", warmup_time);
      } else if (mode == "LOAD") {
        string type = v[1];
        unsigned num_keys = stoi(v[2]);
        unsigned length = stoi(v[3]);
        unsigned report_period = stoi(v[4]);
        unsigned time = stoi(v[5]);
        double contention = stod(v[6]);

        double zipf = contention;
        logger->info("zipf coefficient is {}", zipf);
        double base = get_base(num_keys, zipf);
        unordered_map<unsigned, double> sum_probs;
        sum_probs[0] = 0;
        for (unsigned i = 1; i <= num_keys; i++) {
          sum_probs[i] = sum_probs[i-1] + base / pow((double) i, zipf);
        }

        size_t count = 0;
        auto benchmark_start = std::chrono::system_clock::now();
        auto benchmark_end = std::chrono::system_clock::now();
        auto epoch_start = std::chrono::system_clock::now();
        auto epoch_end = std::chrono::system_clock::now();
        auto total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
        unsigned epoch = 1;

        while (true) {
          string key;
          unsigned k = sample(num_keys, seed, base, sum_probs);
          key = string(8 - to_string(k).length(), '0') + to_string(k);
          unsigned trial = 1;
          if (type == "G") {
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "P") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 1;
          } else if (type == "M") {
            handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            trial = 1;
            handle_request(key, "", pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
            count += 2;
          } else {
            logger->info("invalid request type");
          }

          epoch_end = std::chrono::system_clock::now();
          auto time_elapsed = chrono::duration_cast<std::chrono::seconds>(epoch_end-epoch_start).count();
          // report throughput every report_period seconds
          if (time_elapsed >= report_period) {
            double throughput = (double)count / (double)time_elapsed;
            logger->info("Throughput is {} ops/seconds", throughput);
            logger->info("epoch is {}", epoch);
            epoch += 1;
            count = 0;
            epoch_start = std::chrono::system_clock::now();
          }

          benchmark_end = std::chrono::system_clock::now();
          total_time = chrono::duration_cast<std::chrono::seconds>(benchmark_end-benchmark_start).count();
          if (total_time > time) {
            break;
          }
          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }
        }

        logger->info("Finished");
      } else if (mode == "WARM") {
        unsigned num_keys = stoi(v[1]);
        unsigned length = stoi(v[2]);
        unsigned total_threads = stoi(v[3]);
        unsigned range = num_keys / total_threads;
        unsigned start = thread_id * range + 1;
        unsigned end = thread_id * range + 1 + range;
        string key;
        logger->info("Warming up data");
        auto warmup_start = std::chrono::system_clock::now();
        for (unsigned i = start; i < end; i++) {
          unsigned trial = 1;
          key = string(8 - to_string(i).length(), '0') + to_string(i);
          handle_request(key, string(length, 'a'), pushers, proxy_address, key_address_cache, seed, logger, ut, response_puller, key_address_puller, ip, thread_id, rid, trial);
          // reset rid
          if (rid > 10000000) {
            rid = 0;
          }
          if (i == (end - start)/2) {
            logger->info("Warmed up half");
          }
        }
        logger->info("Finished warming up");
        auto warmup_time = chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now()-warmup_start).count();
        logger->info("warming up data took {} seconds", warmup_time);
      } else {
        logger->info("Invalid mode");
      }
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc != 1) {
    cerr << "usage:" << argv[0] << endl;
    return 1;
  }

  vector<thread> benchmark_threads;

  for (unsigned thread_id = 1; thread_id < BENCHMARK_THREAD_NUM; thread_id++) {
    benchmark_threads.push_back(thread(run, thread_id));
  }

  run(0);
}