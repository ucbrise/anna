#ifndef __SERVER_UTILITY_H__
#define __SERVER_UTILITY_H__

#include <string>
#include "message.pb.h"
#include "socket_cache.h"
#include "zmq_util.h"

using namespace std;

// Define the gossip period (frequency)
#define PERIOD 10000000

typedef KV_Store<string, LWW_KVS_PairLattice<string>> Database;

// a map that represents which keys should be sent to which IP-port combinations
typedef unordered_map<string, unordered_set<string>> address_keyset_map;

class Serializer {
  Database* kvs_;
public:
  Serializer(Database* kvs): kvs_(kvs) {}
  LWW_KVS_PairLattice<string> get(const string& key, unsigned& err_number) {
    return kvs_->get(key, err_number);
  }
  bool put(const string& key, const string& value, const unsigned& timestamp) {
    timestamp_value_pair<string> p = timestamp_value_pair<string>(timestamp, value);
    return kvs_->put(key, LWW_KVS_PairLattice<string>(p));
  }
  void remove(const string& key) {
    kvs_->remove(key);
  }
};

// form the timestamp given a time and a thread id
unsigned long long generate_timestamp(unsigned long long time, unsigned tid) {
    unsigned pow = 10;
    while(tid >= pow)
        pow *= 10;
    return time * pow + tid;        
}

#endif