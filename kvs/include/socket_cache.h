#ifndef SOCKET_CACHE_H_
#define SOCKET_CACHE_H_

#include <map>
#include <string>

#include "zmq.hpp"


// A SocketCache is a map from ZeroMQ addresses to PUSH ZeroMQ sockets. The
// socket corresponding to address `address` can be retrieved from a
// SocketCache `cache` with `cache[address]` or `cache.At(address)`. If a
// socket with a given address is not in the cache when it is requested, one is
// created and connected to the address. An example:
//
//   zmq::context_t context(1);
//   SocketCache cache(&context);
//   // This will create a socket and connect it to "inproc://a".
//   zmq::socket_t& a = cache["inproc://a"];
//   // This will not createa new socket. It will return the socket created in
//   // the previous line. In other words, a and the_same_a_as_before are
//   // references to the same socket.
//   zmq::socket_t& the_same_a_as_before = cache["inproc://a"];
//   // cache.At("inproc://a") is 100% equivalent to cache["inproc://a"].
//   zmq::socket_t& another_a = cache.At("inproc://a");
class SocketCache {
 public:
  explicit SocketCache(zmq::context_t* context, int type) : context_(context), type_(type) {}
  zmq::socket_t& At(const std::string& addr);
  zmq::socket_t& operator[](const std::string& addr);
  void clear_cache();

 private:
  zmq::context_t* context_;
  std::map<std::string, zmq::socket_t> cache_;
  int type_;
};


#endif  // SOCKET_CACHE_H_
