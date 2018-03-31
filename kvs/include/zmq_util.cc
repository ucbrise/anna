#include "zmq_util.h"

#include <iomanip>
#include <ios>

namespace zmq_util {

std::string message_to_string(const zmq::message_t& message) {
  return std::string(static_cast<const char*>(message.data()), message.size());
}

zmq::message_t string_to_message(const std::string& s) {
  zmq::message_t msg(s.size());
  memcpy(msg.data(), s.c_str(), s.size());
  return msg;
}

void send_string(const std::string& s, zmq::socket_t* socket) {
  socket->send(string_to_message(s));
}

std::string recv_string(zmq::socket_t* socket) {
  zmq::message_t message;
  socket->recv(&message);
  return message_to_string(message);
}

void send_msg(void* payload, zmq::socket_t* socket) {
  zmq::message_t msg(sizeof(void*));
  memcpy(msg.data(), &payload, sizeof(void*));
  socket->send(msg);
}

void recv_msg(zmq::socket_t* socket, zmq::message_t& msg) {
  socket->recv(&msg);
}

void send_msgs(std::vector<zmq::message_t> msgs, zmq::socket_t* socket) {
  for (std::size_t i = 0; i < msgs.size(); ++i) {
    socket->send(msgs[i], i == msgs.size() - 1 ? 0 : ZMQ_SNDMORE);
  }
}

bool recv_msgs(zmq::socket_t* socket, std::vector<zmq::message_t>& msgs) {
  bool res;
  int more = true;
  std::size_t more_size = sizeof(more);
  while (more) {
    msgs.emplace_back();
    if ((res = socket->recv(&msgs.back(), ZMQ_DONTWAIT)) == false) return false;
    socket->getsockopt(ZMQ_RCVMORE, static_cast<void*>(&more), &more_size);
  }
  return true;
}

int poll(long timeout, std::vector<zmq::pollitem_t>* items) {
  return zmq::poll(items->data(), items->size(), timeout);
}

}  // namespace zmq_util
