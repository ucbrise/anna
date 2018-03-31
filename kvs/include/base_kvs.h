#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <memory>
#include "core_lattices.h"

using namespace std;

template <typename K, typename V>
class KV_Store{
protected:
  MapLattice<K, V> db;
public:
  KV_Store<K, V>() {}
  KV_Store<K, V>(MapLattice<K, V> &other) {
    db = other;
  }
  V get(const K& k, unsigned& err_number) {
    if (!db.contain(k).reveal()) {
      err_number = 1;
    }
    return db.at(k);
  }
  bool put(const K& k, const V &v) {
    return db.at(k).Merge(v);
  }
  void remove(const K& k) {
    db.remove(k);
  }
};