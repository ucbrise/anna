#include <stdio.h>
#include <stdlib.h>
#include "base_kvs.h"

// Lattice for implementing read (un)committed and item-cut isolation

template <typename T>
struct timestamp_value_pair {
	// MapLattice<int, MaxLattice<int>> v_map;
	int timestamp {-1};
	T value;

	timestamp_value_pair<T>() {
		timestamp = -1;
		value = T();
	}
	// need this because of static cast
	timestamp_value_pair<T>(int a) {
		timestamp = -1;
		value = T();
	}
	timestamp_value_pair<T>(int ts, T v) {
		timestamp = ts;
		value = v;
	}
};

template <typename T>
class RC_KVS_PairLattice : public Lattice<timestamp_value_pair<T>> {
protected:
    void do_merge(const timestamp_value_pair<T> &p) {
    	if (p.timestamp >= this -> element.timestamp) {
    		this -> element.timestamp = p.timestamp;
    		this -> element.value = p.value;
    	} 
    }
public:
    RC_KVS_PairLattice() : Lattice<timestamp_value_pair<T>>() {}
    RC_KVS_PairLattice(const timestamp_value_pair<T> &p)  : Lattice<timestamp_value_pair<T>>(p) {}

    // this Merge is specific to lww lattice
    // return true if the new value replaced the old value
    // return false otherwise
    bool Merge(const timestamp_value_pair<T>& p) {
      if (p.timestamp >= this -> element.timestamp) {
        this -> element.timestamp = p.timestamp;
        this -> element.value = p.value;
        return true;
      } else {
        return false;
      }
    }

    bool Merge(const RC_KVS_PairLattice<T>& pl) {
      return Merge(pl.reveal());
    }
};