#include <stdio.h>
#include <stdlib.h>
#include "base_kvs.h"

// Lattice for implementing causally ordered eventual consistency

template <typename T>
struct version_value_pair {
	MapLattice<int, MaxLattice<int>> v_map;
	T value;

	version_value_pair<T>() {
		v_map = MapLattice<int, MaxLattice<int>>();
		value = T();
	}
	// need this because of static cast
	version_value_pair<T>(int a) {
		v_map = MapLattice<int, MaxLattice<int>>();
		value = T();
	}
	version_value_pair<T>(MapLattice<int, MaxLattice<int>> m, T v) {
		v_map = m;
		value = v;
	}
};

template <typename T>
class KVS_PairLattice : public Lattice<version_value_pair<T>> {
protected:
    void do_merge(const version_value_pair<T> &p) {
    	MapLattice<int, MaxLattice<int>> prev = this->element.v_map;
    	this->element.v_map.merge(p.v_map);

    	if (this->element.v_map == prev);
    	else if (this->element.v_map == p.v_map) {
    		this->element.value.assign(p.value);
    	}
    	else {
    		this->element.value.merge(p.value);
    	}
    }
public:
    KVS_PairLattice() : Lattice<version_value_pair<T>>() {}
    KVS_PairLattice(const version_value_pair<T> &p)  : Lattice<version_value_pair<T>>(p) {}
};