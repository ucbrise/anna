#ifndef CORE_LATTICES_H
#define CORE_LATTICES_H

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <limits>
#include <cassert>
#include "base_lattices.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/concurrent_unordered_map.h"

using namespace std;


// go through bloomL paper and mark which function is monotone/morphism
// find a way in c++ to make the compiler check

class BoolLattice : public Lattice<bool> {
protected:
	void do_merge(const bool &e) {
		element |= e;
	}
public:
	BoolLattice() : Lattice() {}
	BoolLattice(const bool &e) : Lattice(e) {}
	// this should probably be defined by the application
	// just make it an if-top test and put it in the lattice superclass
	const int when_true(const int (*f)()) const{
		if (element) {
			return (*f)();
		}
		else return 0;
	}
};



template <typename T>
class MaxLattice : public Lattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition

		// T::operator< has to be properly overloaded
		int current = this->element;
		if (current < e) {
			this->element = e;
		}
	}
public:
	MaxLattice() : Lattice<T>() {}
	MaxLattice(const T &e) : Lattice<T>(e) {}
	// overload with operator>
	BoolLattice gt(T n) const{
		if (this->element > n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	// overload with operator>=
	BoolLattice gt_eq(T n) const{
		if (this->element >= n) return BoolLattice(true);
		else return BoolLattice(false);
	}

	// non-monotone mehtod should appear outside the initial class definition, fix throughout
	// wrap with namespace? c++ custom type modifier
	// for now, all non-merge methods are non-destructive
	MaxLattice<T> add(T n) const{
		return MaxLattice<T>(this->element + n);
	}
	MaxLattice<T> subtract(T n) const{
		return MaxLattice<T>(this->element - n);
	}
};



template <typename T>
class MinLattice : public Lattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		int current = this->element;
		if (current > e) {
			this->element = e;
		}
	}
public:
	MinLattice() {
		//this->assign(numeric_limits<T>::max());
		this->assign(static_cast<T> (1000000));
	}
	MinLattice(const T &e) : Lattice<T>(e) {}
	const T bot() const {
		//return numeric_limits<T>::max();
		return static_cast<T> (1000000);
	}
	// overload with operator>
	BoolLattice lt(T n) const{
		if (this->element < n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	// overload with operator>=
	// put it in the superclass and reimplement for efficiency
	BoolLattice lt_eq(T n) const{
		if (this->element <= n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	MinLattice<T> add(T n) const{
		return MinLattice<T>(this->element + n);
	}
	MinLattice<T> subtract(T n) const{
		return MinLattice<T>(this->element - n);
	}
};



template <typename T>
class SetLattice : public Lattice<unordered_set<T>> {
protected:
	void do_merge(const unordered_set<T> &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition

		// is there a more efficient bulk merge for c++ set --no
		// maybe worth doing sort-merge std::sort followed by std::set_union (benchmark)
		// for larger set, implement join algorithm
		for ( auto it = e.begin(); it != e.end(); ++it ) {
			this->element.insert(*it);
		}
	}
public:
	SetLattice() : Lattice<unordered_set<T>>() {}
	SetLattice(const unordered_set<T> &e) : Lattice<unordered_set<T>>(e) {}
	MaxLattice<int> size() const{
		return MaxLattice<int>(this->element.size());
	}
	void insert(const T &e) {
		this->element.insert(e);
	}
	SetLattice<T> intersect(unordered_set<T> s) const{
		unordered_set<T> res;
		for ( auto iter_i = s.begin(); iter_i != s.end(); ++iter_i ) {
			for ( auto iter_j = this->element.begin(); iter_j != this->element.end(); ++iter_j ) {
				if (*iter_i == *iter_j) res.insert(*iter_i);
			}
		}
		return SetLattice<T>(res);
	}
	// rename it "map"
	// this should be outside the library
	SetLattice<T> project(bool (*f)(T)) const{
		unordered_set<T> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(*it)) res.insert(*it);
        }
        return SetLattice<T>(res);
	}
	BoolLattice contain(T v) const{
		auto it = this->element.find(v);
		if (it == this->element.end()) return BoolLattice(false);
		else return BoolLattice(true);
	}
};



template <typename K, typename V>
class MapLattice : public Lattice<unordered_map<K, V>> {
protected:
	// can insert_pair be private?
 	void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            // avoid copying the value out of the pair during casting!  Instead
            // cast the pointer. A bit ugly but seems like it should be safe.
            // WHY NOT search->second.merge(v)??
            // or at least
            // (static_cast<V>(search->second)).merge(v);
            static_cast<V *>(&(search->second))->merge(v);
        } else {
            // need to copy v since we will be "growing" it within the lattice
            V new_v = v;
            this->element.emplace(k, new_v);
        }
    }
    void do_merge(const unordered_map<K, V> &m) {
        for (auto ms = m.begin(); ms != m.end(); ++ms) {
            this->insert_pair(ms->first, ms->second);
        }
    }
public:
	MapLattice() : Lattice<unordered_map<K, V>>() {}
	MapLattice(const unordered_map<K, V> &m) : Lattice<unordered_map<K, V>>(m) {}
	MaxLattice<int> size() const{
		return this->element.size();
	}
	// sort and set_intersect
	// how c++ deal with map intersect
	MapLattice<K, V> intersect(MapLattice<K, V> other) const{
		MapLattice<K, V> res;
		unordered_map<K, V> m = other.reveal();
        for (auto it = m.begin(); it != this->m.end(); ++it) {
            if(this->contain(it->first).reveal()) {
            	res.insert_pair(it->first, this->at(it->first));
            	res.insert_pair(it->first, it->second);
            }
        }
        return res;
	}
	// move it out
	MapLattice<K, V> project(bool (*f)(V)) const{
		unordered_map<K, V> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(it->second)) res.emplace(it->first, it->second);
        }
        return MapLattice<K, V>(res);
	}
	// use c++ built-in function
	SetLattice<K> key_set() const{
		unordered_set<K> res;
		for ( auto it = this->element.begin(); it != this->element.end(); ++it) {
			res.insert(it->first);
		}
		return SetLattice<K>(res);
	}
	V &at(K k) {
		return this->element[k];
	}
	BoolLattice contain(K k) const{
		auto it = this->element.find(k);
		if (it == this->element.end()) return BoolLattice(false);
		else return BoolLattice(true);
	}
	void remove(K k) {
		auto it = this->element.find(k);
		if (it != this->element.end()) this->element.erase(k);
	}
};



template <typename V>
class VectorLattice : public Lattice<vector<V>> {
protected:
    void do_merge(const vector<V> &v) {
        bool self_done = false;
        for (int i = 0; i < v.size(); i++) {
            if (!self_done && i == this->element.size()) self_done = true;
            if (!self_done) {
                // merge by position
                this->element[i].merge(v[i]);
            } else {
                // append additional input positions
                this->element.push_back(v[i]);
            }
        }
    }
public:
    VectorLattice() : Lattice<vector<V>>() {}
    VectorLattice(const vector<V> &v) : Lattice<vector<V>>(v) {}
};



// assume that once a value has been deleted, it cannot be re-inserted
template <typename T>
class TombstoneLattice : public MapLattice<T, BoolLattice> {
public:
    TombstoneLattice() : MapLattice<T, BoolLattice>() {}
    TombstoneLattice(const unordered_map<T, BoolLattice> &m)  : MapLattice<T, BoolLattice>(m) {}
    void insert(const T &e) {
		this->insert_pair(e, BoolLattice(false));
	}
	void remove(const T &e) {
		this->insert_pair(e, BoolLattice(true));
	}
	// non-monotone, put it outside the class
	// this is very inefficient for lots of dead elements inside the map (fix later with 2 maps?)
	SetLattice<T> living_elements() const{
		unordered_set<T> res;
		for ( auto it = this->element.begin(); it != this->element.end(); ++it) {
			if (!it->second.reveal()) res.insert(it->first);
		}
		return SetLattice<T>(res);
	}
};



// is there no built-in for this?
template <typename T, size_t S>
struct slotArray {
	T slots[S];
	size_t size;
	slotArray<T, S>() {
		size = S;
	}
	// not sure why this constructor is called instead
	slotArray<T, S>(int a) {
		size = S;
	}
};

template <typename T, size_t S>
class ArrayLattice : public Lattice<slotArray<T, S>> {
protected:
    void do_merge(const slotArray<T, S> &e) {
    	// assume for now that size(e) is smaller, so no resize
    	// make sure c++ detects the case when size is not the same (unit test this)
    	for (int i = 0; i < e.size; i++) {
    		insert(e.slots[i], i);
    	}
    }
public:
	ArrayLattice() : Lattice<slotArray<T, S>>() {}
	ArrayLattice(const slotArray<T, S> &sa) : Lattice<slotArray<T, S>>(sa) {}
	int insert(const T &e, int index) {
		if (index >= S) return -1;
		else {
			T * ptr = &(this->element.slots[index]);
			ptr->merge(e);
			return 0;
		}
	}
};



class AtomicBoolLattice: public AtomicLattice<bool> {
protected:
	void do_merge(const bool &e) {
		if (e == true && element.load() == false) element.store(e);
	}
public:
	AtomicBoolLattice() : AtomicLattice<bool>() {}
	AtomicBoolLattice(const bool &e) : AtomicLattice<bool>(e) {}
	const int when_true(const int (*f)()) const{
		if (element) {
			return (*f)();
		}
		else return 0;
	}
};



template <typename T>
class AtomicMaxLattice : public AtomicLattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		T current = this->element.load();
		if (current < e) {
			while(!this->element.compare_exchange_strong(current, e)){
				if(current >= e) break;
			}
		}
	}
public:
	AtomicMaxLattice() : AtomicLattice<T>() {}
	AtomicMaxLattice(const T &e) : AtomicLattice<T>(e) {}
	BoolLattice gt(T n) const{
		if (this->element > n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	BoolLattice gt_eq(T n) const{
		if (this->element >= n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	MaxLattice<T> add(T n) const{
		return MaxLattice<T>(this->element + n);
	}
	MaxLattice<T> subtract(T n) const{
		return MaxLattice<T>(this->element - n);
	}
};



template <typename T>
class AtomicMinLattice : public AtomicLattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		T current = this->element.load();
		if (current > e) {
			while(!this->element.compare_exchange_strong(current, e)){
				if(current <= e) break;
			}
		}
	}
public:
	AtomicMinLattice() {
		this->assign(static_cast<T> (1000000));
	}
	AtomicMinLattice(const T &e) : AtomicLattice<T>(e) {}
	BoolLattice lt(T n) const{
		if (this->element < n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	BoolLattice lt_eq(T n) const{
		if (this->element <= n) return BoolLattice(true);
		else return BoolLattice(false);
	}
	MinLattice<T> add(T n) const{
		return MinLattice<T>(this->element + n);
	}
	MinLattice<T> subtract(T n) const{
		return MinLattice<T>(this->element - n);
	}
};



template <typename T>
class AtomicSetLattice : public Lattice<tbb::concurrent_unordered_set<T>> {
protected:
	void do_merge(const tbb::concurrent_unordered_set<T> &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		for ( auto it = e.begin(); it != e.end(); ++it ) {
			this->element.insert(*it);
		}
	}
public:
	AtomicSetLattice() : Lattice<tbb::concurrent_unordered_set<T>>() {}
	AtomicSetLattice(const tbb::concurrent_unordered_set<T> &e) : Lattice<tbb::concurrent_unordered_set<T>>(e) {}
	MaxLattice<int> size() const{
		return MaxLattice<int>(this->element.size());
	}
	void insert(const T &e) {
		this->element.insert(e);
	}
	SetLattice<T> intersect(unordered_set<T> s) const{
		unordered_set<T> res;
		for ( auto iter_i = s.begin(); iter_i != s.end(); ++iter_i ) {
			for ( auto iter_j = this->element.begin(); iter_j != this->element.end(); ++iter_j ) {
				if (*iter_i == *iter_j) res.insert(*iter_i);
			}
		}
		return SetLattice<T>(res);
	}
	SetLattice<T> project(bool (*f)(T)) const{
		unordered_set<T> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(*it)) res.insert(*it);
        }
        return SetLattice<T>(res);
	}
	BoolLattice contain(T v) const{
		auto it = this->element.find(v);
		if (it == this->element.end()) return BoolLattice(false);
		else return BoolLattice(true);
	}
};



template <typename K, typename V>
class AtomicMapLattice : public Lattice<tbb::concurrent_unordered_map<K, V>> {
protected:
 	void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            // avoid copying the value out of the pair during casting!  Instead
            // cast the pointer. A bit ugly but seems like it should be safe.
            static_cast<V *>(&(search->second))->merge(v);
        } else {
            // need to copy v since we will be "growing" it within the lattice
            V new_v = v;
            // FIXME: it seems that there is a bug in tbb that fails to enable c++11 features with Clang on Linux. So we have to use insert instead of emplace for now...
            // refer to https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/591305
            this->element.insert({k, new_v});
        }
    }
    void do_merge(const tbb::concurrent_unordered_map<K, V> &m) {
        for (auto ms = m.begin(); ms != m.end(); ++ms) {
            this->insert_pair(ms->first, ms->second);
        }
    }
public:
	AtomicMapLattice() : Lattice<tbb::concurrent_unordered_map<K, V>>() {}
	AtomicMapLattice(const tbb::concurrent_unordered_map<K, V> &m) : Lattice<tbb::concurrent_unordered_map<K, V>>(m) {}
	MaxLattice<int> size() const{
		return this->element.size();
	}
	MapLattice<K, V> intersect(MapLattice<K, V> other) const{
		MapLattice<K, V> res;
		unordered_map<K, V> m = other.reveal();
        for (auto it = m.begin(); it != this->m.end(); ++it) {
            if(this->contain(it->first).reveal()) {
            	res.insert_pair(it->first, this->at(it->first));
            	res.insert_pair(it->first, it->second);
            }
        }
        return res;
	}
	MapLattice<K, V> project(bool (*f)(V)) const{
		unordered_map<K, V> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(it->second)) res.emplace(it->first, it->second);
        }
        return MapLattice<K, V>(res);
	}
	SetLattice<K> key_set() const{
		unordered_set<K> res;
		for ( auto it = this->element.begin(); it != this->element.end(); ++it) {
			res.insert(it->first);
		}
		return SetLattice<K>(res);
	}
	V &at(K k) {
		return this->element[k];
	}
	BoolLattice contain(K k) const{
		auto it = this->element.find(k);
		if (it == this->element.end()) return BoolLattice(false);
		else return BoolLattice(true);
	}
};



// assume that once a value has been deleted, it cannot be re-inserted
template <typename T>
class AtomicTombstoneLattice : public AtomicMapLattice<T, BoolLattice> {
public:
    AtomicTombstoneLattice() : AtomicMapLattice<T, BoolLattice>() {}
    AtomicTombstoneLattice(const tbb::concurrent_unordered_map<T, BoolLattice> &m)  : AtomicMapLattice<T, BoolLattice>(m) {}
    void insert(const T &e) {
		this->insert_pair(e, BoolLattice(false));
	}
	void remove(const T &e) {
		this->insert_pair(e, BoolLattice(true));
	}
	SetLattice<T> living_elements() const{
		unordered_set<T> res;
		for ( auto it = this->element.begin(); it != this->element.end(); ++it) {
			if (!it->second.reveal()) res.insert(it->first);
		}
		return SetLattice<T>(res);
	}
};







#endif