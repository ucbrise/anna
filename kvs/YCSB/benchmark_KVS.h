#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "versioned_kv_store.h"
#include "benchmark/benchmark.h"

KV_Store<int, KVS_PairLattice<MaxLattice<int>>> kvs;
Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>> ckvs;

static void BM_KVSGET(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
	}
	KVS_PairLattice<MaxLattice<int>> pl;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			pl = kvs.get(i);
		}
	}
	kvs = KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSGET(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(i, p);
	}
	KVS_PairLattice<MaxLattice<int>> pl;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			pl = ckvs.get(i);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_KVSGETComparison(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> q = KVS_PairLattice<MaxLattice<int>>(p);
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			q;
		}
	}
}

static void BM_KVSPUT(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
	}
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(2)}, {2, MaxLattice<int>(2)}, {3, MaxLattice<int>(2)}, {4, MaxLattice<int>(2)}, {5, MaxLattice<int>(2)}}));

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
		}
	}
	kvs = KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSPUT_LOWCONTENTION(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	if (state.thread_index == 0) {
		for (int i = 0; i < state.range_x(); i++) {
			ckvs.put(i, p);
		}
	}
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(2)}, {2, MaxLattice<int>(2)}, {3, MaxLattice<int>(2)}, {4, MaxLattice<int>(2)}, {5, MaxLattice<int>(2)}}));

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(i, p);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSPUT_HIGHCONTENTION(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	if (state.thread_index == 0) ckvs.put(0, p);
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(2)}, {2, MaxLattice<int>(2)}, {3, MaxLattice<int>(2)}, {4, MaxLattice<int>(2)}, {5, MaxLattice<int>(2)}}));

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(0, p);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_KVSPUTComparison(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}, {3, MaxLattice<int>(1)}, {4, MaxLattice<int>(1)}, {5, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> res;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			res = p;
		}
	}
}