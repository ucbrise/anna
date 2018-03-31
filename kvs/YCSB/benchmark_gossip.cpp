#include <zmq.hpp>
#include <string>
#include <iostream>
#include <pthread.h>
#include <unistd.h>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include "versioned_kv_store.h"

using namespace std;

// Define the number of threads
#define THREAD_NUM 4

// If the total number of updates to the kvs before the last gossip reaches THRESHOLD, then the thread gossips to others.
#define THRESHOLD 5

// Define the number of request to be processed by each thread.
#define REQUEST_NUM 1000000

// For simplicity, the kvs uses integer as the key type and maxintlattice as the value lattice.
typedef KV_Store<int, KVS_PairLattice<MaxLattice<int>>> Database;

atomic<int> bind_successful {0};
atomic<int> finished_setup {0};

struct put_request{
    int key;
    version_value_pair<MaxLattice<int>> v_pair;
};

struct coordination_data{
    unordered_map<int, KVS_PairLattice<MaxLattice<int>>> data;
    atomic<int> processed {0};
};

// Act as an event loop for the server
void *worker_routine (zmq::context_t *context, int thread_id, put_request *request)
{
    // initialize the thread's kvs replica
    unique_ptr<Database> kvs(new Database);

    // initializa a set lattice that keep track of the keys that get updated
    unique_ptr<SetLattice<int>> change_set(new SetLattice<int>);

    zmq::socket_t publisher (*context, ZMQ_PUB);
    publisher.bind("inproc://" + to_string(thread_id));
    bind_successful++;

    zmq::socket_t subscriber (*context, ZMQ_SUB);

    while (bind_successful.load() != THREAD_NUM) {}

    for (int i = 0; i < THREAD_NUM; i++) {
        if (i != thread_id) {
            subscriber.connect("inproc://" + to_string(i));
        }
    }
    const char *filter = "";
    subscriber.setsockopt(ZMQ_SUBSCRIBE, filter, strlen (filter));

    // A counter that keep track of the number of updates performed to the kvs before the last gossip
    int update_counter = 0;
    int rc;

    finished_setup++;
    while (finished_setup.load() != THREAD_NUM) {}

    for (int i = 0; i < REQUEST_NUM; i++) {
        //if (thread_id == 0) cout << "processing request " << i << "\n";
        kvs->put(request[i].key, request[i].v_pair);
        change_set->insert(request[i].key);
        update_counter++;
        if (update_counter == THRESHOLD) {
            coordination_data *c_data = new coordination_data;
            for (auto it = change_set->reveal().begin(); it != change_set->reveal().end(); it++) {
                c_data->data.emplace(*it, kvs->get(*it));
            }

            zmq_msg_t msg;
            zmq_msg_init_size(&msg, sizeof(coordination_data**));
            memcpy(zmq_msg_data(&msg), &c_data, sizeof(coordination_data**));
            zmq_msg_send(&msg, static_cast<void *>(publisher), 0);

            // Reset the change_set and update_counter
            change_set.reset(new SetLattice<int>);
            update_counter = 0;
            //cout << "The gossip is sent by thread " << thread_id << "\n";
        }
        zmq_msg_t rec;
        zmq_msg_init(&rec);
        if ((rc = zmq_msg_recv(&rec, static_cast<void *>(subscriber), ZMQ_DONTWAIT)) != -1) {
            coordination_data *c_data = *(coordination_data **)zmq_msg_data(&rec);
            zmq_msg_close(&rec);
            //cout << "The gossip is received by thread " << thread_id << "\n";
            // merge delta from other threads
            for (auto it = c_data->data.begin(); it != c_data->data.end(); it++) {
                kvs->put(it->first, it->second);
            }

            if (c_data->processed.fetch_add(1) == THREAD_NUM - 1 - 1) {
                delete c_data;
                //cout << "The gossip is successfully garbage collected by thread " << thread_id << "\n";
            }
        }
    }
    return (NULL);
}

int main ()
{
    //  Prepare our context
    zmq::context_t context (1);

    //  Launch pool of worker threads
    cout << "Starting the server with " << THREAD_NUM << " threads and gossip threshold " << THRESHOLD << "\n";

    cout << "Start generating synthetic put request\n";
    put_request **request = new put_request*[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; i++) {
        request[i] = new put_request[REQUEST_NUM];
    }
    for (int i = 0; i < THREAD_NUM; i++) {
        for (int j = 0; j < REQUEST_NUM; j++) {
            request[i][j].key = 0;
            request[i][j].v_pair = version_value_pair<MaxLattice<int>>(MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{i, MaxLattice<int>(j)}})), 10);
        }
    }
    cout << "Finish generating synthetic put request\n";

    vector<thread> threads;
    for (int thread_id = 0; thread_id != THREAD_NUM; thread_id++) {
        threads.push_back(thread(worker_routine, &context, thread_id, request[thread_id]));
    }

    chrono::steady_clock::time_point c_begin = std::chrono::steady_clock::now();

    for (auto& th: threads) th.join();

    std::chrono::steady_clock::time_point c_end= std::chrono::steady_clock::now();
    cout << "Elapsed time is " << chrono::duration_cast<std::chrono::microseconds>(c_end - c_begin).count() << " microseconds\n";

    return 0;
}