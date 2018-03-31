# Anna: A KVS for any scale

Anna is an in-memory KVS that delivers high performance at multiple scales, from a single core machine to NUMA to geo-distributed deployment. It also provides a wide spectrum of coordination-free isolation levels that aim to meet the consistency requirements of different applications.

## Build Instruction:

1. Install dependencies, Clang and libc++.
On Ubuntu, run:<br />
`sudo apt-get update`.<br />
`sudo apt-get install -y build-essential autoconf automake libtool curl make g++ unzip pkg-config wget clang-3.9`.<br />
`sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 1`.<br />
`sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-3.9 1`.<br />
`sudo apt-get install -y libc++-dev libc++abi-dev libtbb-dev`.<br />

2. Install cmake
`wget https://cmake.org/files/v3.9/cmake-3.9.4-Linux-x86_64.tar.gz`.<br />
`tar xvzf cmake-3.9.4-Linux-x86_64.tar.gz`.<br />
`mv cmake-3.9.4-Linux-x86_64 /usr/bin/cmake`.<br />
`export PATH=$PATH:/usr/bin/cmake/bin`.<br />
`rm cmake-3.9.4-Linux-x86_64.tar.gz`.<br />

3. Install protobuf: after cloning the protobuf repo, run
`./autogen.sh`.<br />
`./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'`.<br />
`make`.<br />
`make check`.<br />
`sudo make install`.<br />
`ldconfig`.<br />

4. Build KVS
`bash scripts/build_release.sh`.<br />

(This command will build a KVS that provides last-writer-win consistency. Lattice composition of other consistency levels can be found in `./kvs/include`)

## IP Configuration:
For each server node:
1. The ip of the current node should be stored in `conf/server/server_ip.txt`.
2. The ip of the seed node should be stored in `conf/server/seed_server.txt`. The seed node can be any proxy node.
3. The ip of all the proxy nodes should be stored in `conf/server/proxy_address.txt`. Each line contains a single proxy ip.

For each proxy node:
1. The ip of the current node should be stored in `conf/proxy/proxy_ip.txt`.

For each user/benchmark node:
1. The ip of the current node should be stored in `conf/user/user_ip.txt`.
2. The ip of all the proxy nodes should be stored in `conf/user/proxy_address.txt`. Each line contains a single proxy ip.

## Run Instruction:

1. Start a server by running `./build/kvs/kvs_server`.
2. Start a proxy by running `./build/kvs/kvs_proxy`.
3. Start a client by running `./build/kvs/kvs_user`.

The accepted input formats are `GET $key` and `PUT $key $value`.