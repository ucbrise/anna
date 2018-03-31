#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h> 
#include "benchmark_KVS.h"

BENCHMARK(BM_KVSGET)->Arg(pow(10, 6));
BENCHMARK(BM_CKVSGET)->Arg(pow(10, 6))->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_KVSGETComparison)->Arg(pow(10, 6))->UseRealTime();
BENCHMARK(BM_KVSPUT)->Arg(pow(10, 6));
BENCHMARK(BM_CKVSPUT_LOWCONTENTION)->Arg(pow(10, 6))->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_CKVSPUT_HIGHCONTENTION)->Arg(pow(10, 6))->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_KVSPUTComparison)->Arg(pow(10, 6))->UseRealTime();

BENCHMARK_MAIN();