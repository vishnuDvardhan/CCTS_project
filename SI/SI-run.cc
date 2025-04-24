#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <thread>
#include "SI.h"

void workerThread(int threadID, SnapshotIsolationManager* manager, int m, int numTrans, int numIters, int constVal, double lambda) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> distIndex(0, m - 1);
    std::uniform_int_distribution<int> distVal(0, constVal);
    std::exponential_distribution<double> distExp(1.0 / lambda);

    for (int t = 0; t < numTrans; t++) {
        auto critStartTime = std::chrono::steady_clock::now();
        bool committed = false;
        int abortCount = 0;

        do {
            int txID = manager->beginTrans();
            std::unordered_map<int, int> localView;
            std::stringstream buffer;

            for (int i = 0; i < numIters; i++) {
                int randInd = distIndex(rng);
                int randVal = distVal(rng);

                int localVal = 0;
                manager->readVal(txID, randInd, localVal, localView);

                buffer << "Thread " << threadID << " Tx " << txID << " reads idx " << randInd << " val " << localVal << " at time "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                    .count() << "\n";

                localVal += randVal;
                manager->writeVal(txID, randInd, localVal, localView);

                buffer << "Thread " << threadID << " Tx " << txID << " writes idx " << randInd << " val " << localVal << " at time "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                    .count() << "\n";

                std::this_thread::sleep_for(std::chrono::milliseconds((int)distExp(rng)));
            }

            bool ok = manager->tryCommit(txID, localView);

            buffer << "Tx " << txID << " tryCommits => " << (ok ? "COMMIT" : "ABORT") << " at time "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                .count() << "\n";

            {
                std::lock_guard<std::mutex> lk(logMutex);
                logFile << buffer.str();
            }

            if (ok) committed = true;
            else abortCount++;

        } while (!committed);

        auto critEndTime = std::chrono::steady_clock::now();
        long long commitDelay = std::chrono::duration_cast<std::chrono::milliseconds>(critEndTime - critStartTime).count();
        totalCommitTime.fetch_add(commitDelay);
        totalCommitted.fetch_add(1);
        totalAborts.fetch_add(abortCount);
    }
}

int main() {
    std::ifstream fin("inp-params.txt");
    if (!fin.is_open()) {
        std::cerr << "Error: Could not open inp-params.txt\n";
        return 1;
    }

    int n, m, numTrans, constVal, numIters;
    double lambda;
    fin >> n >> m >> numTrans >> constVal >> numIters >> lambda;
    fin.close();

    std::cout << "n: " << n << ", m: " << m << ", numTrans: " << numTrans
        << ", constVal: " << constVal << ", numIters: " << numIters
        << ", lambda: " << lambda << "\n";

    logFile.open("si_log.txt");
    if (!logFile.is_open()) {
        std::cerr << "Error: Could not open si_log.txt\n";
        return 1;
    }

    SnapshotIsolationManager manager(m);
    std::vector<std::thread> threads;
    threads.reserve(n);
    for (int i = 0; i < n; i++) {
        threads.emplace_back(workerThread, i + 1, &manager, m, numTrans, numIters, constVal, lambda);
    }
    for (auto& th : threads) {
        th.join();
    }

    long long committedCount = totalCommitted.load();
    double avgDelay = 0.0, avgAborts = 0.0;
    if (committedCount > 0) {
        avgDelay = (double)totalCommitTime.load() / committedCount;
        avgAborts = (double)totalAborts.load() / committedCount;
    }

    logFile << "-----------------------------\n";
    logFile << "Average commit delay (ms): " << avgDelay << "\n";
    logFile << "Average abort count:       " << avgAborts << "\n";
    logFile.close();

    std::ofstream fout("si_result.txt");
    if (fout.is_open()) {
        fout << "Average commit delay (ms): " << avgDelay << "\n";
        fout << "Average abort count:       " << avgAborts << "\n";
        fout.close();
    }

    return 0;
}