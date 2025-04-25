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

std::mutex logMutex;
std::ofstream logFile;

std::atomic<long long> totalCommitTime{ 0 };
std::atomic<long long> totalCommitted{ 0 };
std::atomic<long long> totalAborts{ 0 };

class SnapshotIsolationManager {
private:
    std::atomic<int> nextTxID{ 1000 };
    std::mutex dataMutex;
    std::vector<std::unordered_map<int, int>> committedVersions;

public:
    SnapshotIsolationManager(int m) : committedVersions(1) {
        committedVersions[0].reserve(m);
        for (int i = 0; i < m; ++i) committedVersions[0][i] = 0;
    }

    int beginTrans() {
        return nextTxID.fetch_add(1);
    }

    void readVal(int txID, int index, int& localVal, std::unordered_map<int, int>& localView) {
        if (localView.find(index) != localView.end()) {
            localVal = localView[index];
            return;
        }

        std::lock_guard<std::mutex> lk(dataMutex);
        localVal = committedVersions.back()[index];
    }

    void writeVal(int txID, int index, int val, std::unordered_map<int, int>& localView) {
        localView[index] = val;
    }

    bool tryCommit(int txID, std::unordered_map<int, int>& localView) {
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(0, 3);
        bool commitDecision = (dist(rng) != 0);

        if (!commitDecision) return false;

        std::lock_guard<std::mutex> lk(dataMutex);
        auto newVersion = committedVersions.back();
        for (const auto& [idx, val] : localView) {
            newVersion[idx] = val;
        }
        committedVersions.push_back(std::move(newVersion));
        return true;
    }
};

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

            for (int i = 0; i < numIters; i++) {
                int randInd = distIndex(rng);
                int randVal = distVal(rng);

                int localVal = 0;
                manager->readVal(txID, randInd, localVal, localView);

                {
                    std::lock_guard<std::mutex> lk(logMutex);
                    logFile << "Thread " << threadID
                        << " Tx " << txID
                        << " reads idx " << randInd
                        << " val " << localVal
                        << " at time "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count()
                        << "\n";
                }

                localVal += randVal;
                manager->writeVal(txID, randInd, localVal, localView);

                {
                    std::lock_guard<std::mutex> lk(logMutex);
                    logFile << "Thread " << threadID
                        << " Tx " << txID
                        << " writes idx " << randInd
                        << " val " << localVal
                        << " at time "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count()
                        << "\n";
                }

                double sleep_ms = distExp(rng);
                std::this_thread::sleep_for(std::chrono::milliseconds((int)sleep_ms));
            }

            bool ok = manager->tryCommit(txID, localView);

            {
                std::lock_guard<std::mutex> lk(logMutex);
                logFile << "Tx " << txID
                    << " tryCommits => " << (ok ? "COMMIT" : "ABORT")
                    << " at time "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count()
                    << "\n";
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
    std::cout << "n: " << n << ", m: " << m << ", numTrans: " << numTrans
        << ", constVal: " << constVal << ", numIters: " << numIters
        << ", lambda: " << lambda << "\n";
    fin.close();

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
