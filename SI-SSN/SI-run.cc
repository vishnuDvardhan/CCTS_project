#include <iostream>
#include <fstream>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <mutex>
#include <string>
#include <sstream>
#include "SI-SSN.h" // Your SnapshotIsolationSSNManager header

std::mutex logMutex;
std::ofstream logFile;

std::atomic<long long> totalCommitTime{ 0 };
std::atomic<long long> totalCommitted{ 0 };
std::atomic<long long> totalAborts{ 0 };

double readRatio = 0.7; // Default value; will overwrite from input if available

// ---------------- worker thread ---------------- //
void workerThread(int threadID, SnapshotIsolationManager* manager, int m, int numTrans, int numIters, int constVal, double lambda) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> distIndex(0, m - 1);
    std::uniform_int_distribution<int> distVal(0, constVal);
    std::exponential_distribution<double> distExp(1.0 / lambda);
    std::uniform_real_distribution<double> distProb(0.0, 1.0);

    for (int t = 0; t < numTrans; ++t) {
        int aborts = 0;
        auto start = std::chrono::steady_clock::now();

        while (true) {
            int txID = manager->beginTrans();
            bool readOnly = (distProb(rng) < readRatio);

            std::stringstream buffer;

            for (int i = 0; i < numIters; ++i) {
                int randInd = distIndex(rng);
                int localVal = manager->read(txID, randInd);

                buffer << "Thread " << threadID << " Tx " << txID
                    << " reads idx " << randInd << " val " << localVal
                    << " at time " << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count() << "\n";

                if (!readOnly) {
                    int randVal = distVal(rng);
                    localVal += randVal;
                    manager->write(txID, randInd, localVal);

                    buffer << "Thread " << threadID << " Tx " << txID
                        << " writes idx " << randInd << " val " << localVal
                        << " at time " << std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count() << "\n";
                }

                std::this_thread::sleep_for(std::chrono::milliseconds((int)distExp(rng)));
            }

            bool ok = manager->commit(txID);
            buffer << "Tx " << txID << " tryCommits => " << (ok ? "COMMIT" : "ABORT") << " at time "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count() << "\n";

            if (ok) {
                auto end = std::chrono::steady_clock::now();
                {
                    std::lock_guard<std::mutex> lk(logMutex);
                    logFile << buffer.str();
                }
                long long commitDelay = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                totalCommitTime.fetch_add(commitDelay);
                totalCommitted.fetch_add(1);
                totalAborts.fetch_add(aborts);
                break;
            }
            else {
                ++aborts;
            }
        }
    }
}

// ---------------- main function ---------------- //
int main() {
    std::ifstream fin("inp-params.txt");
    if (!fin.is_open()) {
        std::cerr << "Error: Could not open inp-params.txt\n";
        return 1;
    }

    int n, m, numTrans, constVal, numIters;
    double lambda;
    fin >> n >> m >> numTrans >> constVal >> numIters >> lambda >> readRatio;
    fin.close();

    std::cout << "n=" << n
        << " m=" << m
        << " numTrans=" << numTrans
        << " constVal=" << constVal
        << " numIters=" << numIters
        << " lambda=" << lambda
        << " readRatio=" << readRatio << "\n";

    logFile.open("si_log.txt");
    if (!logFile.is_open()) {
        std::cerr << "Error: Could not open si_log.txt\n";
        return 1;
    }

    // Add program start time measurement
    auto programStartTime = std::chrono::steady_clock::now();

    SnapshotIsolationManager manager(m);
    std::vector<std::thread> threads;
    threads.reserve(n);

    for (int i = 0; i < n; ++i) {
        threads.emplace_back(workerThread, i + 1, &manager, m, numTrans, numIters, constVal, lambda);
    }
    for (auto& th : threads) {
        th.join();
    }

    // Add program end time measurement
    auto programEndTime = std::chrono::steady_clock::now();
    double executionTimeSeconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        programEndTime - programStartTime).count() / 1000.0;

    long long committedCount = totalCommitted.load();
    long long abortedCount = totalAborts.load();
    double avgDelay = 0.0, avgAborts = 0.0;
    double commitsPerSecond = 0.0, abortsPerSecond = 0.0;

    if (committedCount > 0) {
        avgDelay = (double)totalCommitTime.load() / committedCount;
        avgAborts = (double)abortedCount / committedCount;
        commitsPerSecond = committedCount / executionTimeSeconds;
        abortsPerSecond = abortedCount / executionTimeSeconds;
    }

    logFile << "-----------------------------\n";
    logFile << "Average commit delay (ms): " << avgDelay << "\n";
    logFile << "Average abort count:       " << avgAborts << "\n";
    logFile << "Execution time (s):        " << executionTimeSeconds << "\n";
    logFile << "Commits per second:        " << commitsPerSecond << "\n";
    logFile << "Aborts per second:         " << abortsPerSecond << "\n";
    logFile.close();

    std::ofstream fout("si_result.txt");
    if (fout.is_open()) {
        fout << "Average commit delay (ms): " << avgDelay << "\n";
        fout << "Average abort count:       " << avgAborts << "\n";
        fout << "Execution time (s):        " << executionTimeSeconds << "\n";
        fout << "Commits per second:        " << commitsPerSecond << "\n";
        fout << "Aborts per second:         " << abortsPerSecond << "\n";
        fout.close();
    }

    return 0;
}