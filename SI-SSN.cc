// snapshot_isolation_with_ssn.cpp
#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <chrono>
#include <random>
#include <atomic>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <limits>

using namespace std;
using namespace chrono;

struct Version {
    int timestamp;
    int value;
    int pstamp = 0;  // Latest commit time of any reader (initialized to 0)
    int sstamp = numeric_limits<int>::max();  // Earliest commit time of overwriter
};

class DataItem {
public:
    vector<Version> versions;
    mutex mtx;

    int getLatestValueBefore(int ts, Version& outVersion) {
        lock_guard<mutex> lock(mtx);
        for (int i = versions.size() - 1; i >= 0; --i) {
            if (versions[i].timestamp <= ts) {
                outVersion = versions[i];
                return versions[i].value;
            }
        }
        return -1;
    }

    void appendVersion(Version v) {
        lock_guard<mutex> lock(mtx);
        versions.push_back(v);
    }

    Version* getVersionBefore(int ts) {
        lock_guard<mutex> lock(mtx);
        for (int i = versions.size() - 1; i >= 0; --i) {
            if (versions[i].timestamp <= ts) {
                return &versions[i];
            }
        }
        return nullptr;
    }
};

struct Transaction {
    int id;
    int start_ts;
    int cstamp;
    int pstamp = 0;  // initialized to 0 instead of -1
    int sstamp = numeric_limits<int>::max();
    unordered_map<int, int> localWrites;
    vector<pair<int, Version>> readSet;
};

class SSNManager {
public:
    atomic<int> global_ts{ 0 };
    atomic<int> next_tid{ 0 };
    vector<DataItem> database;

    SSNManager(int num_vars) : database(num_vars) {}

    Transaction begin_trans() {
        int ts = global_ts++;
        return Transaction{ next_tid++, ts };
    }

    bool read(Transaction& T, int x, int& l) {
        if (T.localWrites.find(x) != T.localWrites.end()) {
            l = T.localWrites[x];
            return true;
        }
        Version v;
        l = database[x].getLatestValueBefore(T.start_ts, v);
        T.pstamp = max(T.pstamp, v.timestamp);
        T.sstamp = min(T.sstamp, v.sstamp);
        T.readSet.emplace_back(x, v);
        return true;
    }

    void write(Transaction& T, int x, int l) {
        T.localWrites[x] = l;
    }

    char try_commit(Transaction& T) {
        T.cstamp = global_ts++;

        for (auto& [x, val] : T.localWrites) {
            Version* prev = database[x].getVersionBefore(T.start_ts);
            if (prev) T.pstamp = max(T.pstamp, prev->pstamp);
        }

        T.sstamp = min(T.sstamp, T.cstamp);
        for (auto& [x, v] : T.readSet) {
            T.sstamp = min(T.sstamp, v.sstamp);
        }

        if (T.sstamp <= T.pstamp) return 'a';

        for (auto& [x, v] : T.readSet) {
            Version* version = database[x].getVersionBefore(T.start_ts);
            if (version) version->pstamp = max(version->pstamp, T.cstamp);
        }

        for (auto& [x, val] : T.localWrites) {
            Version* prev = database[x].getVersionBefore(T.start_ts);
            if (prev) prev->sstamp = min(prev->sstamp, T.sstamp);

            Version newVersion;
            newVersion.timestamp = T.cstamp;
            newVersion.value = val;
            newVersion.pstamp = T.cstamp;
            newVersion.sstamp = numeric_limits<int>::max();

            database[x].appendVersion(newVersion);
        }

        return 'c';
    }
};


// === Threaded Simulation and Metrics ===
ofstream logFile("si_output.txt");
ofstream resultFile("si_result.txt");
mutex resultMutex;
int totalAborts = 0;
long long totalCommitDelay = 0;

int getRand(int maxVal) {
    static thread_local mt19937 gen(random_device{}());
    uniform_int_distribution<int> dist(0, maxVal - 1);
    return dist(gen);
}

float getFloatRand() {
    static thread_local mt19937 gen(random_device{}());
    uniform_real_distribution<float> dist(0.0, 1.0);
    return dist(gen);
}

int getExpRand(int lambda_ms) {
    static thread_local mt19937 gen(random_device{}());
    exponential_distribution<float> dist(1.0 / lambda_ms);
    return static_cast<int>(dist(gen));
}

string getSysTime() {
    auto now = system_clock::now();
    auto t_c = system_clock::to_time_t(now);
    ostringstream oss;
    tm* timeinfo = localtime(&t_c);
    oss << setw(2) << setfill('0') << timeinfo->tm_hour << ":"
        << setw(2) << setfill('0') << timeinfo->tm_min << ":"
        << setw(2) << setfill('0') << timeinfo->tm_sec;
    return oss.str();
}

void updtMem(SSNManager& sim, int tid, int m, int constVal, int lambda, float readRatio) {
    char status = 'a';
    int abortCnt = 0;
    bool readOnly = getFloatRand() < readRatio;
    auto critStart = high_resolution_clock::now();

    do {
        Transaction T = sim.begin_trans();
        int randIters = getRand(m) + 1;

        for (int i = 0; i < randIters; ++i) {
            int randInd = getRand(m);
            int randVal = getRand(constVal);
            int localVal;

            sim.read(T, randInd, localVal);
            logFile << "Thread " << tid << " Transaction " << T.id << " reads " << randInd << " a value " << localVal << " at time " << getSysTime() << endl;

            if (!readOnly) {
                localVal += randVal;
                sim.write(T, randInd, localVal);
                logFile << "Thread " << tid << " Transaction " << T.id << " writes to " << randInd << " a value " << localVal << " at time " << getSysTime() << endl;
            }

            this_thread::sleep_for(milliseconds(getExpRand(lambda)));
        }

        status = sim.try_commit(T);
        logFile << "Transaction " << T.id << " tryCommits with result " << (status == 'c' ? "commit" : "abort") << " at time " << getSysTime() << endl;
        if (status == 'a') abortCnt++;
    } while (status != 'c');

    auto critEnd = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(critEnd - critStart).count();

    logFile << "Thread " << tid << " Transaction finished with commitDelay = " << duration << " ms and aborts = " << abortCnt << endl;

    lock_guard<mutex> lock(resultMutex);
    totalAborts += abortCnt;
    totalCommitDelay += duration;
}

int main() {
    ifstream infile("inp-params.txt");
    int n, m, constVal, lambda;
    float readRatio;
    infile >> n >> m >> constVal >> lambda >> readRatio;

    SSNManager sim(m);
    vector<thread> threads;

    for (int i = 0; i < n; ++i) {
        threads.emplace_back(updtMem, ref(sim), i, m, constVal, lambda, readRatio);
    }

    for (auto& t : threads) t.join();

    resultFile << "Total Aborts: " << totalAborts << endl;
    resultFile << "Average Aborts per Transaction: " << (double)totalAborts / n << endl;
    resultFile << "Average Commit Delay (ms): " << (double)totalCommitDelay / n << endl;

    return 0;
}
