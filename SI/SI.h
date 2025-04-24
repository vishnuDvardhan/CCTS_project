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
#include <thread> // needed for sleep_for

std::mutex logMutex;
std::ofstream logFile;

std::atomic<long long> totalCommitTime{ 0 };
std::atomic<long long> totalCommitted{ 0 };
std::atomic<long long> totalAborts{ 0 };

struct Version {
    int value;
    int commit_ts;
};

class SnapshotIsolationManager {
private:
    std::atomic<int> nextTxID{ 1000 };
    std::atomic<int> globalTS{ 1 }; // unified timestamp counter
    std::mutex dataMutex;
    std::unordered_map<int, std::vector<Version>> versionChain;
    std::unordered_map<int, int> txStartTimestamps;
    std::unordered_map<int, std::unordered_map<int, int>> txWriteHistory;
    std::unordered_map<int, std::unordered_set<int>> txReadHistory;

public:
    SnapshotIsolationManager(int m) {
        for (int i = 0; i < m; ++i) {
            versionChain[i] = { {0, 0} };
        }
    }

    int beginTrans() {
        int txID = nextTxID.fetch_add(1);
        int start_ts = globalTS.fetch_add(1); // unified timestamp
        txStartTimestamps[txID] = start_ts;
        return txID;
    }

    int getStartTimestamp(int txID) {
        return txStartTimestamps[txID];
    }

    void readVal(int txID, int index, int& localVal, std::unordered_map<int, int>& localView) {
        txReadHistory[txID].insert(index);

        int start_ts = getStartTimestamp(txID);
        if (localView.find(index) != localView.end()) {
            localVal = localView[index];
            return;
        }

        std::lock_guard<std::mutex> lk(dataMutex);
        for (auto it = versionChain[index].rbegin(); it != versionChain[index].rend(); ++it) {
            if (it->commit_ts <= start_ts) {
                localVal = it->value;
                return;
            }
        }
        localVal = 0;
    }

    void writeVal(int txID, int index, int val, std::unordered_map<int, int>& localView) {
        localView[index] = val;
        txWriteHistory[txID][index] = val;
    }

    bool tryCommit(int txID, std::unordered_map<int, int>& localView) {
        int start_ts = getStartTimestamp(txID);

        std::lock_guard<std::mutex> lk(dataMutex);
        for (const auto& [index, _] : localView) {
            const auto& versions = versionChain[index];
            for (const auto& v : versions) {
                if (v.commit_ts > start_ts) {
                    return false;
                }
            }
        }

        int commit_ts = globalTS.fetch_add(1); // unified timestamp
        for (const auto& [index, value] : localView) {
            versionChain[index].push_back({ value, commit_ts });
        }

        return true;
    }
};