#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <atomic>
#include <thread>

struct Version {
    int value;
    int commit_ts;
};

class SnapshotIsolationManager {
private:
    std::atomic<int> nextTxID{ 1000 };
    std::atomic<int> globalTS{ 1 };
    std::mutex dataMutex;

    std::unordered_map<int, std::vector<Version>> versionChain;
    std::unordered_map<int, int> txStartTimestamps;
    std::unordered_map<int, std::unordered_map<int, int>> txLocalViews;

public:
    SnapshotIsolationManager(int m) {
        for (int i = 0; i < m; ++i) {
            versionChain[i] = { {0, 0} };
        }
    }

    int beginTrans() {
        int txID = nextTxID.fetch_add(1);
        int ts = globalTS.fetch_add(1);
        txStartTimestamps[txID] = ts;
        txLocalViews[txID] = {};
        return txID;
    }

    int read(int txID, int index) {
        auto& localView = txLocalViews[txID];
        if (localView.find(index) != localView.end()) {
            return localView[index];
        }

        int start_ts = txStartTimestamps[txID];
        std::lock_guard<std::mutex> lk(dataMutex);
        for (auto it = versionChain[index].rbegin(); it != versionChain[index].rend(); ++it) {
            if (it->commit_ts <= start_ts) {
                return it->value;
            }
        }
        return 0; // fallback
    }

    void write(int txID, int index, int val) {
        txLocalViews[txID][index] = val;
    }

    bool commit(int txID) {
        int start_ts = txStartTimestamps[txID];
        auto& localView = txLocalViews[txID];

        std::lock_guard<std::mutex> lk(dataMutex);

        // Conflict check
        for (const auto& [index, _] : localView) {
            for (const auto& v : versionChain[index]) {
                if (v.commit_ts > start_ts) {
                    return false; // write-write conflict
                }
            }
        }

        int commit_ts = globalTS.fetch_add(1);
        for (const auto& [index, val] : localView) {
            versionChain[index].push_back({ val, commit_ts });
        }

        txLocalViews.erase(txID);
        txStartTimestamps.erase(txID);
        return true;
    }
};
