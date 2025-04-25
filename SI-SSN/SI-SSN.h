#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <atomic>
#include <limits>

struct Version {
    int value;
    int commit_ts;
    int writer_tx;
    int ntstamp = std::numeric_limits<int>::max();
    int pstamp = 0;
};

class SnapshotIsolationSSNManager {
private:
    std::atomic<int> nextTxID{ 1000 };
    std::atomic<int> globalTS{ 1 };
    std::mutex dataMutex;

    std::unordered_map<int, std::vector<Version>> versionChain;
    std::unordered_map<int, int> txStartTimestamps;
    std::unordered_map<int, std::unordered_map<int, int>> txLocalViews;
    std::unordered_map<int, std::unordered_map<int, std::unordered_set<int>>> readers;

    void garbageCollect(int index) {
        auto& chain = versionChain[index];
        size_t keepFrom = 0;
        int minActiveTS = std::numeric_limits<int>::max();

        for (const auto& [tx, ts] : txStartTimestamps) {
            minActiveTS = std::min(minActiveTS, ts);
        }

        for (size_t i = chain.size(); i-- > 0;) {
            if (chain[i].commit_ts <= minActiveTS) {
                keepFrom = i;
                break;
            }
        }

        if (keepFrom > 0) {
            chain.erase(chain.begin(), chain.begin() + keepFrom);
        }
    }

public:
    SnapshotIsolationSSNManager(int m) {
        for (int i = 0; i < m; ++i) {
            versionChain[i] = { {0, 0, -1} };
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
                readers[index][it->commit_ts].insert(txID);
                it->ntstamp = std::min(it->ntstamp, start_ts);
                return it->value;
            }
        }
        return 0;
    }

    void write(int txID, int index, int val) {
        txLocalViews[txID][index] = val;
    }

    bool commit(int txID) {
        int start_ts = txStartTimestamps[txID];
        auto& localView = txLocalViews[txID];

        std::lock_guard<std::mutex> lk(dataMutex);

        // Step 1: Check for write-write conflicts (SI)
        for (const auto& [index, _] : localView) {
            for (const auto& v : versionChain[index]) {
                if (v.commit_ts > start_ts) {
                    return false;
                }
            }
        }

        // Step 2: Calculate exclusion window (SSN)
        int pst = 0;
        int sst = std::numeric_limits<int>::max();

        for (const auto& [index, val] : localView) {
            for (auto it = versionChain[index].rbegin(); it != versionChain[index].rend(); ++it) {
                if (it->commit_ts <= start_ts) {
                    sst = std::min(sst, it->ntstamp);
                    pst = std::max(pst, it->pstamp);
                    break;
                }
            }
        }

        if (pst >= sst) {
            return false;
        }

        // Step 3: Assign commit timestamp and install new versions
        int commit_ts = globalTS.fetch_add(1);
        for (const auto& [index, val] : localView) {
            for (auto it = versionChain[index].rbegin(); it != versionChain[index].rend(); ++it) {
                if (it->commit_ts <= start_ts) {
                    it->pstamp = std::max(it->pstamp, commit_ts);
                    break;
                }
            }
            versionChain[index].push_back({ val, commit_ts, txID });
            garbageCollect(index);
        }

        txLocalViews.erase(txID);
        txStartTimestamps.erase(txID);
        return true;
    }
};
