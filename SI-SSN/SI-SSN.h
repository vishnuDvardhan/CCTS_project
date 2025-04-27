#pragma once

#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <algorithm>
#include <climits>

// Metadata required for SSN as per Table 1 in the paper
struct Version {
    int value;
    int t_cstamp;       // Transaction commit timestamp, c(T)
    int r_pstamp;       // Predecessor high-water mark, p(T)
    int s_pstamp;       // Successor low-water mark, s(T)
    int v_pstamp;       // Version predecessor stamp, p(V)
    Version* v_prev;    // Pointer to overwritten version
    std::unordered_set<int> t_reads;  // Transactions that read this version
    std::unordered_set<int> t_writes; // Transactions that wrote this version
};

enum TransactionStatus {
    IN_FLIGHT,
    COMMITTED,
    ABORTED
};

struct Transaction {
    int txID;
    int start_ts;
    int t_cstamp = -1;      // Commit timestamp
    int t_pstamp = 0;       // Predecessor high-water mark
    int s_pstamp = INT_MAX; // Successor low-water mark
    TransactionStatus t_status = IN_FLIGHT;
    std::unordered_set<int> t_reads;  // Read set indices
    std::unordered_set<int> t_writes; // Write set indices
    std::vector<int> writeValues;     // -1 means no write for that index
};

class SnapshotIsolationManager {
private:
    std::atomic<int> nextTxID{ 1000 };
    std::atomic<int> globalTS{ 1 };
    std::mutex dataMutex;

    int numDataItems;
    std::vector<std::vector<Version*>> versionChain; // versionChain[index] = list of versions
    std::unordered_map<int, Transaction*> transactions; // txID -> Transaction

public:
    SnapshotIsolationManager(int m)
        : numDataItems(m), versionChain(m) {
        for (int i = 0; i < m; ++i) {
            Version* initialVersion = new Version{ 0, 0, 0, INT_MAX, 0, nullptr };
            versionChain[i].push_back(initialVersion);
        }
    }

    ~SnapshotIsolationManager() {
        // Cleanup all memory
        for (auto& versions : versionChain) {
            for (auto* v : versions) {
                delete v;
            }
        }
        for (auto& [_, txn] : transactions) {
            delete txn;
        }
    }

    int beginTrans() {
        int txID = nextTxID.fetch_add(1);
        int ts = globalTS.fetch_add(1);
        {
            std::lock_guard<std::mutex> lk(dataMutex);
            Transaction* txn = new Transaction();
            txn->txID = txID;
            txn->start_ts = ts;
            txn->writeValues.assign(numDataItems, -1);
            transactions[txID] = txn;
        }
        return txID;
    }

    int read(int txID, int index) {
        std::lock_guard<std::mutex> lk(dataMutex);
        auto* txn = transactions[txID];

        // Check if transaction is still valid
        if (txn->t_status == ABORTED) {
            return -1; // Transaction already aborted
        }

        // Record read in transaction's read set
        txn->t_reads.insert(index);

        // First check if we've written to this item
        if (txn->writeValues[index] != -1) {
            return txn->writeValues[index];
        }

        // Get the visible version according to snapshot isolation
        int start_ts = txn->start_ts;
        Version* visibleVersion = nullptr;

        for (auto it = versionChain[index].rbegin(); it != versionChain[index].rend(); ++it) {
            if ((*it)->t_cstamp <= start_ts) {
                visibleVersion = *it;
                break;
            }
        }

        // No visible version found (shouldn't happen with initial versions)
        if (!visibleVersion) {
            return 0;
        }

        // Update the version's reader set and transaction's read set
        visibleVersion->t_reads.insert(txID);

        // SSN: Update transaction's predecessor timestamp (t_pstamp)
        // t_pstamp = max(t_pstamp, c(V))
        txn->t_pstamp = std::max(txn->t_pstamp, visibleVersion->t_cstamp);

        // Update the transaction's predecessor high-water mark
        // based on the version's predecessor timestamp
        txn->t_pstamp = std::max(txn->t_pstamp, visibleVersion->v_pstamp);

        return visibleVersion->value;
    }

    void write(int txID, int index, int val) {
        std::lock_guard<std::mutex> lk(dataMutex);
        auto* txn = transactions[txID];

        // Check if transaction is still valid
        if (txn->t_status == ABORTED) {
            return; // Transaction already aborted
        }

        // Record write intent
        txn->writeValues[index] = val;
        txn->t_writes.insert(index);
    }

    // Function to update version timestamps based on SSN
    void update_version_timestamps(Version* v) {
        for (int readerID : v->t_reads) {
            auto readerIt = transactions.find(readerID);
            if (readerIt != transactions.end() && readerIt->second->t_status == IN_FLIGHT) {
                // Update s_pstamp = min(s_pstamp, r.t_cstamp)
                readerIt->second->s_pstamp = std::min(readerIt->second->s_pstamp, v->t_cstamp);
            }
        }
    }

    // SSN validation function as per the paper
    bool validateSSN(Transaction* txn) {
        // Check the exclusion window condition: p(T) < s(T)
        return txn->t_pstamp < txn->s_pstamp;
    }

    bool commit(int txID) {
        std::lock_guard<std::mutex> lk(dataMutex);
        auto* txn = transactions[txID];

        // Check if transaction is still valid
        if (txn->t_status == ABORTED) {
            return false; // Transaction already aborted
        }

        // Pre-commit phase
        // 1. Finalize pi(T) - already done during reads
        // 2. Acquire commit timestamp
        int commit_ts = globalTS.fetch_add(1);
        txn->t_cstamp = commit_ts;

        // 3. Check exclusion window
        if (!validateSSN(txn)) {
            txn->t_status = ABORTED;
            return false; // Abort due to serializability violation
        }

        // 4. Check for write-write conflicts (basic SI)
        for (int index : txn->t_writes) {
            Version* latest = versionChain[index].back();
            if (latest->t_cstamp > txn->start_ts) {
                txn->t_status = ABORTED;
                return false; // Write-write conflict
            }
        }

        // All validation passed, proceed with commit
        txn->t_status = COMMITTED;

        // Create new versions for each written item
        for (int index : txn->t_writes) {
            Version* oldVersion = versionChain[index].back();
            Version* newVersion = new Version{
                txn->writeValues[index],  // value
                txn->t_cstamp,           // t_cstamp
                0,                       // r_pstamp (initialized to 0)
                INT_MAX,                 // s_pstamp (initialized to ∞)
                oldVersion->t_cstamp,    // v_pstamp = commit timestamp of overwritten version
                oldVersion               // v_prev points to overwritten version
            };

            versionChain[index].push_back(newVersion);

            // Update version timestamps
            update_version_timestamps(oldVersion);
        }

        return true;
    }

    void abort(int txID) {
        std::lock_guard<std::mutex> lk(dataMutex);
        auto* txn = transactions[txID];
        txn->t_status = ABORTED;
    }

    // Clean up memory for completed transactions
    void cleanup() {
        std::lock_guard<std::mutex> lk(dataMutex);
        std::vector<int> toRemove;

        for (auto& [id, txn] : transactions) {
            if (txn->t_status != IN_FLIGHT) {
                toRemove.push_back(id);
            }
        }

        for (int id : toRemove) {
            delete transactions[id];
            transactions.erase(id);
        }
    }
};