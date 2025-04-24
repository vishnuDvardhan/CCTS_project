#include <gtest/gtest.h>
#include "SI.h"

//  Test 1: Read-only transaction should never abort
TEST(SnapshotIsolationTest, ReadOnlyAlwaysCommits) {
    SnapshotIsolationManager manager(1);
    std::unordered_map<int, int> view;
    int tx = manager.beginTrans();
    int val;
    manager.readVal(tx, 0, val, view);
    ASSERT_TRUE(manager.tryCommit(tx, view));
}

//  Test 2: Two non-overlapping writers should both commit
TEST(SnapshotIsolationTest, NonOverlappingWritesDoNotConflict) {
    SnapshotIsolationManager manager(2);
    std::unordered_map<int, int> view1, view2;
    int tx1 = manager.beginTrans();
    manager.writeVal(tx1, 0, 10, view1);
    ASSERT_TRUE(manager.tryCommit(tx1, view1));

    int tx2 = manager.beginTrans();
    manager.writeVal(tx2, 1, 20, view2);
    ASSERT_TRUE(manager.tryCommit(tx2, view2));
}

//  Test 3: T1 commits before T2 starts — no abort
TEST(SnapshotIsolationTest, T1CommitsBeforeT2Starts_NoConflict) {
    SnapshotIsolationManager manager(1);
    std::unordered_map<int, int> view1, view2;
    int tx1 = manager.beginTrans();
    manager.writeVal(tx1, 0, 100, view1);
    ASSERT_TRUE(manager.tryCommit(tx1, view1));

    int tx2 = manager.beginTrans();
    manager.writeVal(tx2, 0, 200, view2);
    ASSERT_TRUE(manager.tryCommit(tx2, view2));
}

//  Test 4: T2 overwrites a value it saw in snapshot — no abort
TEST(SnapshotIsolationTest, OverwriteSameSeenValue) {
    SnapshotIsolationManager manager(1);
    std::unordered_map<int, int> view1, view2;

    int tx1 = manager.beginTrans();
    manager.writeVal(tx1, 0, 5, view1);
    ASSERT_TRUE(manager.tryCommit(tx1, view1));

    int tx2 = manager.beginTrans();
    int val;
    manager.readVal(tx2, 0, val, view2); // should see 5
    manager.writeVal(tx2, 0, 10, view2); // overwrite seen value
    ASSERT_TRUE(manager.tryCommit(tx2, view2));
}

//  Test 5: Only one of two writers to same key should commit
TEST(SnapshotIsolationTest, WriteWriteConflictCausesAbort) {
    SnapshotIsolationManager manager(1);
    std::unordered_map<int, int> view1, view2;

    int tx1 = manager.beginTrans();
    manager.writeVal(tx1, 0, 1, view1);

    int tx2 = manager.beginTrans();
    int dummy;
    manager.readVal(tx2, 0, dummy, view2); // snapshot

    ASSERT_TRUE(manager.tryCommit(tx1, view1));
    manager.writeVal(tx2, 0, 2, view2);
    ASSERT_FALSE(manager.tryCommit(tx2, view2));
}


// Test 6: Write skew anomaly (non-serializable but SI allows it)
// Scenario: T1 reads A and B, writes A; T2 reads A and B, writes B.
TEST(SnapshotIsolationTest, WriteSkewNotSerializable) {
    SnapshotIsolationManager manager(2); // A = 0, B = 0

    std::unordered_map<int, int> view1, view2;

    int tx1 = manager.beginTrans();
    int a1, b1;
    manager.readVal(tx1, 0, a1, view1); // read A
    manager.readVal(tx1, 1, b1, view1); // read B
    if (a1 == 0 && b1 == 0) manager.writeVal(tx1, 0, 1, view1); // set A=1

    int tx2 = manager.beginTrans();
    int a2, b2;
    manager.readVal(tx2, 0, a2, view2); // read A
    manager.readVal(tx2, 1, b2, view2); // read B
    if (a2 == 0 && b2 == 0) manager.writeVal(tx2, 1, 1, view2); // set B=1

    ASSERT_TRUE(manager.tryCommit(tx1, view1));
    ASSERT_TRUE(manager.tryCommit(tx2, view2));

    // After both commit, we end up with A=1 and B=1 despite both checking that A==0 and B==0
    // This is not serializable but SI allows it (classic write skew)
}