#include <gtest/gtest.h>
#include "SI-SSN.h"

// ✅ Test: Read-only transactions always commit
TEST(SnapshotIsolationSSNTest, ReadOnlyAlwaysCommits) {
    SnapshotIsolationManager manager(1);
    int tx = manager.beginTrans();
    int val = manager.read(tx, 0);
    ASSERT_TRUE(manager.commit(tx));
}

// ✅ Test: Write to disjoint keys can proceed concurrently
TEST(SnapshotIsolationSSNTest, DisjointWritesNoAbort) {
    SnapshotIsolationManager manager(2);
    int tx1 = manager.beginTrans();
    int tx2 = manager.beginTrans();
    manager.write(tx1, 0, 100);
    manager.write(tx2, 1, 200);
    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_TRUE(manager.commit(tx2));
}

// ✅ Test: Write-write conflict must abort one
TEST(SnapshotIsolationSSNTest, ConflictingWritesMustAbort) {
    SnapshotIsolationManager manager(1);
    int tx1 = manager.beginTrans();
    int tx2 = manager.beginTrans();
    manager.write(tx1, 0, 111);
    manager.write(tx2, 0, 222);
    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_FALSE(manager.commit(tx2));
}

// ✅ Test: Classic Write Skew — must abort at least one
TEST(SnapshotIsolationSSNTest, WriteSkewShouldBeAborted) {
    SnapshotIsolationManager manager(2); // two variables: x and y

    // Initial values: x = 1, y = 1
    {
        int t0 = manager.beginTrans();
        manager.write(t0, 0, 1);
        manager.write(t0, 1, 1);
        ASSERT_TRUE(manager.commit(t0));
    }

    int tx1 = manager.beginTrans();
    int tx2 = manager.beginTrans();

    // tx1 reads x, writes y
    int x1 = manager.read(tx1, 0);
    manager.write(tx1, 1, 0);

    // tx2 reads y, writes x
    int y2 = manager.read(tx2, 1);
    manager.write(tx2, 0, 0);

    // This creates a dangerous structure:
    // tx1 reads x=1 (committed by t0), writes y=0
    // tx2 reads y=1 (committed by t0), writes x=0
    //
    // If both commit, final state is x=0, y=0 — which was impossible under any serial order
    // So one of them must be aborted

    bool c1 = manager.commit(tx1);
    bool c2 = manager.commit(tx2);

    EXPECT_FALSE(c1 && c2) << "SSN should prevent both tx1 and tx2 from committing due to write skew.";
}


// ✅ Test: Read-your-write correctness
TEST(SnapshotIsolationSSNTest, ReadYourWrites) {
    SnapshotIsolationManager manager(1);
    int tx = manager.beginTrans();
    manager.write(tx, 0, 55);
    int val = manager.read(tx, 0);
    ASSERT_EQ(val, 55);
    ASSERT_TRUE(manager.commit(tx));
}

// ✅ Test: Ignore uncommitted writes from others
TEST(SnapshotIsolationSSNTest, UncommittedWriteInvisible) {
    SnapshotIsolationManager manager(1);
    int tx1 = manager.beginTrans();
    manager.write(tx1, 0, 123); // not committed

    int tx2 = manager.beginTrans();
    int val = manager.read(tx2, 0);
    ASSERT_EQ(val, 0); // tx2 sees only committed state

    ASSERT_TRUE(manager.commit(tx2));
    ASSERT_TRUE(manager.commit(tx1));
}
