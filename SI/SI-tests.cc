#include <gtest/gtest.h>
#include "SI.h"

// ✅ Test: Concurrent writers on different keys should not abort
TEST(SnapshotIsolationTest, ParallelWritersNonConflicting) {
    SnapshotIsolationManager manager(3);

    int tx1 = manager.beginTrans();
    int tx2 = manager.beginTrans();

    manager.write(tx1, 0, 10);
    manager.write(tx2, 1, 20);

    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_TRUE(manager.commit(tx2));
}

// ✅ Test: Concurrent writes to same key should cause one to abort
TEST(SnapshotIsolationTest, ParallelWritersConflicting) {
    SnapshotIsolationManager manager(1);

    int tx1 = manager.beginTrans();
    manager.write(tx1, 0, 10);

    int tx2 = manager.beginTrans();
    manager.read(tx2, 0); // establish snapshot
    manager.write(tx2, 0, 20);

    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_FALSE(manager.commit(tx2));
}

// ✅ Test: Read-your-writes within same transaction
TEST(SnapshotIsolationTest, ReadYourWrites) {
    SnapshotIsolationManager manager(1);

    int tx = manager.beginTrans();
    manager.write(tx, 0, 99);
    int val = manager.read(tx, 0);

    ASSERT_EQ(val, 99);
    ASSERT_TRUE(manager.commit(tx));
}

// ✅ Test: Read snapshot ignores concurrent uncommitted writes
TEST(SnapshotIsolationTest, IgnoreUncommittedWritesFromOthers) {
    SnapshotIsolationManager manager(1);

    int tx1 = manager.beginTrans();
    manager.write(tx1, 0, 123); // not yet committed

    int tx2 = manager.beginTrans();
    int val = manager.read(tx2, 0);

    ASSERT_EQ(val, 0); // tx2 sees initial committed state only
    ASSERT_TRUE(manager.commit(tx2));
    ASSERT_TRUE(manager.commit(tx1));
}

// ✅ Test: Garbage write (write then overwrite before commit)
TEST(SnapshotIsolationTest, OverwriteBeforeCommit) {
    SnapshotIsolationManager manager(1);

    int tx = manager.beginTrans();
    manager.write(tx, 0, 5);
    manager.write(tx, 0, 10); // overwrites previous write

    int val = manager.read(tx, 0);
    ASSERT_EQ(val, 10);
    ASSERT_TRUE(manager.commit(tx));

    int check = manager.read(manager.beginTrans(), 0);
    ASSERT_EQ(check, 10);
}

// ❌ Test: Write skew — non-serializable anomaly allowed by SI
TEST(SnapshotIsolationTest, WriteSkewAllowed) {
    SnapshotIsolationManager manager(2); // two shared booleans A and B initially 0

    int tx1 = manager.beginTrans();
    int A1 = manager.read(tx1, 0);
    int B1 = manager.read(tx1, 1);
    if (A1 == 0 && B1 == 0) manager.write(tx1, 0, 1); // set A = 1 if both 0

    int tx2 = manager.beginTrans();
    int A2 = manager.read(tx2, 0);
    int B2 = manager.read(tx2, 1);
    if (A2 == 0 && B2 == 0) manager.write(tx2, 1, 1); // set B = 1 if both 0

    // Both transactions may commit!
    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_TRUE(manager.commit(tx2));

    int A = manager.read(manager.beginTrans(), 0);
    int B = manager.read(manager.beginTrans(), 1);
    ASSERT_EQ(A, 1);
    ASSERT_EQ(B, 1);

    // This final state (A=1, B=1) would be impossible under serializable schedules
    // but is allowed under SI — classic write skew
}

// ✅ Test: Write-write conflict should cause abort in SI
TEST(SnapshotIsolationTest, WriteWriteConflictShouldAbort) {
    SnapshotIsolationManager manager(1);

    int tx1 = manager.beginTrans();
    manager.write(tx1, 0, 10);

    int tx2 = manager.beginTrans();
    manager.read(tx2, 0); // snapshot before tx1 commit
    manager.write(tx2, 0, 20);

    ASSERT_TRUE(manager.commit(tx1));
    ASSERT_FALSE(manager.commit(tx2));
}
