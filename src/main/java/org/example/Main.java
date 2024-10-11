package org.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

enum LockType {
    S, X, IS, IX;

    public static final boolean[][] compatibilityMatrix = {
            {true, false, true, false},
            {false, false, false, false},
            {true, false, true, true},
            {false, false, true, true},
    };
}

class ObjectLock {
    int ownerId;
    int resourceId;
    LockType type;

    boolean released;
    Lock releasedLock = new ReentrantLock();
    Condition releasedCond  = releasedLock.newCondition();

    ObjectLock blocker;
}

class LockDeque {
    Lock lock = new ReentrantLock();
    Deque<ObjectLock> objectLocks = new LinkedList<>();
}

class LockManager {
    private final Lock global = new ReentrantLock();
    private final Map<Integer, LockDeque> lockTable = new ConcurrentHashMap<>();

    public ObjectLock acquire(int resourceId, int ownerId, LockType lockType) throws Exception {
        global.lock();

        LockDeque ld;

        if(!lockTable.containsKey(resourceId)) {
            ld = new LockDeque();
            lockTable.put(resourceId, ld);
        } else {
            ld = lockTable.get(resourceId);
        }

        global.unlock();

        ObjectLock newLock = new ObjectLock();
        newLock.type = lockType;
        newLock.ownerId = ownerId;
        newLock.resourceId = resourceId;

        ld.lock.lock();

        ObjectLock prevLock = null;
        boolean allCurrentLocksCompatible = true;

        for(ObjectLock ol : ld.objectLocks) {
            if(prevLock == null) {
                prevLock = ol;
            } else if(!LockType.compatibilityMatrix[ol.type.ordinal()][prevLock.type.ordinal()]) {
                allCurrentLocksCompatible = false;
            }

            prevLock = ol;

            if(!allCurrentLocksCompatible) {
                break;
            }
        }

        ObjectLock firstBlocked = null;
        ObjectLock lastBlocked = null;
        boolean hasBlocker = false;

        for(ObjectLock ol : ld.objectLocks) {
            if(ol.ownerId == ownerId) {
                throw new Exception("Lock upgrade not supported");
            }

            if(!LockType.compatibilityMatrix[ol.type.ordinal()][lockType.ordinal()]) {
                hasBlocker = true;

                if(firstBlocked == null) {
                    firstBlocked = ol;
                }

                lastBlocked = ol;
            }
        }

        ld.objectLocks.add(newLock);
        ld.lock.unlock();

        if(allCurrentLocksCompatible) {
            if(hasBlocker) {
                lastBlocked.releasedLock.lock();

                while(!lastBlocked.released) {
                    lastBlocked.releasedCond.await();
                }

                lastBlocked.releasedLock.unlock();
            }
        } else {
            if(hasBlocker) {
                lastBlocked.releasedLock.lock();

                while(!lastBlocked.released) {
                    lastBlocked.releasedCond.await();
                }

                lastBlocked.releasedLock.unlock();
            } else {
                ObjectLock blockerOfFirst = firstBlocked.blocker;

                blockerOfFirst.releasedLock.lock();

                while(!blockerOfFirst.released) {
                    blockerOfFirst.releasedCond.await();
                }

                blockerOfFirst.releasedLock.unlock();
            }
        }

        return newLock;
    }

    public void release(ObjectLock objectLock) {
        global.lock();

        LockDeque ld = lockTable.get(objectLock.resourceId);

        global.unlock();

        ld.lock.lock();

        ld.objectLocks.remove(objectLock);

        objectLock.releasedLock.lock();
        objectLock.released = true;
        objectLock.releasedCond.signalAll();
        objectLock.releasedLock.unlock();

        ld.lock.unlock();
    }
}

public class Main {
    public static void main(String[] args) throws Exception {
        LockManager lm = new LockManager();

        ObjectLock l1 = lm.acquire(1, 1, LockType.S);
        ObjectLock l2 = lm.acquire(1, 2, LockType.S);
        ObjectLock l3 = lm.acquire(1, 3, LockType.IS);

        Thread t = new Thread(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            lm.release(l1);
            lm.release(l2);
            lm.release(l3);
        });

        t.start();

        ObjectLock l4 = lm.acquire(1, 4, LockType.X);
    }
}