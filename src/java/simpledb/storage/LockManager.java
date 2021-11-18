package simpledb.storage;

import java.util.*;

import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.crypto.dsig.keyinfo.RetrievalMethod;

public class LockManager {

    public HashMap<PageId, LocksOnPage> locks;
    public HashMap<TransactionId, HashSet<PageId>> transactions;
    public HashMap<TransactionId, HashSet<TransactionId>> waitForGraph;
    
    class LocksOnPage{
        private TransactionId exclusiveLock;
        private HashSet<TransactionId> sharedLocks;

        LocksOnPage(){
            this.exclusiveLock = null;
            this.sharedLocks = new HashSet<>();
        }

        public Boolean hasExclusiveLock(){
            return this.exclusiveLock != null;
        }

        public Boolean hasSharedLocks(){
            return this.sharedLocks.isEmpty();
        }

        public TransactionId getExclusiveLock(){
            return this.exclusiveLock;
        }

        public HashSet<TransactionId> getSharedLocks(){
            return this.sharedLocks;
        }

        public void addExclusiveLock(TransactionId tid){
            this.exclusiveLock = tid;
        }

        public void addSharedLocks(TransactionId tid){
            this.sharedLocks.add(tid);
        }

        public void removeExclusiveLock(TransactionId tid){
            if (exclusiveLock == tid){
                this.exclusiveLock = null;
            }
        }

        public void removeSharedLocks(TransactionId tid){
            if (sharedLocks.contains(tid)){
                sharedLocks.remove(tid);
            }
        }
    }

    public LockManager() {
        locks = new HashMap<>();
        transactions = new HashMap<>();
        waitForGraph = new HashMap<>();
    }

    public synchronized Boolean releaseLock(TransactionId tid, PageId pid) {
        if (locks.containsKey(pid)){
            LocksOnPage locksOnCurPage = locks.get(pid);
            if (locksOnCurPage.getExclusiveLock() == tid){
                locksOnCurPage.removeExclusiveLock(tid);
            }
            if (locksOnCurPage.getSharedLocks().contains(tid)){
                locksOnCurPage.removeSharedLocks(tid);
            }
            locks.remove(pid);
            notifyAll();
            return true;
        }
        return false;
    }

    public synchronized void releaseAll(TransactionId tid) {
        if (transactions.containsKey(tid)) {
            Iterator<PageId> iter = transactions.get(tid).iterator();
            while (iter.hasNext()) {
                PageId pid = iter.next();
                this.releaseLock(tid, pid);
                iter.remove();
            }
        }
        waitForGraph.remove(tid);
    }

    public synchronized Boolean lockStatus(PageId pid) {
        if (locks.containsKey(pid)) {
            LocksOnPage currentLocks = locks.get(pid);
            if (currentLocks.getExclusiveLock() != null) {
                return true;
            }
            return currentLocks.getSharedLocks().size() > 0;
        }

        return false;
    }

    public synchronized Boolean lockStatus(TransactionId tid, PageId pid) {
        if (locks.containsKey(pid)){
            LocksOnPage currentLocks = locks.get(pid);
            if (currentLocks.getExclusiveLock() == tid){
                return true;
            }
            return currentLocks.getSharedLocks().contains(tid);
        }
        return false;
    }

    public synchronized Boolean upgradeLock(TransactionId tid, PageId pid) {
        LocksOnPage currentLocks = locks.get(pid);
        if (currentLocks.getSharedLocks().size() == 1) {
            currentLocks.removeSharedLocks(tid);
            currentLocks.addExclusiveLock(tid);
            return true;
        }
        return false;
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        this.locks.putIfAbsent(pid, new LocksOnPage());
        this.transactions.putIfAbsent(tid, new HashSet<>());
        LocksOnPage currentLocks = this.locks.get(pid);
        
        // while locked
        while(this.lockStatus(pid)) {
            // if no exclusive lock
            if (!currentLocks.hasExclusiveLock()) {
                // if perm is read/write
                if (perm.equals(Permissions.READ_WRITE)) {
                    // if it already has sharedlock (read only)
                    if (currentLocks.getSharedLocks().contains(tid)) {
                        // upgrade to exclusivelock
                        if (upgradeLock(tid, pid)) {
                            return;
                        }
                    }
                    this.waitForGraph.putIfAbsent(tid, new HashSet<>());
                    for (TransactionId waitingTid : currentLocks.getSharedLocks()) {
                        if (waitingTid != tid) {
                            this.waitForGraph.get(tid).add(waitingTid);
                        }
                    }
                    // check for deadlocks
                    if (detectDeadlock()) {
                        for (TransactionId waitingTid : currentLocks.getSharedLocks()) {
                            if (waitingTid != tid){
                                this.waitForGraph.get(tid).remove(waitingTid);
                            }
                        }
                        notifyAll();
                        throw new TransactionAbortedException();
                    }
                } else if (perm.equals(Permissions.READ_ONLY)){
                    if (lockStatus(tid, pid)) {
                        return;
                    }
                    currentLocks.addSharedLocks(tid);
                    this.transactions.putIfAbsent(tid, new HashSet<>());
                    transactions.get(tid).add(pid);
                    return;
                }
            // if there is exclusivelock and read/write
            } else {
                if (currentLocks.getExclusiveLock()==tid) {
                    return;
                } else {
                    this.waitForGraph.putIfAbsent(tid, new HashSet<>());
                    this.waitForGraph.get(tid).add(currentLocks.getExclusiveLock());
                    // check for deadlocks
                    if (detectDeadlock()) {
                        this.waitForGraph.get(tid).remove(currentLocks.getExclusiveLock());
                        notifyAll();
                        throw new TransactionAbortedException();
                    }
                }
            }
            try {
                wait();
            } catch (InterruptedException e){
                System.out.println("error");
            }
        }
        // if not lock, just add locks
        if  (perm.equals(Permissions.READ_WRITE)) {
            currentLocks.addExclusiveLock(tid);
        }
        if (perm.equals(Permissions.READ_ONLY)) {
            currentLocks.addSharedLocks(tid);
        }
        this.transactions.putIfAbsent(tid, new HashSet<>());
        transactions.get(tid).add(pid);
    }

    public synchronized Boolean detectDeadlock() throws TransactionAbortedException{
        HashMap<TransactionId, Integer> dlMap = new HashMap<>();
        Deque<TransactionId> queue = new LinkedList<>();

        for (TransactionId tid: this.transactions.keySet()){
            dlMap.putIfAbsent(tid,0);
        }
        
        for (TransactionId tid1: this.transactions.keySet()){
            if (this.waitForGraph.containsKey(tid1)){
                for (TransactionId tid2: this.waitForGraph.get(tid1)){
                    dlMap.replace(tid2, dlMap.get(tid2) + 1);
                }
            }
        }

        for  (TransactionId tid: this.transactions.keySet()){
            if (dlMap.get(tid) == 0){
                queue.offer(tid);
            }
        }

        int count = 0;
        while (!queue.isEmpty()){
            TransactionId tid1 = queue.poll();
            count += 1;

            if (!this.waitForGraph.containsKey(tid1)){
                continue;
            }

            for (TransactionId tid2: this.waitForGraph.get(tid1)) {
                if (dlMap.get(tid2) != 0) {
                    dlMap.replace(tid2, dlMap.get(tid2) - 1);
                } 
                if (dlMap.get(tid2) == 0) {
                    queue.offer(tid2);
                }
            }
        }
        return count != this.transactions.size();
    }

}