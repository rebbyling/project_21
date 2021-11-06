package simpledb.storage;

import java.util.*;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Permissions;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    public HashMap<TransactionId, Set<PageId>> tidToPg;
    private final HashMap<PageId, Set<TransactionId>> pgToTid;
    private final HashMap<PageId, Permissions> pgToPerm;
    private final HashMap<TransactionId, PageId> acqLockPg;
    
    public LockManager() {
        tidToPg = new HashMap<TransactionId, Set<PageId>>();
        pgToTid = new HashMap<PageId, Set<TransactionId>>();
        pgToPerm = new HashMap<PageId, Permissions>();
        acqLockPg = new HashMap<TransactionId, PageId>();
    }

    public synchronized Boolean releaseLock(TransactionId tid, PageId pid) {
        if (!tidToPg.containsKey(tid) || !tidToPg.get(tid).contains(pid)) {
            return false;
        }
        if (!pgToTid.containsKey(pid) || !pgToTid.get(pid).contains(tid)) {
            return false;
        }
        
        tidToPg.get(tid).remove(pid);
        if (tidToPg.get(tid).isEmpty()) {
            tidToPg.remove(tid);
        }

        pgToTid.get(pid).remove(tid);
        if( pgToTid.get(pid).isEmpty()) {
            pgToTid.remove(pid);
            pgToPerm.remove(pid);
        }
        return true;
    }
    
    public synchronized void releaseAll(TransactionId tid) {
        if (tidToPg.containsKey(tid)) {
            Iterator<PageId> iter = tidToPg.get(tid).iterator();
            while (iter.hasNext()) {
                PageId pid = iter.next();
                this.releaseLock(tid, pid);
                iter.remove();
            }
        }
    }

    public synchronized void releaseTransaction(TransactionId tid) {
        Set<PageId> pids = new HashSet<PageId>();
        if (tidToPg.containsKey(tid)) {
            for (PageId pid : tidToPg.get(tid)) {
                pids.add(pid);
            }
        }

        for (PageId pid : pids) {
            releaseLock(tid, pid);
        }
        acqLockPg.remove(tid);

    }

    public synchronized Boolean lockStatus(TransactionId tid, PageId pid, Permissions perm) {
        if (!pgToPerm.containsKey(pid)) {
            return true;
        }

        if (pgToPerm.get(pid).equals(Permissions.READ_ONLY)) {
            if (perm.equals(Permissions.READ_ONLY)) {
                return true;
            } else {
                return !(pgToTid.containsKey(pid) && pgToTid.get(pid).size() >= 2) && pgToTid.get(pid).contains(tid);
            }
        } else {
            return pgToTid.get(pid).contains(tid);
        }
    }

    public synchronized Boolean acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        
        if (!acqLockPg.containsKey(tid)) {
            acqLockPg.put(tid, pid);
            detectDeadlock(tid, pid);
        }

        if (!lockStatus(tid, pid, perm)) {
            return false;
        }
        
        if (!tidToPg.containsKey(tid)) {
            this.tidToPg.put(tid, new HashSet<PageId>());
        }
        
        if (!pgToTid.containsKey(pid)) {
            this.pgToTid.put(pid, new HashSet<TransactionId>());
        }
        
        pgToPerm.put(pid, perm);
        pgToTid.get(pid).add(tid);
        tidToPg.get(tid).add(pid);
        acqLockPg.remove(tid);

        return true;
    }

    private synchronized Set<TransactionId> getWaitingTid(TransactionId tid) {
        Set<TransactionId> tidSet = new HashSet<TransactionId>();
        if (!acqLockPg.containsKey(tid)) {
            return tidSet;
        }
        PageId pid = acqLockPg.get(tid);
        if (pgToTid.containsKey(acqLockPg.get(tid))) {
            for (TransactionId iterateTid : pgToTid.get(pid)) {
                tidSet.add(iterateTid);
            }
        }
        return tidSet;
    }

    public synchronized void detectDeadlock(Set<TransactionId> tidSet, TransactionId tid) throws TransactionAbortedException{

        if (tidSet.contains(tid)) {
            throw new TransactionAbortedException();
        }
        tidSet.add(tid);
        
        for (TransactionId iterateTid: getWaitingTid(tid)) {
            if (!iterateTid.equals(tid)) {
                detectDeadlock(tidSet, tid);
            }
        }
    }

    public synchronized void detectDeadlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (pgToTid.containsKey(pid)) {
            for (TransactionId iterateTid: pgToTid.get(pid)) {
                if (!tid.equals(iterateTid)) {
                    detectDeadlock(new HashSet<TransactionId>(), tid);
                }
            }
        }
    }
}