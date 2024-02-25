package dbengine.transaction;


import dbengine.storage.ITuple;
import dbengine.transaction.model.LockMode;
import dbengine.transaction.model.LockStrategy;
import dbengine.transaction.model.TxnReadView;
import dbms.SystemCatalog;

public class RepeatableRead extends IsolationLevelCommon {

    @Override
    public ITuple lockIfVisible(ITuple toBeLocked, LockMode lockMode, TxnReadView readView, LockStrategy strategy) {
        if (lockMode == null) {
            // 无锁，即快照读，寻找可见版本
            return findVisibleTuple(toBeLocked, readView);
        }
        // 有锁，即当前读，直接从链表头节点开始加锁，头节点为最新值，new->old
        return acquireLockImprovementInRR(toBeLocked, lockMode, strategy);
    }

    @Override
    public void unlockIfPossible(ITuple primaryLockedTuple, ITuple secondaryLockedTuple,
                                 LockMode lockMode, boolean isTreeSearchLastNotMatchCondition) {
        releaseLockImprovementInRR(primaryLockedTuple, secondaryLockedTuple, lockMode, isTreeSearchLastNotMatchCondition);
    }

    @Override
    public TxnReadView getTxnReadView() {
        if (readView == null) {
            int txnId = SystemCatalog.getTxnId(Thread.currentThread().getId());
            readView = new TxnReadView(txnId);
        }
        return readView;
    }

}
