package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement- complete
        TransactionTableEntry trans = this.transactionTable.get(transNum); //create new trans table
        long lastLS = trans.lastLSN; //create LSN from prev LSN
        LogRecord comLog = new CommitTransactionLogRecord(transNum, lastLS); //create new log record
        long updateLS = logManager.appendToLog(comLog); //create LSN to update
        trans.lastLSN = updateLS; //update LSN to prev one
        this.pageFlushHook(updateLS); //flush log
        trans.transaction.setStatus(Transaction.Status.COMMITTING); //commit transaction
        return updateLS;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement- complete
        TransactionTableEntry trans = this.transactionTable.get(transNum); //create new trans table
        long lastLS = trans.lastLSN; //create new LSN to set to previous
        long updateLS = logManager.appendToLog(new AbortTransactionLogRecord(transNum, lastLS)); //new LSN to update
        trans.transaction.setStatus(Transaction.Status.ABORTING); //abort records
        trans.lastLSN = updateLS; //update LSN to previous LSN
        return updateLS;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement- complete
        TransactionTableEntry trans = transactionTable.get(transNum); //create new trans table
        Transaction newTrans = trans.transaction; //new transaction from table
        if (newTrans.getStatus().equals(Transaction.Status.ABORTING)) { //if transaction is aborting
            long lastLS = trans.lastLSN; //create LSN for previous LSN
            long holdLast = lastLS; //create placeholder for previous LSN
            while (holdLast != 0) { //as long as previous is there
                lastLS = holdLast; //set LSN to holder
                LogRecord newLog = logManager.fetchLogRecord(holdLast); //create mew log to hold prev
                holdLast = newLog.getPrevLSN().get(); //set place holder to prev LSN
            }
            rollbackToLSN(transNum, lastLS - 1); //rollback from prev LSN
        }
        LogRecord newLastL = new EndTransactionLogRecord(transNum, trans.lastLSN); //create new log for ending
        logManager.appendToLog(newLastL); //add to manager the last log
        transactionTable.remove(transNum); //remove from table
        newTrans.setStatus(Transaction.Status.COMPLETE); //set the status to complete
        return newLastL.LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        while (currentLSN > LSN) { //while current LSN is greater than LSM
            LogRecord recordAtCurr = logManager.fetchLogRecord(currentLSN); //get record at current LSN
            if (recordAtCurr.isUndoable()) { //if record at current LSN is undoable
                LogRecord compLog = recordAtCurr.undo(transactionEntry.lastLSN); //get CLR by calling undo
                logManager.appendToLog(compLog); //append the CLR
                compLog.redo(this, diskSpaceManager, bufferManager); //call redo on CLR
                transactionEntry.lastLSN = compLog.LSN; //set trans entry to comp log
            }
            currentLSN = recordAtCurr.getUndoNextLSN().orElse(recordAtCurr.getPrevLSN().get()); //update current LSN
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement- complete
        TransactionTableEntry trans = transactionTable.get(transNum); //create new table entry
        long lastLS = trans.lastLSN; //create prev LSN
        long addLs = this.logManager.appendToLog //create LSN to add to manager
                (new UpdatePageLogRecord(transNum, pageNum, lastLS, pageOffset, before, after));
        trans.lastLSN = addLs; //set LSN to appended one
        if (!dirtyPageTable.containsKey(pageNum)) { //dirty page table does not have pagenum
            dirtyPageTable.put(pageNum, addLs); //add pagenum to it
        }
        return addLs;
    }


    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement- complete
        rollbackToLSN(transNum, savepointLSN); //use rollbackToLSN for savepoint
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table- complete

        TransactionTableEntry trTaEntry; //create new table entry
        LogRecord createLog; //create new record
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) { //loop through entry sets
            if (!EndCheckpointLogRecord.fitsInOneRecord
                    (chkptDPT.size() + 1, chkptTxnTable.size())) { //if records fit
                createLog = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable); //set record to checkpoint
                logManager.appendToLog(createLog); //add to log manager
                chkptDPT = new HashMap<>(); //create new table maps
                chkptTxnTable = new HashMap<>();
            }
            chkptDPT.put(entry.getKey(), entry.getValue()); //add to tables the entries
        }
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) { //loop through map
            if (!EndCheckpointLogRecord.fitsInOneRecord
                    (chkptDPT.size(), chkptTxnTable.size() + 1)) { //if record fits into one
                createLog = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable); //set log ad checkpoint tables
                logManager.appendToLog(createLog); //add it to log manager
                chkptDPT = new HashMap<>(); //maps as before
                chkptTxnTable = new HashMap<>();
            }
            trTaEntry = entry.getValue(); //add to entry
            chkptTxnTable.put(entry.getKey(),
                    new Pair<>(trTaEntry.transaction.getStatus(), trTaEntry.lastLSN)); //add to table
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement- complete
        //setup
        Iterator<LogRecord> iter = logManager.scanFrom(LSN); //create new log record iteration
        while (iter.hasNext()) { //while the next in log record is not null
            LogRecord logNext = iter.next(); //the next in the iteration
            //for transaction operations
            if (logNext.getTransNum().isPresent()) { //if record is for transaction operation
                Long getNum = logNext.getTransNum().get(); //get transaction num of next record
                if (!transactionTable.containsKey(getNum)) { //if the table does not contain key of num
                    transactionTable.put(getNum,
                            new TransactionTableEntry(newTransaction.apply(getNum))); } //put num in table
                transactionTable.get(getNum).lastLSN = logNext.LSN; } //set the record's LSN to the table's last LSN
            //for page related
            if (logNext.getPageNum().isPresent()) { //if the record is page related
                if (logNext.type.equals(LogType.UPDATE_PAGE) || //if the page is update page
                        logNext.type.equals(LogType.UNDO_UPDATE_PAGE)) { //or if its undo update page
                    dirtyPageTable.putIfAbsent(logNext.getPageNum().get(), logNext.getLSN()); } //dirty pages
                if (logNext.type.equals(LogType.FREE_PAGE) || //if the page is free page
                        logNext.type.equals(LogType.UNDO_ALLOC_PAGE)) { //or if its undo alloc page
                    dirtyPageTable.remove(logNext.getPageNum().get()); //remove the page num from dirty table
                    logManager.flushToLSN(logNext.getLSN()); } //flush to LSN
            }
            //for changes in transaction
            if (logNext.type.equals(LogType.COMMIT_TRANSACTION) || //if the transaction type is commit
                    logNext.type.equals(LogType.END_TRANSACTION) || //or if its end trans
                    logNext.type.equals(LogType.ABORT_TRANSACTION)) { //or if its abort trans
                Long getNum = logNext.getTransNum().get(); //get the trans num from log
                transactionTable.putIfAbsent(getNum,
                        new TransactionTableEntry(newTransaction.apply(getNum))); //put num in transaction table
                TransactionTableEntry tbl = transactionTable.get(getNum); //create table entry from num
                Transaction transaction = tbl.transaction; //set transaction to table transaction
                tbl.lastLSN = logNext.getLSN(); //set the LSN of transaction to log LSN
                if (logNext.type.equals(LogType.COMMIT_TRANSACTION)) { //if the transaction is commit
                    transaction.setStatus(Transaction.Status.COMMITTING); } //set status to committing
                else if (logNext.type.equals(LogType.END_TRANSACTION)) { //if transaction is end
                    transaction.cleanup(); //cleanup transaction
                    transactionTable.remove(getNum); //remove from transaction table
                    endedTransactions.add(getNum); //add to ended transactions
                    transaction.setStatus(Transaction.Status.COMPLETE);} //set status to complete
                else if (logNext.type.equals(LogType.ABORT_TRANSACTION)) { //if abort transaction
                    transaction.setStatus(Transaction.Status.RECOVERY_ABORTING); } //set status to recovery aborting
            }
            //for end checkpoint
            if (logNext.type.equals(LogType.END_CHECKPOINT)) { //if the record is end checkpoint record
                for (Map.Entry<Long, Long> log: logNext.getDirtyPageTable().entrySet()) { //loop through dirty page tbl
                    if (dirtyPageTable.containsKey(log.getKey())) { //if the DPT contains key
                        dirtyPageTable.replace(log.getKey(), log.getValue()); } //replace with log value
                    else {
                        dirtyPageTable.put(log.getKey(), log.getValue()); } //otherwise put log value in
                }
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> transaction:
                        logNext.getTransactionTable().entrySet()) { //lopp through transaction table
                    if (transaction.getValue().getFirst().equals(Transaction.Status.COMPLETE) || //if trans is complete
                            endedTransactions.contains(transaction.getKey())) { //or if transaction is ended
                        continue; }
                    if (transactionTable.containsKey(transaction.getKey())) { //if the transaction has key
                        if (transaction.getValue().getSecond() >
                                transactionTable.get(transaction.getKey()).lastLSN) { //if the entry is > LSN
                            transactionTable.get(transaction.getKey()).lastLSN =
                                    transaction.getValue().getSecond(); } //set LSN of trans to larger
                        Transaction.Status trStatus = transaction.getValue().getFirst(); //set status from trans
                        Transaction.Status keyStat =
                                transactionTable.get(transaction.getKey()).transaction.getStatus(); //set stat from key
                        if (!trStatus.equals(Transaction.Status.RUNNING) && //if status is running and not second stat
                                !trStatus.equals(keyStat)) {
                            if (trStatus.equals(Transaction.Status.COMPLETE) || //if trans is complete
                                    (trStatus.equals(Transaction.Status.COMMITTING) &&
                                            keyStat.equals(Transaction.Status.RUNNING))) { //or commit and 2nd is run
                                transactionTable.get(transaction.getKey())
                                        .transaction.setStatus(trStatus); } //update status
                            else if (trStatus.equals(Transaction.Status.ABORTING) &&
                                    keyStat.equals(Transaction.Status.RUNNING)) { //if stat is abort and 2nd is running
                                transactionTable.get(transaction.getKey()).transaction.setStatus
                                        (Transaction.Status.RECOVERY_ABORTING); } //set stat to recovery abort
                        }
                    }
                    else {
                        TransactionTableEntry tbl = //create new table entry
                                new TransactionTableEntry(newTransaction.apply(transaction.getKey()));
                        tbl.lastLSN = transaction.getValue().getSecond(); //set LSN to trans LSN
                        Transaction.Status getStat = transaction.getValue().getFirst(); //set status to trans status
                        if (getStat.equals(Transaction.Status.ABORTING)) { //if status is aborting
                            getStat = Transaction.Status.RECOVERY_ABORTING; } //set status to recovery aborting
                        tbl.transaction.setStatus(getStat); //update status
                        transactionTable.put(transaction.getKey(), tbl); } //put in transaction table
                }
            }
        }
        //after all records in log are processed
        for (Map.Entry<Long, TransactionTableEntry> tbl: transactionTable.entrySet()) { //loop through transaction tbl
            TransactionTableEntry getTbl = tbl.getValue(); //get table value
            if (getTbl.transaction.getStatus().equals(Transaction.Status.COMMITTING)) { //if status is committing
                getTbl.transaction.cleanup(); //clean up transaction
                getTbl.transaction.setStatus(Transaction.Status.COMPLETE); //set status to complete
                transactionTable.remove(tbl.getKey()); //remove transaction from table
                logManager.appendToLog
                        (new EndTransactionLogRecord(tbl.getKey(), getTbl.lastLSN)); } //append end record
            else if (getTbl.transaction.getStatus().equals(Transaction.Status.RUNNING)) { //if status is running
                getTbl.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING); //set status to recovery abort
                LogRecord endRec = new AbortTransactionLogRecord
                        (tbl.getKey(), getTbl.lastLSN); //create end record
                logManager.appendToLog(endRec); //add end record to log manager
                getTbl.lastLSN = endRec.getLSN(); } //set LSN to end record LSN
        }
    }

    public boolean isPartitionType(LogRecord record) {
        return record.getType() == LogType.ALLOC_PART || record.getType() == LogType.FREE_PART ||
                record.getType() == LogType.UNDO_ALLOC_PART || record.getType() == LogType.UNDO_FREE_PART;
    }

    public boolean isPageType(LogRecord record) {
        return record.getType() == LogType.UNDO_FREE_PAGE || record.getType() == LogType.UNDO_UPDATE_PAGE ||
                record.getType() == LogType.UNDO_ALLOC_PAGE || record.getType() == LogType.UPDATE_PAGE ||
                record.getType() == LogType.ALLOC_PAGE || record.getType() == LogType.FREE_PAGE;
    }

    public TransactionTableEntry createNewXact(TransactionTableEntry entry, long transNum) {
        return createNewXactType(entry, transNum, null);
    }

    public TransactionTableEntry createNewXactType(TransactionTableEntry entry, long transNum, Transaction.Status status) {
        if (entry != null) return entry;
        Transaction xact = newTransaction.apply(transNum);
        if (status != null) {
            xact.setStatus(status);
        }
        startTransaction(xact);
        return this.transactionTable.get(xact.getTransNum());
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement- complete
        long maxLS = Long.MAX_VALUE; //create new LSN
        for (Long prevLS : dirtyPageTable.values()) { //loop through prev LSNs of table
            if (maxLS > prevLS) { //if new is more than old
                maxLS = prevLS; //set new as old
            }
        }
        Iterator<LogRecord> LSNManager = logManager.scanFrom(maxLS); //create new iterative log record
        boolean boolRec = true; //set boolean
        while (LSNManager.hasNext()) { //loop through LSN iterator
            LogRecord LSNRecord = LSNManager.next(); //create new log record for next of manager
            if (LSNRecord.isRedoable()) { //if the record is redoable
                LogType LSNlog = LSNRecord.getType(); //create log type for record
                if (LSNlog == LogType.UPDATE_PAGE || LSNlog == LogType.UNDO_UPDATE_PAGE ||
                        LSNlog == LogType.FREE_PAGE || LSNlog == LogType.UNDO_ALLOC_PAGE) { //if logs are page types
                    Page buffPg = bufferManager.fetchPage(new DummyLockContext(),
                            LSNRecord.getPageNum().get()); //create new page buffer
                    long buffPgNum = buffPg.getPageNum(); //new num for buffers
                    try { //exc
                        if (!dirtyPageTable.containsKey(buffPgNum) || dirtyPageTable.get(buffPgNum) > LSNRecord.LSN ||
                                buffPg.getPageLSN() >= LSNRecord.LSN) { //if dirty pages do not have key
                            boolRec = false; //set bool to false
                        }
                    } finally {
                        buffPg.unpin(); //unpin specific pages
                    }
                }
                if (boolRec) { //if true
                    LSNRecord.redo(this, diskSpaceManager, bufferManager); //go thorugh with redoing LSN
                }
                boolRec = true; //set to true
            }
        }
    }
    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement- complete
        PriorityQueue<Long> priQue = new PriorityQueue<>((Long v1, Long v2) -> v2.compareTo(v1)); //create pri queue
        for (TransactionTableEntry table: transactionTable.values()) { //loop through table
            priQue.offer(table.lastLSN); //sorted on last LSN for transactions
        }
        while (!priQue.isEmpty()) { //while the queue is still there
            long pollLSN = priQue.poll(); //create new LSN from queue
            LogRecord pollRecord = logManager.fetchLogRecord(pollLSN); //create new record from fetching record of LSN
            long pollTransNum = pollRecord.getTransNum().get(); //create new trans num from record
            TransactionTableEntry tbl = transactionTable.get(pollTransNum); //create new table from polling trans
            if (pollRecord.isUndoable()) { //if record is undoable
                LogRecord specCLR = pollRecord.undo(tbl.lastLSN); //undo the record
                logManager.appendToLog(specCLR); //append to appropriate CLR
                tbl.lastLSN = specCLR.LSN; //replace entry with new one
                specCLR.redo(this, diskSpaceManager, bufferManager); //redo the CLR specific
            }
            long pollUndoLSN = pollRecord.getUndoNextLSN().orElse(pollRecord.getPrevLSN().get()); //new LSN from above
            if (pollUndoLSN == 0) { //if LSN is 0
                end(pollTransNum); //end the transaction
            } else {
                priQue.offer(pollUndoLSN); //continue
            }
        }
    }


    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
