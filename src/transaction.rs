mod error;
mod manager;

use crate::{
    account::Account,
    context::TransactionContext,
    database::Database,
    node::TrieValue,
    overlay::OverlayState,
    path::{AddressPath, RawPath, StoragePath},
    storage::{overlay_root::OverlayedRoot, proofs::AccountProof},
};
use alloy_primitives::{map::HashMap, StorageValue, B256};
pub use error::TransactionError;
pub use manager::TransactionManager;
use sealed::sealed;
use std::{fmt::Debug, ops::Deref, sync::Arc};

#[sealed]
pub trait TransactionKind: Debug {}

#[derive(Debug)]
pub struct RW {}

#[sealed]
impl TransactionKind for RW {}

#[derive(Debug)]
pub struct RO {}

#[sealed]
impl TransactionKind for RO {}

// Compile-time assertion to ensure that `Transaction` is `Send`
const _: fn() = || {
    fn consumer<T: Send>() {}
    consumer::<Transaction<&Database, RO>>();
    consumer::<Transaction<&Database, RW>>();
    consumer::<Transaction<Arc<Database>, RO>>();
    consumer::<Transaction<Arc<Database>, RW>>();
};

#[derive(Debug)]
pub struct Transaction<DB, K: TransactionKind> {
    committed: bool,
    context: TransactionContext,
    database: DB,
    pending_changes: HashMap<RawPath, Option<TrieValue>>,
    _marker: std::marker::PhantomData<K>,
}

impl<DB: Deref<Target = Database>, K: TransactionKind> Transaction<DB, K> {
    pub(crate) fn new(context: TransactionContext, database: DB) -> Self {
        Self {
            committed: false,
            context,
            database,
            pending_changes: HashMap::default(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn get_account(
        &mut self,
        address_path: &AddressPath,
    ) -> Result<Option<Account>, TransactionError> {
        let account =
            self.database.storage_engine.get_account(&mut self.context, address_path).unwrap();
        self.database.update_metrics_ro(&self.context);
        Ok(account)
    }

    pub fn get_storage_slot(
        &mut self,
        storage_path: &StoragePath,
    ) -> Result<Option<StorageValue>, TransactionError> {
        let storage_slot =
            self.database.storage_engine.get_storage(&mut self.context, storage_path).unwrap();
        self.database.update_metrics_ro(&self.context);
        Ok(storage_slot)
    }

    pub fn state_root(&self) -> B256 {
        self.context.root_node_hash
    }

    pub fn compute_root_with_overlay(
        &self,
        overlay_state: OverlayState,
    ) -> Result<OverlayedRoot, TransactionError> {
        self.database
            .storage_engine
            .compute_state_root_with_overlay(&self.context, overlay_state)
            .map_err(|_| TransactionError)
    }

    pub fn get_account_with_proof(
        &self,
        address_path: AddressPath,
    ) -> Result<Option<AccountProof>, TransactionError> {
        let result = self
            .database
            .storage_engine
            .get_account_with_proof(&self.context, address_path)
            .unwrap();
        Ok(result)
    }

    pub fn get_storage_with_proof(
        &self,
        storage_path: StoragePath,
    ) -> Result<Option<AccountProof>, TransactionError> {
        let result = self
            .database
            .storage_engine
            .get_storage_with_proof(&self.context, storage_path)
            .unwrap();
        Ok(result)
    }

    pub fn clear_cache(&mut self) {
        self.context.clear_cache();
    }

    pub fn debug_account(
        &self,
        output_file: impl std::io::Write,
        address_path: AddressPath,
        verbosity_level: u8,
    ) -> Result<(), TransactionError> {
        self.database
            .storage_engine
            .debugger()
            .print_path(&self.context, &address_path.into(), output_file, verbosity_level)
            .unwrap();
        Ok(())
    }

    pub fn debug_storage(
        &self,
        output_file: Box<dyn std::io::Write>,
        storage_path: StoragePath,
        verbosity_level: u8,
    ) -> Result<(), TransactionError> {
        self.database
            .storage_engine
            .debugger()
            .print_path(&self.context, &storage_path.into(), output_file, verbosity_level)
            .unwrap();
        Ok(())
    }
}

impl<DB: Deref<Target = Database>> Transaction<DB, RW> {
    pub fn set_account(
        &mut self,
        address_path: AddressPath,
        account: Option<Account>,
    ) -> Result<(), TransactionError> {
        self.pending_changes.insert(address_path.into(), account.map(TrieValue::Account));
        Ok(())
    }

    pub fn set_storage_slot(
        &mut self,
        storage_path: StoragePath,
        value: Option<StorageValue>,
    ) -> Result<(), TransactionError> {
        self.pending_changes.insert(storage_path.into(), value.map(TrieValue::Storage));
        Ok(())
    }

    pub fn commit(mut self) -> Result<(), TransactionError> {
        let mut changes = self.pending_changes.drain().collect::<Vec<_>>();
        if !changes.is_empty() {
            self.database.storage_engine.set_values(&mut self.context, changes.as_mut()).unwrap();
        }

        let mut transaction_manager = self.database.transaction_manager.lock();
        self.database.storage_engine.commit(&self.context).unwrap();

        self.database.update_metrics_rw(&self.context);

        transaction_manager.remove_tx(self.context.snapshot_id, true);

        self.committed = true;
        Ok(())
    }

    pub fn rollback(mut self) -> Result<(), TransactionError> {
        let mut transaction_manager = self.database.transaction_manager.lock();
        transaction_manager.remove_tx(self.context.snapshot_id, true);

        self.committed = false;
        Ok(())
    }
}

impl<DB: Deref<Target = Database>> Transaction<DB, RO> {
    pub fn commit(mut self) -> Result<(), TransactionError> {
        let mut transaction_manager = self.database.transaction_manager.lock();
        transaction_manager.remove_tx(self.context.snapshot_id, false);

        self.committed = true;
        Ok(())
    }
}

impl<DB, K: TransactionKind> Drop for Transaction<DB, K> {
    fn drop(&mut self) {
        // TODO: panic if the transaction is not committed
    }
}

/// A transaction that starts without a write lock and can be upgraded to write mode.
///
/// This transaction type allows you to:
/// 1. Accumulate changes without holding a write lock
/// 2. Compute the state root hash (acquires write lock)
/// 3. Either commit (fast - just disk sync) or abort (fast - just reset metadata)
///
/// # Example
///
/// ```ignore
/// let mut tx = db.begin_upgradable();
/// tx.set_account(path, Some(account))?;
///
/// let hash = tx.compute_root()?;  // Write lock acquired here
/// if hash == expected_hash {
///     tx.commit()?;  // Fast - just disk sync
/// } else {
///     tx.abort()?;   // Fast - just reset metadata
/// }
/// ```
pub struct UpgradableTransaction<DB> {
    database: DB,
    pending_changes: HashMap<RawPath, Option<TrieValue>>,
    context: Option<TransactionContext>,
    computed: bool,
}

impl<DB> std::fmt::Debug for UpgradableTransaction<DB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpgradableTransaction")
            .field("pending_changes_count", &self.pending_changes.len())
            .field("computed", &self.computed)
            .finish_non_exhaustive()
    }
}

// Compile-time assertion to ensure that `UpgradableTransaction` is `Send`
const _: fn() = || {
    fn consumer<T: Send>() {}
    consumer::<UpgradableTransaction<&Database>>();
    consumer::<UpgradableTransaction<Arc<Database>>>();
};

impl<DB: Deref<Target = Database>> UpgradableTransaction<DB> {
    pub(crate) fn new(database: DB) -> Self {
        Self { database, pending_changes: HashMap::default(), context: None, computed: false }
    }

    /// Accumulates an account change. No write lock is held.
    ///
    /// Returns an error if `compute_root()` has already been called.
    pub fn set_account(
        &mut self,
        address_path: AddressPath,
        account: Option<Account>,
    ) -> Result<(), TransactionError> {
        if self.computed {
            return Err(TransactionError);
        }
        self.pending_changes.insert(address_path.into(), account.map(TrieValue::Account));
        Ok(())
    }

    /// Accumulates a storage slot change. No write lock is held.
    ///
    /// Returns an error if `compute_root()` has already been called.
    pub fn set_storage_slot(
        &mut self,
        storage_path: StoragePath,
        value: Option<StorageValue>,
    ) -> Result<(), TransactionError> {
        if self.computed {
            return Err(TransactionError);
        }
        self.pending_changes.insert(storage_path.into(), value.map(TrieValue::Storage));
        Ok(())
    }

    /// Reads an account from the database. Uses a read context (no write lock).
    pub fn get_account(
        &mut self,
        address_path: &AddressPath,
    ) -> Result<Option<Account>, TransactionError> {
        let mut context = self.database.storage_engine.read_context();
        self.database
            .storage_engine
            .get_account(&mut context, address_path)
            .map_err(|_| TransactionError)
    }

    /// Reads a storage slot from the database. Uses a read context (no write lock).
    pub fn get_storage_slot(
        &mut self,
        storage_path: &StoragePath,
    ) -> Result<Option<StorageValue>, TransactionError> {
        let mut context = self.database.storage_engine.read_context();
        self.database
            .storage_engine
            .get_storage(&mut context, storage_path)
            .map_err(|_| TransactionError)
    }

    /// Returns the current state root (before any pending changes are applied).
    pub fn state_root(&self) -> B256 {
        if let Some(ref context) = self.context {
            context.root_node_hash
        } else {
            self.database.storage_engine.read_context().root_node_hash
        }
    }

    /// Acquires the write lock, applies all pending changes, and returns the computed state root.
    ///
    /// After calling this method, you must call either `commit()` or `abort()`.
    /// This method can only be called once per transaction.
    ///
    /// # Performance
    /// - Acquires write lock
    /// - Applies all changes (trie traversal, RLP encoding, keccak256 hashing)
    /// - Writes pages to memory-mapped buffer (no disk sync yet)
    pub fn compute_root(&mut self) -> Result<B256, TransactionError> {
        if self.computed {
            // Already computed, just return the cached hash
            return Ok(self.context.as_ref().unwrap().root_node_hash);
        }

        // Acquire write lock
        let mut context = self.database.storage_engine.write_context();
        let min_snapshot_id =
            self.database.transaction_manager.lock().begin_rw(context.snapshot_id)?;
        if min_snapshot_id > 0 {
            self.database.storage_engine.unlock(min_snapshot_id - 1);
        }

        // Apply all changes (this computes the hash)
        let mut changes = self.pending_changes.drain().collect::<Vec<_>>();
        if !changes.is_empty() {
            self.database
                .storage_engine
                .set_values(&mut context, changes.as_mut())
                .map_err(|_| TransactionError)?;
        }

        let root_hash = context.root_node_hash;
        self.context = Some(context);
        self.computed = true;

        Ok(root_hash)
    }

    /// Persists the changes to disk. Must call `compute_root()` first.
    ///
    /// # Performance
    /// - Just syncs pages to disk (no hash recomputation)
    /// - Updates and syncs the root page
    pub fn commit(self) -> Result<(), TransactionError> {
        if !self.computed {
            return Err(TransactionError);
        }

        let context = self.context.unwrap();

        // Just persist - no recomputation needed
        let mut transaction_manager = self.database.transaction_manager.lock();
        self.database.storage_engine.commit(&context).map_err(|_| TransactionError)?;

        self.database.update_metrics_rw(&context);
        transaction_manager.remove_tx(context.snapshot_id, true);

        Ok(())
    }

    /// Discards all changes and releases the write lock.
    ///
    /// If `compute_root()` was not called, this is a no-op.
    /// If `compute_root()` was called, this resets the dirty metadata slot.
    ///
    /// # Performance
    /// - No disk I/O
    /// - Just resets metadata (~64 bytes)
    pub fn abort(self) -> Result<(), TransactionError> {
        if !self.computed {
            // Nothing to abort - no write lock was acquired
            return Ok(());
        }

        let context = self.context.unwrap();

        // Reset dirty metadata slot
        self.database.storage_engine.reset_dirty_slot();

        // Release the write lock
        let mut transaction_manager = self.database.transaction_manager.lock();
        transaction_manager.remove_tx(context.snapshot_id, true);

        Ok(())
    }
}
