package database

import (
	"encoding/binary"
	"errors"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/pwrlabs/pwrgo/config/merkletree"
)

var (
	tree                 *merkletree.MerkleTree
	initOnce             sync.Once
	initErr              error
	lastCheckedBlockKey  = []byte("lastCheckedBlock")
	blockRootPrefix      = "blockRootHash_"
)

// DatabaseService provides singleton service for interacting with the underlying 
// BoltDB-backed MerkleTree. Provides methods for managing account balances, 
// transfers, block tracking, and Merkle root hash operations.
//
// This service maintains:
// - Account balances stored in the Merkle tree
// - Last checked block number for synchronization  
// - Historical block root hashes for validation
//
// The underlying MerkleTree is automatically closed on shutdown.
type DatabaseService struct{}

// initialize sets up the singleton MerkleTree instance with proper cleanup
func initialize() error {
	initOnce.Do(func() {
		var err error
		tree, err = merkletree.NewMerkleTree("database")
		if err != nil {
			initErr = err
			return
		}

		// Set up graceful shutdown
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			<-c
			if tree != nil {
				tree.Close()
			}
			os.Exit(0)
		}()
	})
	return initErr
}

// GetInstance returns the singleton DatabaseService instance
func GetInstance() (*DatabaseService, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	return &DatabaseService{}, nil
}

// GetRootHash returns the current Merkle root hash
func (ds *DatabaseService) GetRootHash() ([]byte, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	return tree.GetRootHash()
}

// GetRootHash returns the current Merkle root hash (static method)
func GetRootHash() ([]byte, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	return tree.GetRootHash()
}

// Flush pending writes to disk
func (ds *DatabaseService) Flush() error {
	if err := initialize(); err != nil {
		return err
	}
	return tree.FlushToDisk()
}

// Flush pending writes to disk (static method)
func Flush() error {
	if err := initialize(); err != nil {
		return err
	}
	return tree.FlushToDisk()
}

// RevertUnsavedChanges reverts all unsaved changes to the Merkle tree, 
// restoring it to the last flushed state. This is useful for rolling back 
// invalid transactions or when consensus validation fails.
func (ds *DatabaseService) RevertUnsavedChanges() error {
	if err := initialize(); err != nil {
		return err
	}
	return tree.RevertUnsavedChanges()
}

// RevertUnsavedChanges reverts all unsaved changes (static method)
func RevertUnsavedChanges() error {
	if err := initialize(); err != nil {
		return err
	}
	return tree.RevertUnsavedChanges()
}

// GetBalance retrieves the balance stored at the given address
//
// Parameters:
//   - address: 20-byte account address
//
// Returns:
//   - non-negative balance, zero if absent
func (ds *DatabaseService) GetBalance(address []byte) (*big.Int, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	if address == nil {
		return nil, errors.New("address must not be nil")
	}

	data, err := tree.GetData(address)
	if err != nil {
		return nil, err
	}
	
	if data == nil || len(data) == 0 {
		return big.NewInt(0), nil
	}
	
	balance := new(big.Int)
	balance.SetBytes(data)
	return balance, nil
}

// GetBalance retrieves the balance stored at the given address (static method)
func GetBalance(address []byte) (*big.Int, error) {
	if err := initialize(); err != nil {
		return nil, err
	}
	if address == nil {
		return nil, errors.New("address must not be nil")
	}

	data, err := tree.GetData(address)
	if err != nil {
		return nil, err
	}
	
	if data == nil || len(data) == 0 {
		return big.NewInt(0), nil
	}
	
	balance := new(big.Int)
	balance.SetBytes(data)
	return balance, nil
}

// SetBalance sets the balance for the given address
//
// Parameters:
//   - address: 20-byte account address
//   - balance: non-negative balance
func (ds *DatabaseService) SetBalance(address []byte, balance *big.Int) error {
	if err := initialize(); err != nil {
		return err
	}
	if address == nil {
		return errors.New("address must not be nil")
	}
	if balance == nil {
		return errors.New("balance must not be nil")
	}
	if balance.Sign() < 0 {
		return errors.New("balance must be non-negative")
	}

	return tree.AddOrUpdateData(address, balance.Bytes())
}

// SetBalance sets the balance for the given address (static method)
func SetBalance(address []byte, balance *big.Int) error {
	if err := initialize(); err != nil {
		return err
	}
	if address == nil {
		return errors.New("address must not be nil")
	}
	if balance == nil {
		return errors.New("balance must not be nil")
	}
	if balance.Sign() < 0 {
		return errors.New("balance must be non-negative")
	}

	return tree.AddOrUpdateData(address, balance.Bytes())
}

// Transfer transfers amount from sender to receiver
//
// Parameters:
//   - sender: sender address
//   - receiver: receiver address  
//   - amount: amount to transfer
//
// Returns:
//   - true if transfer succeeded, false on insufficient funds
func (ds *DatabaseService) Transfer(sender, receiver []byte, amount *big.Int) (bool, error) {
	if err := initialize(); err != nil {
		return false, err
	}
	if sender == nil {
		return false, errors.New("sender must not be nil")
	}
	if receiver == nil {
		return false, errors.New("receiver must not be nil")
	}
	if amount == nil {
		return false, errors.New("amount must not be nil")
	}
	if amount.Sign() < 0 {
		return false, errors.New("amount must be non-negative")
	}

	senderBalance, err := ds.GetBalance(sender)
	if err != nil {
		return false, err
	}

	if senderBalance.Cmp(amount) < 0 {
		return false, nil // Insufficient funds
	}

	newSenderBalance := new(big.Int).Sub(senderBalance, amount)
	if err := ds.SetBalance(sender, newSenderBalance); err != nil {
		return false, err
	}

	receiverBalance, err := ds.GetBalance(receiver)
	if err != nil {
		return false, err
	}

	newReceiverBalance := new(big.Int).Add(receiverBalance, amount)
	if err := ds.SetBalance(receiver, newReceiverBalance); err != nil {
		return false, err
	}

	return true, nil
}

// Transfer transfers amount from sender to receiver (static method)
func Transfer(sender, receiver []byte, amount *big.Int) (bool, error) {
	if err := initialize(); err != nil {
		return false, err
	}
	if sender == nil {
		return false, errors.New("sender must not be nil")
	}
	if receiver == nil {
		return false, errors.New("receiver must not be nil")
	}
	if amount == nil {
		return false, errors.New("amount must not be nil")
	}
	if amount.Sign() < 0 {
		return false, errors.New("amount must be non-negative")
	}

	senderBalance, err := GetBalance(sender)
	if err != nil {
		return false, err
	}

	if senderBalance.Cmp(amount) < 0 {
		return false, nil // Insufficient funds
	}

	newSenderBalance := new(big.Int).Sub(senderBalance, amount)
	if err := SetBalance(sender, newSenderBalance); err != nil {
		return false, err
	}

	receiverBalance, err := GetBalance(receiver)
	if err != nil {
		return false, err
	}

	newReceiverBalance := new(big.Int).Add(receiverBalance, amount)
	if err := SetBalance(receiver, newReceiverBalance); err != nil {
		return false, err
	}

	return true, nil
}

// GetLastCheckedBlock returns the last checked block number, or zero if unset
func (ds *DatabaseService) GetLastCheckedBlock() (int64, error) {
	if err := initialize(); err != nil {
		return 0, err
	}

	data, err := tree.GetData(lastCheckedBlockKey)
	if err != nil {
		return 0, err
	}

	if data == nil || len(data) < 8 {
		return 0, nil
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}

// GetLastCheckedBlock returns the last checked block number (static method)
func GetLastCheckedBlock() (int64, error) {
	if err := initialize(); err != nil {
		return 0, err
	}

	data, err := tree.GetData(lastCheckedBlockKey)
	if err != nil {
		return 0, err
	}

	if data == nil || len(data) < 8 {
		return 0, nil
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}

// SetLastCheckedBlock updates the last checked block number
//
// Parameters:
//   - blockNumber: non-negative block number
func (ds *DatabaseService) SetLastCheckedBlock(blockNumber int64) error {
	if err := initialize(); err != nil {
		return err
	}
	if blockNumber < 0 {
		return errors.New("block number must be non-negative")
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(blockNumber))
	return tree.AddOrUpdateData(lastCheckedBlockKey, buf)
}

// SetLastCheckedBlock updates the last checked block number (static method)
func SetLastCheckedBlock(blockNumber int64) error {
	if err := initialize(); err != nil {
		return err
	}
	if blockNumber < 0 {
		return errors.New("block number must be non-negative")
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(blockNumber))
	return tree.AddOrUpdateData(lastCheckedBlockKey, buf)
}

// SetBlockRootHash records the Merkle root hash for a specific block
//
// Parameters:
//   - blockNumber: block height
//   - rootHash: 32-byte Merkle root
func (ds *DatabaseService) SetBlockRootHash(blockNumber int64, rootHash []byte) error {
	if err := initialize(); err != nil {
		return err
	}
	if rootHash == nil {
		return errors.New("root hash must not be nil")
	}

	key := blockRootPrefix + strconv.FormatInt(blockNumber, 10)
	return tree.AddOrUpdateData([]byte(key), rootHash)
}

// SetBlockRootHash records the Merkle root hash for a specific block (static method)
func SetBlockRootHash(blockNumber int64, rootHash []byte) error {
	if err := initialize(); err != nil {
		return err
	}
	if rootHash == nil {
		return errors.New("root hash must not be nil")
	}

	key := blockRootPrefix + strconv.FormatInt(blockNumber, 10)
	return tree.AddOrUpdateData([]byte(key), rootHash)
}

// GetBlockRootHash retrieves the Merkle root hash for a specific block
//
// Parameters:
//   - blockNumber: block height
//
// Returns:
//   - 32-byte root hash, or nil if absent
func (ds *DatabaseService) GetBlockRootHash(blockNumber int64) ([]byte, error) {
	if err := initialize(); err != nil {
		return nil, err
	}

	key := blockRootPrefix + strconv.FormatInt(blockNumber, 10)
	return tree.GetData([]byte(key))
}

// GetBlockRootHash retrieves the Merkle root hash for a specific block (static method)
func GetBlockRootHash(blockNumber int64) ([]byte, error) {
	if err := initialize(); err != nil {
		return nil, err
	}

	key := blockRootPrefix + strconv.FormatInt(blockNumber, 10)
	return tree.GetData([]byte(key))
}

// Close closes the underlying MerkleTree (for manual cleanup)
func (ds *DatabaseService) Close() error {
	if tree != nil {
		return tree.Close()
	}
	return nil
}

// Close closes the underlying MerkleTree (static method)
func Close() error {
	if tree != nil {
		return tree.Close()
	}
	return nil
}