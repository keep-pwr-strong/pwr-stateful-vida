package dbservice

import (
    "encoding/binary"
    "math/big"
    "sync"

    "github.com/pwrlabs/pwrgo/config/merkletree"
)

var (
    tree                *merkletree.MerkleTree
    initOnce            sync.Once
    lastCheckedBlockKey = []byte("lastCheckedBlock")
    blockRootPrefix     = "blockRootHash_"
)

// initialize sets up the singleton MerkleTree instance
func initialize() {
    initOnce.Do(func() {
        tree, _ = merkletree.NewMerkleTree("database")
    })
}

// GetRootHash returns the current Merkle root hash
func GetRootHash() ([]byte, error) {
    initialize()
    return tree.GetRootHash()
}

// Flush pending writes to disk
func Flush() error {
    initialize()
    return tree.FlushToDisk()
}

// RevertUnsavedChanges reverts all unsaved changes
func RevertUnsavedChanges() error {
    initialize()
    return tree.RevertUnsavedChanges()
}

// GetBalance retrieves the balance stored at the given address
func GetBalance(address []byte) (*big.Int, error) {
    initialize()
    if address == nil {
        return big.NewInt(0), nil
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
func SetBalance(address []byte, balance *big.Int) error {
    initialize()
    if address == nil || balance == nil {
        return nil
    }

    return tree.AddOrUpdateData(address, balance.Bytes())
}

// Transfer transfers amount from sender to receiver
func Transfer(sender, receiver []byte, amount *big.Int) (bool, error) {
    initialize()
    if sender == nil || receiver == nil || amount == nil {
        return false, nil
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

    receiverBalance, _ := GetBalance(receiver)
    newReceiverBalance := new(big.Int).Add(receiverBalance, amount)
    if err := SetBalance(receiver, newReceiverBalance); err != nil {
        return false, err
    }

    return true, nil
}

// GetLastCheckedBlock returns the last checked block number
func GetLastCheckedBlock() (int64, error) {
    initialize()
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
func SetLastCheckedBlock(blockNumber int) error {
    initialize()
    blockBytes := make([]byte, 8)
    binary.BigEndian.PutUint64(blockBytes, uint64(blockNumber))
    return tree.AddOrUpdateData(lastCheckedBlockKey, blockBytes)
}

// SetBlockRootHash records the Merkle root hash for a specific block
func SetBlockRootHash(blockNumber int, rootHash []byte) error {
    initialize()
    if rootHash == nil {
        return nil
    }

    key := []byte(blockRootPrefix + string(rune(blockNumber)))
    return tree.AddOrUpdateData(key, rootHash)
}

// GetBlockRootHash retrieves the Merkle root hash for a specific block
func GetBlockRootHash(blockNumber int64) ([]byte, error) {
    initialize()
    key := []byte(blockRootPrefix + string(rune(blockNumber)))
    return tree.GetData(key)
}

// Close explicitly closes the DatabaseService
func Close() error {
    if tree != nil {
        return tree.Close()
    }
    return nil
}
