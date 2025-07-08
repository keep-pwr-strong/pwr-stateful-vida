# Python Merkle Tree Implementation

This is a Python conversion of the Java Merkle Tree implementation, maintaining full API compatibility and feature parity while adapting to Python's ecosystem.

## Features

- **Complete API Compatibility**: All major methods from the Java version
- **Thread Safety**: Concurrent operations using `threading.RLock`
- **Persistent Storage**: File-based storage with automatic serialization
- **Memory Management**: In-memory caching with proper cleanup
- **Complex Tree Logic**: Full hanging nodes algorithm for efficient tree construction
- **Data Integrity**: SHA-256 hashing and proper tree structure maintenance

## Quick Start

```python
from utils.merkle_tree import MerkleTree

# Create a new tree
with MerkleTree("my_tree") as tree:
    # Add some data
    tree.add_or_update_data(b"key1", b"value1")
    tree.add_or_update_data(b"key2", b"value2")
    
    # Retrieve data
    data = tree.get_data(b"key1")
    print(f"Retrieved: {data}")
    
    # Get tree info
    print(f"Leaves: {tree.get_num_leaves()}")
    print(f"Root hash: {tree.get_root_hash().hex()}")
    
    # Persist to disk
    tree.flush_to_disk()
```

## API Reference

### Core Methods

- `add_or_update_data(key: bytes, data: bytes)` - Add or update key-value data
- `get_data(key: bytes) -> Optional[bytes]` - Retrieve data for a key
- `contains_key(key: bytes) -> bool` - Check if key exists
- `get_root_hash() -> Optional[bytes]` - Get current root hash
- `get_num_leaves() -> int` - Get number of leaves
- `get_depth() -> int` - Get tree depth

### Management Methods

- `flush_to_disk()` - Persist changes to storage
- `revert_unsaved_changes()` - Revert uncommitted changes
- `clear()` - Clear all data
- `close()` - Close the tree

## Architecture

### Storage Backend
- **File-based storage** using Python's `pickle` module
- **Three storage files**: metadata, nodes, and key-data
- **Atomic operations** with proper error handling

### Threading Model
- **Thread-safe operations** using `threading.RLock`
- **Concurrent access** support for multiple readers/writers
- **Proper locking** for cache and storage operations

### Memory Management
- **In-memory caching** for frequently accessed nodes
- **Lazy loading** from disk when needed
- **Cache cleanup** on flush operations

## Technical Adaptations from Java

| Java Feature | Python Implementation |
|--------------|----------------------|
| `byte[]` | `bytes` |
| `ByteBuffer` | `struct` module |
| `ReadWriteLock` | `threading.RLock` |
| `RocksDB` | File-based storage with `pickle` |
| `PWRHash.hash256()` | `hashlib.sha256()` |
| Inner classes | Nested classes |
| Exception types | Python exception hierarchy |

## Testing

Run the test suite:
```bash
python test_merkle_tree.py
```

Run the demo:
```bash
python demo.py
```

## Performance Characteristics

- **O(log n)** for most operations due to tree structure
- **Efficient batch operations** with hanging nodes algorithm
- **Minimal memory footprint** with caching strategy
- **Fast persistence** with pickle serialization

## Thread Safety

The implementation is fully thread-safe:
- All public methods acquire appropriate locks
- Cache operations are atomic
- Storage operations use file locking
- No race conditions in tree structure modifications

## Error Handling

- **Graceful degradation** for missing files
- **Proper exception handling** for I/O operations
- **Data validation** for all inputs
- **Rollback capability** for failed operations

## Limitations

- **File-based storage** instead of RocksDB (due to compilation issues)
- **Python GIL** may limit true parallelism
- **Pickle serialization** is Python-specific (not cross-language)

## Future Improvements

- Add RocksDB support when compilation issues are resolved
- Implement compression for storage efficiency
- Add more comprehensive logging
- Performance optimizations for large datasets 