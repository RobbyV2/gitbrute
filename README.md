# gitbrute

A high-performance Rust tool to brute-force git commit hashes to match a specific prefix or regex pattern by manipulating timestamps.

## Usage

```bash
# Match a specific prefix
gitbrute --prefix 12345

# Match a regex pattern
gitbrute --pattern "^00+7"

# Use specific number of threads (default: all CPUs)
gitbrute --prefix abc --cpus 4
```
