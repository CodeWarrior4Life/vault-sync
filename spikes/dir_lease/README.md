# dir_lease spike (throwaway, not for merge)

Tests the premise: the sync-state lease is an exclusive flock on the state DIRECTORY fd, and all
writes go through that fd (openat/renameat), so move+recreate cannot redirect the holder's writes
and rm -rf makes them fail with ENOENT instead of resurrecting the tree.

    cd spikes/dir_lease && cargo run --release

Scratch dirs are created under /var/tmp (override with SPIKE_BASE=/some/real/fs/dir) and removed
at the end. Never point it at a real sync-state dir.
