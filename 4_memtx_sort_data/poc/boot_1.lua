local clock = require('clock')

-- First load without the sort data enabled.
local before_cfg = clock.time()
box.cfg{
    memtx_memory = 12 * 1024 * 1024 * 1024,
    memtx_use_sort_data = false
}
local after_cfg = clock.time()
local cfg_time = after_cfg - before_cfg

-- No-op to update the VClock.
box.space._space:alter({})

-- Enable the memtx sort data and create a snapshot.
box.cfg{ memtx_use_sort_data = true }
local before_snapshot = clock.time()
box.snapshot()
local after_snapshot = clock.time()
local snapshot_time = after_snapshot - before_snapshot

-- Print stats and exit.
print('Snapshot read time (without sort data): ' .. cfg_time .. 's')
print('Snapshot write time (with sort data): ' .. snapshot_time .. 's')
os.exit()
