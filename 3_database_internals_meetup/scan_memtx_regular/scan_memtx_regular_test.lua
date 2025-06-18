box.cfg {
    memtx_memory = 96 *  1024 * 1024 * 1024,
    wal_mode = 'none',
}

local COLUMN_TO_AGGREGATE = 2

require('fiber').set_slice(1000000)

local clock = require('clock')
local start = clock.time64()

local sum = 0
for _, tuple in box.space.test:pairs() do
	sum = sum + tuple[COLUMN_TO_AGGREGATE]
end

local finish = clock.time64()
local diff = finish - start

print('Time: ' .. tonumber(diff) .. 'ns')
print('Elem per second: ' .. (box.space.test:len() / (tonumber(diff) / 1000000000.0)))
os.exit()
