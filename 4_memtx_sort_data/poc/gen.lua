box.cfg {
    memtx_memory = 12 * 1024 * 1024 * 1024,
    memtx_use_sort_data = false,
}

local count = tonumber(arg[1])
local tx_count = 100000

require('fiber').set_max_slice(1000000)

-- Space 1

local s = box.schema.space.create('s')
s:create_index('pk')
s:create_index('i1', {parts = {{2, 'unsigned'}}, unique = false})
s:create_index('i2', {parts = {{2, 'unsigned', sort_order = 'desc'}}, unique = false})

box.begin()
for i = 1, count do
    s:insert({i, math.random(0, 1000000000)})
    if i % tx_count == 0 then
        box.commit()
        box.begin()
    end
end
box.commit()

-- Space 2

local s2 = box.schema.space.create('s2')
s2:create_index('pk')
s2:create_index('i1', {parts = {{2, 'unsigned'}}, unique = false})
s2:create_index('i2', {parts = {{2, 'unsigned', sort_order = 'desc'}}, unique = false})

box.begin()
for i = 1, count do
    s2:insert({i, math.random(0, 1000000000)})
    if i % tx_count == 0 then
        box.commit()
        box.begin()
    end
end
box.commit()

box.snapshot()

os.exit()
