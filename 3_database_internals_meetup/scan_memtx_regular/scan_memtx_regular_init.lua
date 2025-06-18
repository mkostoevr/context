box.cfg {
    memtx_memory = 96 *  1024 * 1024 * 1024,
    wal_mode = 'none',
}

local tuple_count = 10000000
local field_count = 1000

t = {0}
for i = 1, field_count do
    table.insert(t, 1)
end
function tuple(i)
    t[1] = i
    return t
end

require('fiber').set_slice(1000000)

local s = box.schema.create_space('test')
s:create_index('pk')

box.begin()
for i = 1, tuple_count do
    s:insert(tuple(i))
    if i % 1000 == 0 then
        box.commit()
        box.begin()
    end
end
box.commit()

box.snapshot()
