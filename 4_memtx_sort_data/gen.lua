local snapc = arg[1]
local walc = arg[2]
local skc = arg[3]
local usehint = arg[4] == 'true'
local mk = arg[4] == 'mk'

function tuple(i)
    local t
    if mk then
        t = {}
    else
        t = {i}
    end
    for i = 1, skc do
        table.insert(t, math.random(1, 1000000000))
    end
    table.insert(t, math.random(1, 1000000000))
    table.insert(t, math.random(1, 1000000000))
    table.insert(t, math.random(1, 1000000000))
    table.insert(t, math.random(1, 1000000000))
    if mk then
        return {i, {data = t}}
    else
        return t
    end
end

box.cfg{
    memtx_use_sort_data = true,
    memtx_memory = 12 * 1024 * 1024 * 1024,
    wal_mode = tonumber(walc) == 0 and 'none' or nil,
}

require('fiber').set_slice(1000000)

local s = box.schema.create_space('test')
s:create_index('i0', {hint = usehint})

for i = 1, skc do
    if mk then
        s:create_index('i' .. i, {parts = {{field = i + 1, type = 'unsigned', path = 'data[*]'}}, unique = false})
    else
        s:create_index('i' .. i, {parts = {{field = i + 1, type = 'unsigned'}}, unique = false, hint = usehint})
    end
end

for i = 1, snapc do
    s:insert(tuple(i))
end
box.snapshot()

for i = 1, walc do
    s:insert(tuple(snapc + i))
end
os.exit()

local snapc = arg[1]
local walc = arg[2]
local skc = arg[3]

box.cfg{
    wal_mode = tonumber(walc) == 0 and 'none' or nil,
    memtx_memory = 64 * 1024 * 1024 * 1024,
}

require('fiber').set_slice(1000000)

local s = box.schema.create_space('test')
s:create_index('i0')

for i = 1, skc do
    s:create_index('i' .. i, {parts = {i + 1, 'unsigned'}, unique = false})
end

s.stat(s.id, snapc, skc + 1)
box.snapshot()

s.stat(s.id, walc, skc + 1)
os.exit()
