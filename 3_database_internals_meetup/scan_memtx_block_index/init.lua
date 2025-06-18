box.cfg {
    memtx_memory = 96 *  1024 * 1024 * 1024,
    wal_mode = 'none',
}

local tuple_count = 1024 * 1024 * 1024

t = {0, require('varbinary').new(string.rep("\x01\x00\x00\x00\x00\x00\x00\x00", 8192))}
function tuple(i)
    t[1] = i
    return t
end

require('fiber').set_slice(1000000)

local s = box.schema.create_space('test')
s:create_index('pk')

for i = 1, tuple_count / 8192 do
    s:insert(tuple(i * 8192))
end

box.snapshot()
os.exit()
