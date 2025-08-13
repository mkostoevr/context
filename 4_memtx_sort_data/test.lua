local thread_count = tonumber(arg[1])
if thread_count == -1 then
    thread_count = nil
    memtx_use_sort_data = true
end

box.cfg {
    memtx_memory = 14 * 1024 * 1024 * 1024,
    memtx_sort_threads = thread_count,
    memtx_use_sort_data = memtx_use_sort_data
}

--[[
require('fiber').set_slice(1000000)
for _, tuple in box.space.test.index.i1:pairs() do
    print(require('yaml').encode(tuple))
end
--]]

print('Sort data:', memtx_use_sort_data)
print('thread_count:', arg[1])
for i, o in ipairs(box.space.test.index) do
    print('i' .. i .. ' count: ' .. o:len())
end

os.exit()
