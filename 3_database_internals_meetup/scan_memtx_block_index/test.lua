box.cfg {
    memtx_memory = 96 *  1024 * 1024 * 1024,
    wal_mode = 'none',
    listen = 3306
}

capi_connection = require('net.box'):new(3306)
box.schema.user.grant('guest','read,write,execute,create,drop','universe')
box.schema.func.create('test_module', {language = "C"})

require('fiber').set_slice(1000000)
local clock = require('clock')
local start = clock.time64()

capi_connection:call('test_module', {box.space.test.id, 0})

local finish = clock.time64()
local diff = finish - start

print('Time: ' .. tonumber(diff) .. 'ns')
print('Elem per second: ' .. (box.space.test:len() * 8192 / (tonumber(diff) / 1000000000.0)))
os.exit()

