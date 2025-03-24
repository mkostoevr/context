--[[

Results:

Ping: 156373ns
Call #1: 80606ns
Call #2: 54082ns

Ping: 123254ns
Call #1: 78435ns
Call #2: 54457ns

Ping: 282033ns
Call #1: 92558ns
Call #2: 57559ns

--]]

box.cfg{
    listen = 3301,
    memtx_memory = 1024 * 1024 * 1024 * 8,
    net_msg_max = 2000000000,
    readahead = 2000000000,
}

s = box.schema.space.create('s')
s:create_index('pk')

function bench_call() end
function bench_insert(id) s:insert({id}) end
function bench_replace(id) s:replace({id}) end
function bench_select(id) s:select({id}) end
function bench_delete(id) s:delete({id}) end

box.schema.user.grant('guest','read,write,execute,create,drop','universe')
