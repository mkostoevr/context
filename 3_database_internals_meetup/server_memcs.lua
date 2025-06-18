box.cfg {
    memtx_memory = 96 * 1024 * 1024 * 1024,
    net_msg_max = 1000,
    readahead = 1024 * 1024 * 1024,
    listen = 3301, -- "127.0.0.1:3301"
}

box.schema.user.grant('guest','read,write,execute,create,drop','universe')

box.schema.space

function bench_func(a, b)
    return
end
