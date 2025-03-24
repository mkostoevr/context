#!/usr/bin/env tarantool

--[[

Lua function:

First: 129198
Max: 1417035
Avg: 23776
Min: 18136

First: 71426
Max: 4177521
Avg: 23937
Min: 17900

Ping: 105624ns
Call #1: 76424ns
Call #2: 52055ns

Ping: 76524ns
Call #1: 74745ns
Call #2: 52083ns

C function:

First: 641078
Max: 1870439
Avg: 26234
Min: 19947

Ping: 135287ns
Call #1: 721694ns
Call #2: 159243ns

Ping: 192914ns
Call #1: 93439ns
Call #2: 56348ns

---

Ping: 75442ns
Call #1: 651569ns
Call #2: 124732ns

Ping: 74715ns
Call #1: 89715ns
Call #2: 54301ns

--]]

box.cfg{
    listen = '127.0.0.1:3301',
    memtx_memory = 1 * 2^30,
}

box.schema.user.grant('guest','read,write,execute,create,drop','universe')

local log = require("log")
local fiber = require("fiber")

clock = require('clock')

fiber.set_max_slice{warn = 1.5, err = 10}

-- create data schema
box.atomic(function ()
    format = {
        { 'id', 'string' },
        { 'client_id', 'string' },
        { 'pan', 'string' },
        { 'ts', 'int64' },
        { 'msg_type', 'int64' },
        { 'mcc', 'string' },
        { 'de22', 'string' },
        { 'de3', 'string' },
        { 'mti', 'string' },
        { 'response', 'string' },
        { 'country', 'string' },
        { 'msg_mode', 'string' },
        { 'amount', 'int64' },
    }

    local sp = box.schema.create_space('test', {
        engine = 'memcs',
        field_count = #format,
        format = format,
        if_not_exists = true,
    })

    sp:create_index('pk', {
        parts = { 'id' },
        if_not_exists = true,
        unique = true
    })

    local covers = {}
    for i=2,#format do
        table.insert(covers, i)
    end

    sp:create_index('client_id', {
        parts = { 'client_id' },
        if_not_exists = true,
        unique = false,
        covers = covers
    })

    sp:create_index('pan', {
        parts = { 'pan' },
        if_not_exists = true,
        unique = false,
        covers = covers
    })
end)

function generate_rows(n, p, match_prob)
    p = p or 10
    match_prob = match_prob or 0.01

    -- inserts n randomly generated rows
    -- into space 'sp'

    message_types = {10, 20, 30, 40, 50, 60, 70, 80, 90}
    mti = {"0300", "0310", "0320", "0330", "0400", "0410", "0420", "0430"}
    de22 = {"81", "01", "10", "82", "61", "16", "62"}
    de3 = {"100", "200", "300", "400", "500", "600", "700"}
    --           Беларусь  Куба   Россия  Турция
    countries = {"112",    "192", "643",  "792"}
    msg_modes = {"P", "T", "C", "D", "L", "A", "X"}

    box.space.test:truncate()

    local should_match_rows = 0

    for i = 1, n do
        local prob = math.random()
        local should_match = prob > (1 - match_prob);

        if should_match then
            should_match_rows = should_match_rows + 1

            box.space.test:insert{
                tostring(i),                -- id
                'some client '..i%p,        -- client_id
                'some pan '..i%p,           -- pan_id
                os.time() + 100000,         -- ts
                50,                         -- msg_type
                '5921',                     -- mcc
                '10',                       -- de22
                '999',                      -- de3
                '0999',                     -- mti
                '00',                       -- response
                '643',                      -- country
                '100',                      -- msg_mode
                5                           -- amount
            }
        else
            box.space.test:insert{
                tostring(i),
                'some client '..i%p,
                'some pan '..i%p,
                os.time() + math.random(-100000, 0),
                message_types[math.random(1, #message_types)],
                tostring(1),
                tostring(1),
                tostring(de3[math.random(1, #de3)]),
                tostring(mti[math.random(1, #mti)]),
                'some response',
                tostring(countries[math.random(1, #countries)]),
                tostring(msg_modes[math.random(1, #msg_modes)]),
                math.random(0, 10000),
            }
        end

    end

    log.info(('should match %d'):format(should_match_rows))
end


function register_func(f_name)

    local full_f_name = 'procs.'..f_name
    box.schema.func.create(full_f_name, {language = 'C', if_not_exists = true})

    rawset(_G, f_name, function (...) return box.func[full_f_name]:call{...} end)

    rawset(_G, 'time_'..f_name, function (...)
        beg = clock.monotonic64()
        local ret = box.func[full_f_name]:call{...}
        dur_ns = clock.monotonic64() - beg

        return { ret=ret, dur_ns=dur_ns }
    end)

    rawset(_G, 'bench_'..f_name, function (proc_args, opts)
        log.info(proc_args)

        opts = opts or {}
        opts.iters = opts.iters or 1000
        opts.yield_every = opts.yield_every or 10

        local reqs = 0
        local timings_sum_ns = 0
        local timings_count = 0

        local time_delta = 1

        local prog_printer_fib = fiber.create(function ()
            fiber.self():name("progress_reporter")

            while true do
                fiber.sleep(time_delta)

                if timings_count <= 0 then
                    goto continue
                end

                rps = reqs / time_delta
                reqs = 0

                local avg_lat_ns = tonumber(timings_sum_ns / timings_count)
                local avg_lat_ms = tonumber(avg_lat_ns / 1000000)
                timings_sum_ns = 0
                timings_count = 0

                log.info('rps: %d, latency (avg): %.02f ns = %.02f ms', rps, avg_lat_ns, avg_lat_ms)

                ::continue::
            end

        end)

        for i=0,opts.iters do
            local beg = clock.monotonic64()
            local ret = box.func[full_f_name]:call({proc_args})
            local dur_ns = clock.monotonic64() - beg

            timings_sum_ns = timings_sum_ns + dur_ns
            timings_count = timings_count + 1
            reqs = reqs + 1

            if i % opts.yield_every == 0 then
                fiber.yield()
            end
        end

        prog_printer_fib:cancel()

        return { ret=ret, dur_ns=dur_ns }
    end)
end

-- expose functions from 'procs' module
register_func('i_payment_after_drinking')
register_func('counter_1')
register_func('counter_2')

function lua_func()
    return
end

rawset(_G, 'sp', box.space.test)
rawset(_G, 'procs', procs)
