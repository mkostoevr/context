-- Test runner options.
local use_perf = true -- run `perf record` on the tests.
local repetition_count = 1

-- Test config-specific options.
local options = arg[1] ~= nil and require(arg[1]) or {}

local function non_nil(value, otherwise)
    if value ~= nil then
        return value
    else
        return otherwise
    end
end

-- General options.
local space_size = non_nil(options.space_size, 3000000)
local space_engine = non_nil(options.engine, 'memcs')

-- User PoV tuning options.
local in_one_transaction = non_nil(options.in_one_transaction, true)
local batch_size = non_nil(options.batch_size, 1000)
local wal_mode = non_nil(options.wal_mode, 'write')

-- The indexes used to delete/update/select.
local search_index_name = non_nil(options.search_index_name, 'pk')
local write_index_name = non_nil(options.write_index_name, 'pk')

-- Lookup options.
local from_key_offset = non_nil(options.from_key_offset, -1 --[[ empty key. ]])
local from_key_part_count = non_nil(options.from_key_part_count, 1)
local until_key_offset = non_nil(options.until_key_offset, space_size * 0.9)
local until_key_part_count = non_nil(options.until_key_part_count, 1)

-- The data schema.
local format = options.format or {
    {name = 'id', type = 'unsigned', generator = {name = 'incrementing'}}, -- 123456789
--    {name = 'non_unique', type = 'unsigned', generator = {name = 'random', min = 1, max = 100}}, -- 887241132
--    {name = 'long_step', type = 'unsigned', generator = {name = 'long_step', step_size = 100}}, -- 111222333
--    {name = 'repeating', type = 'unsigned', generator = {name = 'repeating', steps = 100}}, -- 123123123
--    {name = 'unique', type = 'unsigned', generator = {name = 'random_unique'}}, -- 975642138
    {count = 100, name = 'extra%i', type = 'unsigned', generator = {name = 'incrementing'}},
}

local indexes = options.indexes or {
    {name = 'pk', opts = {parts = {{'id', 'unsigned'}}, unique = true}},
--    {name = 'non_unique', opts = {parts = {{'non_unique', 'unsigned'}}, unique = false}},
--    {name = 'long_step', opts = {parts = {{'long_step', 'unsigned'}}, unique = false}},
--    {name = 'repeating', opts = {parts = {{'repeating', 'unsigned'}}, unique = false}},
--    {name = 'multipart', opts = {parts = {{'long_step', 'unsigned'}, {'repeating', 'unsigned'}}, unique = true}},
--    {name = 'unique', opts = {parts = {{'unique', 'unsigned'}}, unique = true}},
}

local field_by_name = {}
for fieldno, field in pairs(format) do
    field_by_name[field.name] = field
end
local index_by_name = {}
for _, index in pairs(indexes) do
    index_by_name[index.name] = index
end
if index_by_name[search_index_name] == nil then
    print('unexisting search_index_name')
end
if index_by_name[write_index_name] == nil then
    print('unexisting write_index_name')
end

-- Save the original format to print it out.
local orig_format = {}
for fieldno, field in pairs(format) do
    orig_format[fieldno] = field
end

-- Find template fields.
local template_fields = {}
for fieldno, field in pairs(format) do
    if string.find(field.name, "%%i") or field.count ~= nil then
        -- Template fields must have count specified and %i in format.
        assert(field.count ~= nil)
        assert(string.find(field.name, "%%i"))
        template_fields[fieldno] = field
    end
end

-- Drop the template fields to insert real ones later.
for fieldno, _ in pairs(template_fields) do
    format[fieldno] = nil
end

-- Generate real fields from templates.
for _, template in pairs(template_fields) do
    for i = 1, template.count do
        local field = {}
        for k, v in pairs(template) do
            if k == 'name' then
                -- Generate a real field name from the template.
                field.name = string.format(template.name, i)
            elseif k == 'count' then
                -- Skip the 'count' template property.
            else
                field[k] = v
            end
        end
        table.insert(format, field)
    end
end

-- Data to be filled with must be specified for each field.
local function incrementing(i)
    if i == -1 then
        return 'incrementing'
    end
    return i
end
local function long_step(step_size)
    return function(i)
        if i == -1 then
            return 'long, steps: ' .. step_size
        end
        return math.floor(i / step_size)
    end
end
local function repeating(steps)
    return function(i)
        if i == -1 then
            return 'repeating, steps: ' .. steps
        end
        return i % steps
    end
end
local function random(min, max)
    local values = {}
    for i = 1, space_size do
        values[i] = math.random(min, max)
    end
    return function(i)
        if i == -1 then
            return 'random from ' .. min .. ' to ' .. max
        end
        return values[i]
    end
end
local random_unique_values = {}
for i = 1, space_size do
    random_unique_values[i] = i
end
for i = #random_unique_values, 2, -1 do
    local j = math.random(i)
    random_unique_values[i], random_unique_values[j] = random_unique_values[j], random_unique_values[i]
end
local function random_unique(i)
    if i == -1 then
        return 'random unique'
    end
    return random_unique_values[i]
end

local gen_field_value = {}
for fieldno, field in pairs(format) do
    if field.generator.name == 'incrementing' then
        -- 123456789
        gen_field_value[fieldno] = incrementing
    elseif field.generator.name == 'random' then
        -- 887241132
        gen_field_value[fieldno] = random(field.generator.min,
                                          field.generator.max)
    elseif field.generator.name == 'long_step' then
        -- 111222333
        gen_field_value[fieldno] = long_step(field.generator.step_size)
    elseif field.generator.name == 'repeating' then
        -- 123123123
        gen_field_value[fieldno] = repeating(field.generator.steps)
    else
        -- 975642138
        assert(field.generator.name == 'random_unique')
        gen_field_value[fieldno] = random_unique
    end
    format[fieldno].generator = nil
end
assert(#gen_field_value == #format)

-- A machinery to run perf right once a test has started.
local ffi = require('ffi')
ffi.cdef([[
    pid_t fork(void);
    int execve(const char *pathname, char *const argv[], char *const envp[]);
    int kill(pid_t pid, int sig);
]])

local function to_const_char(input)
    local result = ffi.new('char const*[?]', #input + 1, input)
    result[#input] = nil
    return ffi.cast('char *const*', result)
end

local perf_pid = -1
local function perf_run()
    local args = {'/usr/bin/bash', '-c',
                  'perf record --call-graph dwarf,65528 -F 500 -p ' .. box.info.pid .. ' -o perf.data'}
    local env = {}
    local env_list = require('fun').iter(env):map(function(k, v) return k .. '=' .. v end):totable()
    perf_pid = ffi.C.fork()
    if perf_pid == -1 then
        error('fork failed: ' .. perf_pid)
    elseif perf_pid > 0 then
        return
    end
    local argv = to_const_char(args)
    local envp = to_const_char(env_list)
    ffi.C.execve('/usr/bin/bash', argv, envp)
    io.stderr:write('\n\nperf execve failed\n\n' .. require('errno').strerror())
    os.exit(1)
end

local function perf_stop()
    assert(perf_pid > 0)
    local SIGSTOP = 19
    ffi.C.kill(perf_pid, SIGSTOP)
end

local function perf_resume()
    assert(perf_pid > 0)
    local SIGCONT = 18
    ffi.C.kill(perf_pid, SIGCONT)
end

local function perf_kill()
    assert(perf_pid > 0)
    local SIGCONT = 18
    local SIGTERM = 15
    local ESRCH = 3

    -- Continue its excution after perf_stop.
    ffi.C.kill(perf_pid, SIGCONT)

    -- Shut it down gracefully.
    ffi.C.kill(perf_pid, SIGTERM)

    -- Wait till the shutdown happens.
    local fiber = require('fiber')
    local errno = require('errno')
    while errno() ~= ESRCH do
        fiber.sleep(0.25)
        ffi.C.kill(perf_pid, 0)
    end
end

local function perf_start()
    if perf_pid == -1 then
        perf_run()
    else
        perf_resume()
    end
end

local fio = require('fio')
local clock = require('clock')
local fiber = require('fiber')
local key_def = require('key_def')
local alloc = require('internal.alloc')

local function log(s)
    io.stdout:write(s)
    io.stdout:flush()
end

local function clear()
    for _, file in pairs(fio.glob('./000*.snap')) do
        fio.unlink(file)
    end
    for _, file in pairs(fio.glob('./000*.xlog')) do
        fio.unlink(file)
    end
    for _, file in pairs(fio.glob('./000*.vylog')) do
        fio.unlink(file)
    end
end

clear()
box.cfg {
    wal_mode = wal_mode,
    too_long_threshold = 100500,
    memtx_use_mvcc_engine = false,
    memtx_memory = 1024 * 1024 * 1024 * 16,
}
alloc.setlimit(box.cfg.memtx_memory)
fiber.set_max_slice(100500)

-- Create the test space and indexes.
local s = box.schema.create_space('s', {format = format, field_count = #format,
                                        engine = space_engine})
for _, index in ipairs(indexes) do
    s:create_index(index.name, index.opts)
end

-- Get the test index data.
local search_index = s.index[search_index_name]
local write_index = s.index[write_index_name]
local kd = key_def.new(search_index.parts)
local write_kd = key_def.new(write_index.parts)
local kd_c_parts = {}
for _, part in pairs(search_index.parts) do
    assert(part.exclude_null == false)
    assert(part.is_nullable == false)
    assert(part.sort_order == "asc")
    table.insert(kd_c_parts, part.fieldno - 1)
    table.insert(kd_c_parts, part.type)
end
local write_kd_c_parts = {}
for _, part in pairs(write_index.parts) do
    assert(part.exclude_null == false)
    assert(part.is_nullable == false)
    assert(part.sort_order == "asc")
    table.insert(write_kd_c_parts, part.fieldno - 1)
    table.insert(write_kd_c_parts, part.type)
end


-- Reset the space data.
local tuple = {}
local function refill_space()
    s:truncate()
    box.begin()
    for i = 1, space_size do
        for j = 1, #format do
            tuple[j] = gen_field_value[j](i)
        end
        s:insert(tuple)
    end
    box.commit()
    assert(s:len() == space_size)
end

-- Set by prepare_for_tests.
local from_key
local until_key
local process_count

-- Fill the space with data and prepare test info.
local function prepare_for_tests()
    -- Fill the space.
    refill_space()

    local function get_key(offset, part_count)
        if offset == -1 then
            return {}
        end
        local key_offset_tuple =
            search_index:select(nil, {offset = offset, limit = 1})[1]
        local key_as_tuple = kd:extract_key(key_offset_tuple)
        -- Only get the required key parts.
        local key = {}
        for i = 1, part_count do
            key[i] = key_as_tuple[i]
        end
        assert(#key == part_count)
        return key
    end

    -- Get the until key.
    from_key = get_key(from_key_offset, from_key_part_count)
    until_key = get_key(until_key_offset, until_key_part_count)

    local from_offset = search_index:count(from_key, 'lt')
    if #from_key == 0 then
        from_offset = 0
    end
    local until_offset = search_index:count(until_key, 'lt')
    process_count = until_offset - from_offset
end

-- Filter for the SQL delete until test.
local function delete_process_until_sql_filter()
    return field_by_name['id'] ~= nil and
           search_index.id == 0 and
           #search_index.parts == 1
end

-- Filter for an update test.
local function has_non_unique_field_filter()
    return field_by_name['non_unique'] ~= nil
end

-- Filter for the SQL update until test.
local function update_until_sql_filter()
    return field_by_name['non_unique'] ~= nil and
           field_by_name['id'] ~= nil and
           search_index.id == 0 and
           #search_index.parts == 1
end

-- Filter for Arrow functions.
local function space_is_memcs_filter()
    return space_engine == 'memcs';
end

-- Filter for range deletion API.
local function delete_range_filter()
    -- The space is MemCS and the index.delete_range method exists.
    return space_engine == 'memcs' and box.space._space.delete_range ~= nil
end

-- Delete using a regular space:pairs(). Overheads:
-- - a lookup each step (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each deleted tuple.
local function delete_until_lua_naive()
    for _, tuple in search_index:pairs(from_key, {iterator = 'GE'}) do
        -- Break if the until key reached.
        if kd:compare_with_key(tuple, until_key) == 0 then
            break
        end
        write_index:delete(write_kd:extract_key(tuple))
    end
    assert(s:len() == space_size - process_count)
end

-- Deletes tuples by batches. Overheads:
-- - a lookup each batch_size steps (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each deleted tuple.
local keys = {}
for i = 1, batch_size do
    table.insert(keys, i)
end
local function delete_until_lua_batched()
    -- Collect the tuples, extract key batches and delete tuple batches.
    local i = 1
    for _, tuple in search_index:pairs(from_key, {iterator = 'GE'}) do
        -- Break if the until key reached.
        if kd:compare_with_key(tuple, until_key) == 0 then
            break
        end
        keys[i] = write_kd:extract_key(tuple)
        if i == batch_size then
            -- Delete the keys collected.
            for j = 1, batch_size do
                write_index:delete(keys[j])
            end
            i = 1
        else
            i = i + 1
        end
    end
    -- Delete the rest not forming a batch.
    for j = 1, i do
        write_index:delete(keys[j])
    end
    -- Check the result.
    assert(s:len() == space_size - process_count)
end

-- Delete tuples using the SQL engine. Overheads are unknown to the writer.
local function delete_until_sql()
    local from_value = 1
    if #from_key ~= 0 then
        assert(#from_key == 1)
        from_value = from_key[1]
    end
    box.execute('DELETE FROM s ' ..
                'WHERE id >= ' .. from_value .. ' AND ' ..
                      'id < ' .. until_key[1] .. ';')
    assert(s:len() == space_size - process_count)
end

-- Deletes tuples one by one using regular C iterators. Overheads:
-- - a lookup each step (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each deleted tuple.
box.schema.func.create('procs.delete_until_c_naive',
                       {language = 'C', if_not_exists = true})
local function delete_until_c_naive()
    box.func['procs.delete_until_c_naive']:call({s.id, search_index.id,
                                                 write_index.id, kd_c_parts,
                                                 from_key, until_key})
    assert(s:len() == space_size - process_count)
end

-- Deletes tuples by batches in C. Overheads:
-- - a lookup each batch_size steps (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each deleted tuple.
box.schema.func.create('procs.delete_until_c_batched',
                       {language = 'C', if_not_exists = true})
local function delete_until_c_batched()
    box.func['procs.delete_until_c_batched']:call({s.id, search_index.id,
                                                   write_index.id, kd_c_parts,
                                                   from_key, until_key,
                                                   batch_size})
    assert(s:len() == space_size - process_count)
end

-- Deletes tuples by batches in C. Overheads:
-- - a lookup to find the amount to delete.
-- - a lookup each batch_size steps (iterator invalidation).
-- - extract key from each deleted tuple.
box.schema.func.create('procs.delete_until_c_nocmp_batched',
                       {language = 'C', if_not_exists = true})
local function delete_until_c_nocmp_batched()
    box.func['procs.delete_until_c_nocmp_batched']:call({s.id,
                                                         search_index.id,
                                                         write_index.id,
                                                         kd_c_parts,
                                                         from_key, until_key,
                                                         batch_size})
    assert(s:len() == space_size - process_count)
end

-- Deletes tuples using Arrow stream to get write index keys.
box.schema.func.create('procs.delete_until_c_nocmp_arrow',
                       {language = 'C', if_not_exists = true})
local function delete_until_c_nocmp_arrow()
    box.func['procs.delete_until_c_nocmp_arrow']:call({s.id,
                                                       search_index.id,
                                                       write_index.id,
                                                       kd_c_parts,
                                                       write_kd_c_parts,
                                                       from_key, until_key,
                                                       batch_size})
    assert(s:len() == space_size - process_count)
end

-- Delete using the index.delete_range. Overheads:
-- - get space_size / batch_size quantiles.
-- - ??? copare with key each batch_size'th tuple.
-- - ??? extract key from each batch_size'th tuple.
--
-- Warning: search_index & write_index is not a thing here:
--          everything is performed on the search index.
local function delete_until_lua_range_api()
    -- Get keys by batch qiantiles.
    local quantile = batch_size / space_size
    local quantile_count = space_size / batch_size
    local keys = {}
    for i = 1, quantile_count - 1 do
        local key = search_index:quantile(quantile * i)
        -- Drop PK parts (work around a bug in
        -- the first version of MemCS quantile).
        for i = #search_index.parts + 1, #key do
            key[i] = nil
        end
        table.insert(keys, key)
    end
    table.insert(keys, {}) -- The empty key is the last one.

    -- Drop all quantile keys less than the `from_key`.
    if #from_key ~= 0 then
        local drop_first = 0
        for i, key in ipairs(keys) do
            if #key == 0 then
                break
            end
            if kd:compare_keys(key, from_key) > 0 then
                drop_first = i - 1
                break
            end
        end
        -- Drop the ones less or equal to the `from_key`.
        for i = 1, drop_first do
            table.remove(keys, 1)
        end
    end

    -- Delete the quantiles found up to the `until_key`.
    for i, end_key in ipairs(keys) do
        -- Delete the last range and exit if the until key overran/reached.
        if #end_key == 0 or kd:compare_keys(end_key, until_key) >= 0 then
            search_index:delete_range(from_key, until_key)
            break
        end
        search_index:delete_range(from_key, end_key)
    end
    assert(s:len() == space_size - process_count)
end

-- Update until the end key using regular Lua iterators. Overheads:
-- - a lookup each step (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each updated tuple.
local function update_until_lua_naive()
    for _, tuple in search_index:pairs(from_key, {iterator = 'GE'}) do
        -- Break if the until key reached.
        if kd:compare_with_key(tuple, until_key) == 0 then
            break
        end
        write_index:update(write_kd:extract_key(tuple),
                           {{'=', 'non_unique', 0}})
    end
end

-- Updates tuples by batches. Overheads:
-- - a lookup each batch_size steps (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each updated tuple.
local function update_until_lua_batched()
    -- Collect the tuples, extract key batches and update tuple batches.
    local i = 1
    for _, tuple in search_index:pairs(from_key, {iterator = 'GE'}) do
        -- Break if the until key reached.
        if kd:compare_with_key(tuple, until_key) == 0 then
            break
        end
        keys[i] = write_kd:extract_key(tuple)
        if i == batch_size then
            -- Update the keys collected.
            for j = 1, batch_size do
                write_index:update(keys[j], {{'=', 'non_unique', 0}})
            end
            i = 1
        else
            i = i + 1
        end
    end
    -- Update the rest not forming a batch.
    for j = 1, i do
        write_index:update(keys[j], {{'=', 'non_unique', 0}})
    end
end

-- Update tuples using the SQL engine. Overheads are unknown to the writer.
local function update_until_sql()
    local from_value = 1
    if #from_key ~= 0 then
        assert(#from_key == 1)
        from_value = from_key[1]
    end
    box.execute('UPDATE s SET non_unique = 0 ' ..
                'WHERE id >= ' .. from_value .. ' AND ' ..
                      'id < ' .. until_key[1] .. ';')
end

-- Updates tuples one by one using regular C iterators. Overheads:
-- - a lookup each step (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each updated tuple.
box.schema.func.create('procs.update_until_c_naive',
                       {language = 'C', if_not_exists = true})
local function update_until_c_naive()
    box.func['procs.update_until_c_naive']:call({s.id, search_index.id,
                                                 write_index.id, kd_c_parts,
                                                 {{'=', 'non_unique', 0}},
                                                 from_key, until_key})
end

-- Updates tuples by batches in C. Overheads:
-- - a lookup each batch_size steps (iterator invalidation).
-- - compare each tuple with the end key.
-- - extract key from each updated tuple.
box.schema.func.create('procs.update_until_c_batched',
                       {language = 'C', if_not_exists = true})
local function update_until_c_batched()
    box.func['procs.update_until_c_batched']:call({s.id, search_index.id,
                                                   write_index.id, kd_c_parts,
                                                   {{'=', 'non_unique', 0}},
                                                   from_key, until_key,
                                                   batch_size})
end

-- Select tuples from beginning up to some range end.
local function process_until_lua()
    local processed = 0
    for _, tuple in search_index:pairs(from_key, {iterator = 'ge'}) do
        -- Break if the until key reached.
        if kd:compare_with_key(tuple, until_key) == 0 then
            break
        end
        -- Process the tuple.
        processed = processed + 1
    end
    assert(processed == process_count)
end

-- Select tuples from beginning up to some range end using SQL.
local function process_until_sql()
    local from_value = 1
    if #from_key ~= 0 then
        assert(#from_key == 1)
        from_value = from_key[1]
    end
    local result = box.execute(
        'SELECT * FROM s WHERE id >= ' .. from_value .. ' AND ' ..
                              'id < ' .. until_key[1] .. ';')
    assert(#result.rows == process_count)
    -- Process the tuples.
end

-- Select tuples up until the range end in C.
box.schema.func.create('procs.process_until_c',
                       {language = 'C', if_not_exists = true})
local function process_until_c()
    box.func['procs.process_until_c']:call({s.id, search_index.id,
                                            kd_c_parts, from_key, until_key})
end

local function bench(name, func, cleanup)
    log(name .. ': ')
    if use_perf then
        perf_start()
    end
    local time_start = clock.time()
    if in_one_transaction then
        box.begin()
    end
    func()
    if in_one_transaction then
        box.commit()
    end
    local time_end = clock.time()
    if use_perf then
        perf_stop()
    end
    local time = time_end - time_start
    log(string.format('%.02f\n', time))
end

local tests = {
    { name = 'delete_until_lua_naive',
      func = delete_until_lua_naive,
      cleanup = refill_space },
    { name = 'delete_until_lua_batched',
      func = delete_until_lua_batched,
      cleanup = refill_space },
    { name = 'delete_until_lua_range_api',
      func = delete_until_lua_range_api,
      filter = delete_range_filter,
      cleanup = refill_space },
    { name = 'delete_until_sql',
      func = delete_until_sql,
      filter = delete_process_until_sql_filter,
      cleanup = refill_space },
    { name = 'delete_until_c_naive',
      func = delete_until_c_naive,
      cleanup = refill_space },
    { name = 'delete_until_c_batched',
      func = delete_until_c_batched,
      cleanup = refill_space },
    { name = 'delete_until_c_nocmp_batched',
      func = delete_until_c_nocmp_batched,
      cleanup = refill_space },
    { name = 'delete_until_c_nocmp_arrow',
      func = delete_until_c_nocmp_arrow,
      filter = space_is_memcs_filter,
      cleanup = refill_space },
    { name = 'update_until_lua_naive',
      func = update_until_lua_naive,
      filter = has_non_unique_field_filter,
      cleanup = refill_space },
    { name = 'update_until_lua_batched',
      func = update_until_lua_batched,
      filter = has_non_unique_field_filter,
      cleanup = refill_space },
    { name = 'update_until_sql',
      func = update_until_sql,
      filter = update_until_sql_filter,
      cleanup = refill_space },
    { name = 'update_until_c_naive',
      func = update_until_c_naive,
      filter = has_non_unique_field_filter,
      cleanup = refill_space },
    { name = 'update_until_c_batched',
      func = update_until_c_batched,
      filter = has_non_unique_field_filter,
      cleanup = refill_space },
    { name = 'process_until_lua',
      func = process_until_lua },
    { name = 'process_until_sql',
      func = process_until_sql,
      filter = delete_process_until_sql_filter },
    { name = 'process_until_c',
      func = process_until_c },
}

local tests_to_run = {}
for _, name_or_pattern in pairs(arg) do
    for _, test in ipairs(tests) do
        if test.name == name_or_pattern or
           string.find(test.name, name_or_pattern) then
            if test.filter == nil or test.filter() then
                table.insert(tests_to_run, test)
            else
                log('Matching test ' .. test.name .. ' skipped.\n')
            end
        end
    end
end

if #tests_to_run == 0 then
    print('No test to run. To run all:\n  tarantool ' .. arg[0] .. ' ' .. (arg[1] or '<options_file>') .. ' ' .. ' ".*"')
    os.exit()
end

log('\n')
log('Engine: ' .. space_engine .. '\n')
log('Size: ' .. space_size .. '\n')
log('\n')
log('Format:\n')
for i, field in ipairs(orig_format) do
    local generator_name = gen_field_value[i](-1)
    log('  ' .. field.name .. ': ' .. field.type .. ' (' .. generator_name .. ')')
    if field.count ~= nil then
        log(' (template, count: ' .. field.count .. ')')
    end
    log('\n')
end
log('\n')
log('Indexes:\n')
for i, index in ipairs(indexes) do
    log('  ' .. index.name .. ':\n')
    for i, part in ipairs(index.opts.parts) do
        log('    ' .. part[1] .. ': ' .. part[2] .. '\n')
    end
end
log('\n')
log('Search index: ' .. search_index_name .. '\n')
log('Until offset: ' .. until_key_offset .. '\n')
log('Search part count: ' .. until_key_part_count .. '\n')
log('Write index: ' .. write_index_name .. '\n')
log('Batch size: ' .. batch_size .. '\n')
log('\n')
log('WAL mode: ' .. wal_mode .. '\n')
log('In one transaction: ' .. tostring(in_one_transaction) .. '\n')

local function print_table(table, caption)
    log(caption .. ':\n')
    for key, value in pairs(table) do
        log('  ' .. key .. ': ' .. value .. '\n')
    end
    log('\n')
end

log('\nInitialization...\n\n')

prepare_for_tests()

-- Self-written function to print tree stats.
-- See the 0001-PoC-tree-stats-in-box.internal.doit.patch.
if box.internal.doit ~= nil then
    --box.internal.doit(s.id, search_index.id)
end
print_table(box.slab.info(), 'box.slab.info')
print_table(box.info.memory(), 'box.info.memory')

local box_stat_memtx = box.stat.memtx()
print_table(box_stat_memtx.data, 'box.stat.memtx.data')
print_table(box_stat_memtx.index, 'box.stat.memtx.index')

log('Testing...\n\n')
for i, test in ipairs(tests_to_run) do
    for j = 1, repetition_count do
        bench(test.name, test.func)
        if test.cleanup ~= nil and
           -- Do not clean-up after the last test.
           (i ~= #tests_to_run or j ~= repetition_count) then
            test.cleanup()
        end
    end
end

if use_perf then
    log('\nWaiting for perf...\n\n')
    perf_kill()
end

log('\nDeleted|updated|processed count (per test): ' .. process_count .. '\n\n')

os.exit()
