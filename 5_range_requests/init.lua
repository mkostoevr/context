local space_size = 30000
local space_engine = 'memcs'
local repetition_count = nil      -- Default: arg[2] if exists, 10 if arg[1] exists, 1 othervice.
local repetition_count_warmup = 0 -- Default: no warm-up.

-- User PoV tuning options.
local in_one_transaction = true
local batch_size = 1000
local wal_mode = 'write'

-- The indexes used to delete/update/select.
local search_index_name = 'pk'
local write_index_name = 'pk'

-- Lookup options.
local from_key_offset = -1 -- empty key.
local from_key_part_count = 1
local until_key_offset = space_size * 0.9
local until_key_part_count = 1

-- The data schema.
local format = {
    {name = 'id', type = 'unsigned', generator = {name = 'incrementing'}}, -- 123456789
    {name = 'non_unique', type = 'unsigned', generator = {name = 'random', min = 1, max = 100}}, -- 887241132
--    {name = 'long_step', type = 'unsigned', generator = {name = 'long_step', step_size = 100}}, -- 111222333
--    {name = 'repeating', type = 'unsigned', generator = {name = 'repeating', steps = 100}}, -- 123123123
--    {name = 'unique', type = 'unsigned', generator = {name = 'random_unique'}}, -- 975642138
}

local indexes = {
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

-- Default repetition count: 1 if no function selected, 10 otherwise.
if repetition_count == nil then
    if arg[2] ~= nil then
        repetition_count = tonumber(arg[2])
    elseif arg[1] ~= nil then
        repetition_count = 10
    else
        repetition_count = 1
    end
end

local fio = require('fio')
local clock = require('clock')
local fiber = require('fiber')
local key_def = require('key_def')

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
    memtx_memory = 1024 * 1024 * 1024 * 28,
}
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

-- Delete using a regular space:pairs(). Overheads:
-- - copare with key each batch_size'th tuple.
-- - extract key from each batch_size'th tuple.
local function delete_until_lua_range_api()
    while true do
        local tuple = search_index:select(from_key, {iterator = 'GE',
                                                     offset = batch_size,
                                                     limit = 1})[1]
        -- Delete the last range and exit if the until key overran/reached.
        if kd:compare_with_key(tuple, until_key) >= 0 then
            write_index:delete_range(from_key, until_key)
            break
        end
        write_index:delete_range(from_key, write_kd:extract_key(tuple))
    end
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
    box.once('init', function()
        local function print_table(table, caption)
            log(caption .. ':\n')
            for key, value in pairs(table) do
                log('  ' .. key .. ': ' .. value .. '\n')
            end
            log('\n')
        end

        prepare_for_tests()

        -- Self-written function to print tree stats.
        -- See the 0001-PoC-tree-stats-in-box.internal.doit.patch.
        if box.internal.doit ~= nil then
            box.internal.doit(s.id, search_index.id)
        end
        print_table(box.slab.info(), 'box.slab.info')
        print_table(box.info.memory(), 'box.info.memory')

        local box_stat_memtx = box.stat.memtx()
        print_table(box_stat_memtx.data, 'box.stat.memtx.data')
        print_table(box_stat_memtx.index, 'box.stat.memtx.index')
    end)

    log(name .. ': ')
    local time_start = clock.time()
    if in_one_transaction then
        box.begin()
    end
    func()
    if in_one_transaction then
        box.commit()
    end
    local time_end = clock.time()
    local time = time_end - time_start
    log(string.format('%.02f', time))
    if cleanup ~= nil then
        cleanup()
    end
    log('\n')
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

local function run_tests(repetition_count)
    for _, test in ipairs(tests) do
        if arg[1] == nil or string.find(test.name, arg[1]) then
            if test.filter == nil or test.filter() then
                for i = 1, repetition_count do
                    bench(test.name, test.func, test.cleanup)
                end
            end
        end
    end
end

log('\n')
log('Engine: ' .. space_engine .. '\n')
log('Size: ' .. space_size .. '\n')
log('\n')
log('Format:\n')
for i, field in ipairs(format) do
    local generator_name = gen_field_value[i](-1)
    log('  ' .. field.name .. ': ' .. field.type .. ' (' .. generator_name .. ')\n')
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

if repetition_count_warmup ~= 0 then
    log('\nWarming-up...\n')
    run_tests(repetition_count_warmup)
else
    log('\nWarm-up skipped.');
end

log('\nTesting...\n')
run_tests(repetition_count)

log('\nDeleted|updated|processed count (per test): ' .. process_count .. '\n\n')

os.exit()
