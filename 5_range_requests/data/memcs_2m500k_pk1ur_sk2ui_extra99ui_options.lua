local options = {}

options.space_size = 2500000
options.space_engine = 'memcs'
options.repetition_count = 1
options.repetition_count_warmup = 0

options.in_one_transaction = true
options.batch_size = 1000
options.wal_mode = 'write'

options.search_index_name = 'sk'
options.write_index_name = 'pk'

options.from_key_offset = -1 -- empty key.
options.from_key_part_count = 1
options.until_key_offset = options.space_size * 0.9
options.until_key_part_count = 1

options.format = {
    {name = 'id', type = 'unsigned', generator = {name = 'random_unique'}},
    {name = 'sk', type = 'unsigned', generator = {name = 'incrementing'}},
    {count = 99, name = 'extra%i', type = 'unsigned', generator = {name = 'incrementing'}},
}

options.indexes = {
    {name = 'pk', opts = {parts = {{'id', 'unsigned'}}, unique = true}},
    {name = 'sk', opts = {parts = {{'sk', 'unsigned'}}, unique = true}},
}

return options
