# THE IDEA

We need to check if the range request API is required to improve performance of Tarantool use cases. We gonna do things the most performant way known (currently possible) and see if we can improve in that using range API.

The issue covers the following requirements:
- range requests: can be done already using Lua, SQL, C API;
- partial return: can be done already using after, offset.

<!--

SCENARIOS:
- Drop by primary key (like rebalancing):
  - With no SK.
  - With 1 SK.
  - With 2 SKs.
  - With 5 SKs.
- Drop by secondary key (like TTL).
  - With 1 SK.
  - With 2 SKs.
  - With 5 SKs.

TODO:
- read view effects? COW.
- should nulls affect?
- different engines.
- on secondary keys
- with secondary keys
- partial keys
- non-unique indexes
- harder part types (slower comparators - just Any| Number?)
- transactions?
- WAL?
- MVCC? Slow-down.
-->

## Deletion
<!--
### Delete using several batch sizes

- Whats the optimal one?
- Why? (the leaf size? just less lookups? logarithmically/optimal size?)
- Does it do anything with cache line sizes.

-->
### Simple delete from a range

Conditions:
- Single random index.

Can show huw much of time is wasted in the txn machinery in the simplest case (so how much can we improve the performance in case we don wverything in a single IPROTO operation) - less WAL writes, less transaction statements, less TX stories to handle.

In case of deletion until some key a user can select the following approaches[^1][^2]:
1. Iteration in Lua, comparison using `key_def` and deletion via an index.
   
   <details>
   <summary>Code</summary>
   
   ```lua
   for _, tuple in search_index:pairs(from_key, {iterator = 'GE'}) do
       -- Break if the until key reached.
       if kd:compare_with_key(tuple, until_key) == 0 then
           break
       end
       write_index:delete(write_kd:extract_key(tuple))
   end
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 209.09s
   - incrementaly inserted data: 191.51s
2. Iteration over the space and deletion of keys by batches:
   
   <details>
   <summary>Code</summary>
   
   ```lua
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
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 211.86s (like the same?)
   - incrementaly inserted data: 178.22s (-6.94%)
3. Deletion using SQL:
   
   <details>
   <summary>Code</summary>
   
   ```lua
   box.execute('DELETE FROM s ' ..
               'WHERE id >= ' .. from_key[1] .. ' AND ' ..
                     'id < ' .. until_key[1] .. ';')
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 113.04s (x1.8)
   - incrementaly inserted data: 92.67s (x2)
4. Deletion via C API and iterators:
   
   <details>
   <summary>Code</summary>
   
   ```C
   /* Create the search index iterator. */
   box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_ALL,
                                           from_key, from_key_end);
   if (it == NULL)
       return ERROR("couldn't create an iterator");
   auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

   /* Iterate over the space and delete tuples. */
   box_tuple_t *tuple;
   for (;;) {
       /* Savepoint memory for the extracted key. */
       size_t region_svp = box_region_used();
       auto region_guard = make_scoped_guard([region_svp]() {
           box_region_truncate(region_svp);
       });

       /* Get the next tuple. */
       if (box_iterator_next(it, &tuple) != 0)
           return ERROR("couldn't advance the iterator");
       if (tuple == NULL)
           return ERROR("unexpected end of space");

       /* The until key reached - stop deletion.*/
       if (box_tuple_compare_with_key(tuple, until_key, kd) == 0)
           break;

       /* Delete the tuple. */
       uint32_t key_size;
       char *key = box_tuple_extract_key(tuple, space_id,
                                         write_index_id, &key_size);
       if (key == NULL)
           return ERROR("couldn't extract key from tuple");
       if (box_delete(space_id, write_index_id, key,
                      key + key_size, &tuple) != 0) {
           return ERROR("couldn't delete a the tuple");
       }
   }
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 63.40s (x3.3)
   - incrementaly inserted data: 51.15s (x3.7)
5. Deletion of key batches via C API.
   
   <details>
   <summary>Code</summary>
   
   ```C
   /* Create the search index iterator. */
   box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_ALL,
                                           from_key, from_key_end);
   if (it == NULL)
       return ERROR("couldn't create an iterator");
   auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

   /* Iterate over the space and delete tuple batches. */
   box_tuple_t *tuple;
   for (;;) {
       /* Get the next tuple. */
       if (box_iterator_next(it, &tuple) != 0)
           return ERROR("couldn't advance the iterator");
       if (tuple == NULL)
           return ERROR("unexpected end of space");

       /* The until key reached - stop deletion.*/
       if (box_tuple_compare_with_key(tuple, until_key, kd) == 0)
           break;

       /* Extract the tuple key and save it. */
       uint32_t key_size;
       char *key = box_tuple_extract_key(tuple, space_id,
                                         write_index_id, &key_size);
       if (key == NULL)
           return ERROR("couldn't extract key from tuple");
       keys[keys_size] = key;
       key_ends[keys_size++] = key + key_size;

       /* Delete the batch if collected enough. */
       if (keys_size == batch_size) {
           for (int i = 0; i < batch_size; i++) {
               if (box_delete(space_id, write_index_id,
                              keys[i], key_ends[i],
                              &tuple) != 0) {
                   return ERROR("couldn't delete a tuple");
               }
           }
           keys_size = 0;
           box_region_truncate(region_keys_svp);
       }
   }

   /* Handle the remained collected keys not forming a full batch. */
   for (int i = 0; i < keys_size; i++) {
       if (box_delete(space_id, write_index_id, keys[i],
                      key_ends[i], &tuple) != 0) {
           return ERROR("couldn't delete a tuple in tail");
       }
   }
   ```
   
   </details>
   
   Time: 
   - randomly inserted data: 56.77s (x3.7)
   - incrementaly inserted data: 40.95s (x4.7)

So using the C API and deleting keys by can improve the request performance 4 - 5 times.

**TODO**: perf picture.

<!--
Full key vs partial one:
- Is there going to be any difference here in it? It seems delete range with full keys must be enough.

### PK-only resharding like in aeon

Conditions:
- Multipart key: user + its pk.
- Random insertion.
- Dropping part of data by primary key.

Can show what are the performance ruiners in the real life applications (ish).

### Dropping data by TTL in MemCS

Conditions:
- Engine: MemCS vs MemTX
- Random multipart PK.
- Sequential secondary key.
- Dropping by SK.

Going to show:
- how much time is wasted during tuple generation.
- How much Might we get rid of if it happened by the parts required to delete the tuple from other indexes (we've taen only prmary key parts for example).
-->

## Update

What's the use case at all?

## Select

Is range select API required? It can be used to select tuples without comparison with the until key on each step. We simply find the last tuple and compare raw pointers.

Here's what the user can do now[^3][^4]:
1. Use regular Lua iterators and the `key_def` module to process tuples up to some `until` key:
   
   <details>
   <summary>Code</summary>
   
   ```lua
   for _, tuple in search_index:pairs(from_key, {iterator = 'ge'}) do
       -- Break if the until key reached.
       if kd:compare_with_key(tuple, until_key) == 0 then
           break
       end
       -- Process the tuple.
   end
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 72s
   - incrementaly inserted data: 63s
2. Use SQL:
   
   <details>
   <summary>Code</summary>
   
   ```lua
   local result = box.execute(
       'SELECT * FROM s WHERE id >= ' .. from_key[1].. ' AND ' ..
                             'id < ' .. until_key[1] .. ';')
   -- Process the tuples.
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 27s (x2.7)
   - incrementaly inserted data: 24s (x2.7)
3. Use C iterators and do the same operations as in Lua:
   
   <details>
   <summary>Code</summary>
   
   ```C
   /* Create the search index iterator. */
   box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_ALL,
                                           from_key, from_key_end);
   if (it == NULL)
       return ERROR("couldn't create an iterator");
   auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

   /* Iterate over the space and delete tuples. */
   box_tuple_t *tuple;
   for (;;) {
       /* Savepoint memory for the extracted key. */
       size_t region_svp = box_region_used();
       auto region_guard = make_scoped_guard([region_svp]() {
           box_region_truncate(region_svp);
       });

       /* Get the next tuple. */
       if (box_iterator_next(it, &tuple) != 0)
           return ERROR("couldn't advance the iterator");
       if (tuple == NULL)
           return ERROR("unexpected end of space");

       /* The until key reached - stop processing.*/
       if (box_tuple_compare_with_key(tuple, until_key, kd) == 0)
           break;

       /* Process the tuple. */
   }
   ```
   
   </details>
   
   Time:
   - randomly inserted data: 7s (x10)
   - incrementaly inserted data: 2s (x31.8)

So using the C API can improve the request performance 10 - 32 times. Here's the perf picture we have using the C API.

Space bulit using random insertions[^5]:
- Iteration: 87.53%
- Comparison: 7.93%
- Region truncate: 3.11%

Space built using sequential insertions[^6]:
- Iteration: 58.88%
- Comparison: 24.99%
- Region truncate: 11.36%

So we can shave off 8% - 25% of runtime in C (by dropping comparisons). The incerease is expected to significantly less in higher layers, there it more seems like a syntactic sugar for not doing SQL or implementing it manually.

<!--
Simple select until some tuple with a single index:
   - what's the percentage of:
     - comparisons (we can get rid of these if we use a new API)
     - block getting (probaly the most of the time)
     - txn management (if MVCC enabled: dirting tuples, read gap creation).
       - does the index count affect? read gaps index-specific or whatever?

-->
[^1]: [`data/memtx_30M_pk1ur_delete_until.stdout`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ur_delete_until.stdout)
[^2]: [`data/memtx_30M_pk1ui_delete_until.stdout`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ui_delete_until.stdout)
[^3]: [`data/memtx_30M_pk1ur_process_until.stdout`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ur_process_until.stdout)
[^4]: [`data/memtx_30M_pk1ui_process_until.stdout`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ui_process_until.stdout)
[^5]: [`data/memtx_30M_pk1ur_process_until_c.stack`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ur_process_until_c.stack)
[^6]: [`data/memtx_30M_pk1ui_process_until_c.stack`](https://github.com/mkostoevr/context/tree/master/5_range_requests/data/memtx_30M_pk1ui_process_until_c.stack)
