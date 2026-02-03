#include "module.h"
#include "msgpuck.h"
#include <stdlib.h>
#include <stdio.h>

#include "arrow/abi.h"

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

#define MIN(a, b) ((a) < (b) ? (a) : (b))

#define ERROR(...) box_error_set(__FILE__, __LINE__, ER_PROC_C, __VA_ARGS__)

static const char *field_type_strs[] = {
	/* [FIELD_TYPE_ANY]      = */ "any",
	/* [FIELD_TYPE_UNSIGNED] = */ "unsigned",
	/* [FIELD_TYPE_STRING]   = */ "string",
	/* [FIELD_TYPE_NUMBER]   = */ "number",
	/* [FIELD_TYPE_DOUBLE]   = */ "double",
	/* [FIELD_TYPE_INTEGER]  = */ "integer",
	/* [FIELD_TYPE_BOOLEAN]  = */ "boolean",
	/* [FIELD_TYPE_VARBINARY] = */"varbinary",
	/* [FIELD_TYPE_SCALAR]   = */ "scalar",
	/* [FIELD_TYPE_DECIMAL]  = */ "decimal",
	/* [FIELD_TYPE_UUID]     = */ "uuid",
	/* [FIELD_TYPE_DATETIME] = */ "datetime",
	/* [FIELD_TYPE_INTERVAL] = */ "interval",
	/* [FIELD_TYPE_ARRAY]    = */ "array",
	/* [FIELD_TYPE_MAP]      = */ "map",
	/* [FIELD_TYPE_INT8]     = */ "int8",
	/* [FIELD_TYPE_UINT8]    = */ "uint8",
	/* [FIELD_TYPE_INT16]    = */ "int16",
	/* [FIELD_TYPE_UINT16]   = */ "uint16",
	/* [FIELD_TYPE_INT32]    = */ "int32",
	/* [FIELD_TYPE_UINT32]   = */ "uint32",
	/* [FIELD_TYPE_INT64]    = */ "int64",
	/* [FIELD_TYPE_UINT64]   = */ "uint64",
	/* [FIELD_TYPE_FLOAT32]  = */ "float32",
	/* [FIELD_TYPE_FLOAT64]  = */ "float64",
	/* [FIELD_TYPE_DECIMAL32]  = */ "decimal32",
	/* [FIELD_TYPE_DECIMAL64]  = */ "decimal64",
	/* [FIELD_TYPE_DECIMAL128] = */ "decimal128",
	/* [FIELD_TYPE_DECIMAL256] = */ "decimal256",
};

static_assert(lengthof(field_type_strs) == field_type_MAX,
	      "Each field type must be present in field_type_strs");

static uint32_t
strnindex(const char *const *haystack, const char *needle, uint32_t len,
	  uint32_t hmax)
{
	if (len == 0)
		return hmax;
	for (unsigned index = 0; index != hmax && haystack[index]; index++) {
		if (strncasecmp(haystack[index], needle, len) == 0 &&
		    strlen(haystack[index]) == len)
			return index;
	}
	return hmax;
}

template <typename Functor>
struct ScopedGuard {
	Functor f;
	bool is_active;

	explicit ScopedGuard(const Functor& fun)
		: f(fun), is_active(true) {
		/* nothing */
	}

	~ScopedGuard()
	{
		if (is_active)
			f();
	}

private:
	ScopedGuard(ScopedGuard&&) = delete;
	explicit ScopedGuard(const ScopedGuard&) = delete;
	ScopedGuard& operator=(const ScopedGuard&) = delete;
};

template <typename Functor>
inline ScopedGuard<Functor>
make_scoped_guard(Functor guard)
{
	return ScopedGuard<Functor>(guard);
}

/* Array: {fieldno, type, ...}. */
static int
args_parse_index_parts(const char **args, uint32_t **fields,
		       uint32_t **types, uint32_t *part_count)
{
	if (mp_typeof(**args) != MP_ARRAY)
		return ERROR("index parts not array");
	uint32_t parts_len = mp_decode_array(args);
	if (parts_len % 2 != 0)
		return ERROR("non-even c_parts");
	*part_count = parts_len / 2;
	if (*part_count > 8)
		return ERROR("more than 8 parts?");
	*fields = (uint32_t *)malloc(sizeof(**fields) * *part_count);
	if (*fields == NULL)
		return ERROR("can't allocate fields array");
	auto fields_guard = make_scoped_guard([fields]() { free(*fields); });
	*types = (uint32_t *)malloc(sizeof(**types) * *part_count);
	if (*types == NULL)
		return ERROR("can't allocate types array");
	auto types_guard = make_scoped_guard([types]() { free(*types); });
	for (uint32_t i = 0; i < *part_count; i++) {
		if (mp_typeof(**args) != MP_UINT)
			return ERROR("part fieldno not uint");
		(*fields)[i] = mp_decode_uint(args);
		if (mp_typeof(**args) != MP_STR)
			return ERROR("part type not str");
		uint32_t len;
		const char *str = mp_decode_str(args, &len);
		(*types)[i] = strnindex(field_type_strs, str,
					len, field_type_MAX);
		if ((*types)[i] == field_type_MAX)
			return ERROR("unknown part type: %.*s", len, str);
	}
	fields_guard.is_active = false;
	types_guard.is_active = false;
	return 0;
}

static int
args_parse_key_def(const char **args, struct key_def **kd)
{
	uint32_t *fields, *types, part_count;
	if (args_parse_index_parts(args, &fields, &types, &part_count) != 0)
		return -1;
	auto guard = make_scoped_guard([fields, types]() {
		free(fields);
		free(types);
	});
	*kd = box_key_def_new(fields, types, part_count);
	if (*kd == NULL)
		return ERROR("couldn't create a key definition");
	return 0;
}

extern "C" int
delete_until_c_naive(box_function_ctx_t *ctx,
		     const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 6)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
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
			return ERROR("couldn't delete a tuple");
		}
	}
	return 0;
}

static int
arrow_array_row_to_mp(struct ArrowArray *array, int row_i, uint32_t *types,
		      char *data, ptrdiff_t *data_sz)
{
	for (int col_i = 0; col_i < array->n_children; col_i++) {
		struct ArrowArray *column = array->children[col_i];
		uint64_t *u64_values = (uint64_t *)column->buffers[1];
		switch (types[col_i]) {
		case FIELD_TYPE_UNSIGNED:
			data = mp_encode_uint_safe(data, data_sz,
						   u64_values[row_i]);
			break;
		default:
			return ERROR("Unknown field type: %s",
				     field_type_strs[types[col_i]]);
		}
	}
	return 0;
}

static int
arrow_array_transpose(struct ArrowArray *array, uint32_t *types,
		      int key_count, char **keys)
{
	for (int row_i = 0; row_i < key_count; row_i++) {
		ptrdiff_t key_len = 0;
		if (arrow_array_row_to_mp(array, row_i, types, NULL, &key_len))
			return -1;
		keys[row_i] = (char *)malloc(
			-key_len + mp_sizeof_array(array->n_children));
		if (keys[row_i] == NULL)
			return ERROR("can't allocate a key");
		char *data = mp_encode_array(keys[row_i], array->n_children);
		if (arrow_array_row_to_mp(array, row_i, types, data, NULL) != 0)
			return -1;
	}
	return 0;
}

extern "C" int
delete_until_c_batched(box_function_ctx_t *ctx,
		       const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 7)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */

	/* The delete batch size. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("batch size not uint");
	uint32_t batch_size = mp_decode_uint(&args);

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Allocate the deletion key buffer. */
	size_t region_svp = box_region_used();
	auto region_guard = make_scoped_guard([region_svp]() {
		box_region_truncate(region_svp);
	});
	char **keys = (char **)box_region_alloc(batch_size * sizeof(*keys));
	char **key_ends =
		(char **)box_region_alloc(batch_size * sizeof(*key_ends));
	int keys_size = 0;

	/* Next memory is for the extracted keys. */
	size_t region_keys_svp = box_region_used();

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
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
	return 0;
}

extern "C" int
delete_until_c_nocmp_batched(box_function_ctx_t *ctx,
			     const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 7)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */
	const char *until_key_end = args;

	/* The delete batch size. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("batch size not uint");
	uint32_t batch_size = mp_decode_uint(&args);

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Get the amount to delete. */
	ssize_t rows_remained = box_index_count(space_id, index_id, ITER_LT,
						until_key, until_key_end);
	if (rows_remained < 0)
		return ERROR("can't count the amount to delete");

	/* Allocate the deletion key buffer. */
	size_t region_svp = box_region_used();
	auto region_guard = make_scoped_guard([region_svp]() {
		box_region_truncate(region_svp);
	});
	char **keys = (char **)box_region_alloc(batch_size * sizeof(*keys));
	char **key_ends =
		(char **)box_region_alloc(batch_size * sizeof(*key_ends));
	int keys_size = 0;

	/* Next memory is for the extracted keys. */
	size_t region_keys_svp = box_region_used();

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
						from_key, from_key_end);
	if (it == NULL)
		return ERROR("couldn't create an iterator");
	auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

	/* Iterate over the space and delete tuple batches. */
	box_tuple_t *tuple;
	while (rows_remained) {
		/* Get the next tuple. */
		if (box_iterator_next(it, &tuple) != 0)
			return ERROR("couldn't advance the iterator");
		if (tuple == NULL)
			return ERROR("unexpected end of space");

		/* Extract the tuple key and save it. */
		uint32_t key_size;
		char *key = box_tuple_extract_key(tuple, space_id,
						  write_index_id, &key_size);
		if (key == NULL)
			return ERROR("couldn't extract key from tuple");
		keys[keys_size] = key;
		key_ends[keys_size++] = key + key_size;
		rows_remained--;

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
	return 0;
}

extern "C" int
delete_until_c_nocmp_arrow(box_function_ctx_t *ctx,
			   const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 8)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* Write index parts. */
	uint32_t *fields, *types, part_count;
	if (args_parse_index_parts(&args, &fields, &types, &part_count) != 0)
		return -1;
	auto parts_guard = make_scoped_guard([fields, types]() {
		free(fields);
		free(types);
	});

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */
	const char *until_key_end = args;

	/* The delete batch size. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("batch size not uint");
	uint32_t batch_size = mp_decode_uint(&args);

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Get the amount to delete. */
	ssize_t rows_remained = box_index_count(space_id, index_id, ITER_LT,
						until_key, until_key_end);
	if (rows_remained < 0)
		return ERROR("can't count the amount to delete");

	/* Create the search index scanner. */
	struct ArrowArrayStream stream = {};
	box_arrow_options_t *options = box_arrow_options_new();
	auto arrow_options_guard = make_scoped_guard([options]() {
		box_arrow_options_delete(options);
	});
	box_arrow_options_set_iterator(options, ITER_GE);
	box_arrow_options_set_batch_row_count(options, batch_size);
	if (box_index_arrow_stream(space_id, index_id, part_count,
				   fields, from_key, from_key_end,
				   options, &stream) != 0)
		return ERROR("couldn't create an Arrow stream");
	auto it_guard = make_scoped_guard([&stream]() {
		if (stream.release != NULL)
			stream.release(&stream);
	});

	/* Scan the write index part batches and delete the rows. */
	for (;;) {
		size_t region_svp = box_region_used();
		auto region_guard = make_scoped_guard([region_svp]() {
			box_region_truncate(region_svp);
		});

		/* Get the next batch. */
		struct ArrowArray array;
		if (stream.get_next(&stream, &array) != 0)
			return ERROR("couldn't read the next stream batch");
		if (array.n_children == 0)
			break; /* End of data. */
		if (array.n_children != part_count)
			return ERROR("unexpected n_children: %d", array.n_children);

		/* Transpose the batch (get msgpack keys). */
		int key_count = MIN(array.length, rows_remained);
		char **keys = (char **)malloc(sizeof(*keys) * key_count);
		if (keys == NULL)
			return ERROR("can't allocate a key batch");
		arrow_array_transpose(&array, types, key_count, keys);

		/* Drop the keys. */
		for (int i = 0; i < key_count; i++) {
			char *key = keys[i];
			const char *key_end = key;
			mp_next(&key_end);
			box_tuple_t *dummy;
			if (box_delete(space_id, write_index_id,
				       key, key_end, &dummy) != 0) {
				return ERROR("couldn't delete a tuple");
			}
		}
		rows_remained -= key_count;

		/* Free the keys. */
		for (int i = 0; i < key_count; i++)
			free(keys[i]);
		free(keys);
	}
	return 0;
}

extern "C" int
update_until_c_naive(box_function_ctx_t *ctx,
		     const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 7)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* Update ops. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("non-array update ops");
	const char *ops = args;
	mp_next(&args); /* Skip the options without checking. */
	const char *ops_end = args;

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
						from_key, from_key_end);
	if (it == NULL)
		return ERROR("couldn't create an iterator");
	auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

	/* Iterate over the space and update tuples. */
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

		/* The until key reached - stop update.*/
		if (box_tuple_compare_with_key(tuple, until_key, kd) == 0)
			break;

		/* Update the tuple. */
		uint32_t key_size;
		char *key = box_tuple_extract_key(tuple, space_id,
						  write_index_id, &key_size);
		if (key == NULL)
			return ERROR("couldn't extract key from tuple");
		if (box_update(space_id, write_index_id, key, key + key_size,
			       ops, ops_end, 1, &tuple) != 0)
			return ERROR("couldn't update a tuple");
	}
	return 0;
}

extern "C" int
update_until_c_batched(box_function_ctx_t *ctx,
		       const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 8)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* Update ops. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("non-array update ops");
	const char *ops = args;
	mp_next(&args); /* Skip the options without checking. */
	const char *ops_end = args;

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */

	/* The update batch size. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("batch size not uint");
	uint32_t batch_size = mp_decode_uint(&args);

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Allocate the update key buffer. */
	size_t region_svp = box_region_used();
	auto region_guard = make_scoped_guard([region_svp]() {
		box_region_truncate(region_svp);
	});
	char **keys = (char **)box_region_alloc(batch_size * sizeof(*keys));
	char **key_ends =
		(char **)box_region_alloc(batch_size * sizeof(*key_ends));
	int keys_size = 0;

	/* Next memory is for the extracted keys. */
	size_t region_keys_svp = box_region_used();

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
						from_key, from_key_end);
	if (it == NULL)
		return ERROR("couldn't create an iterator");
	auto it_guard = make_scoped_guard([it]() { box_iterator_free(it); });

	/* Iterate over the space and update tuple batches. */
	box_tuple_t *tuple;
	for (;;) {
		/* Get the next tuple. */
		if (box_iterator_next(it, &tuple) != 0)
			return ERROR("couldn't advance the iterator");
		if (tuple == NULL)
			return ERROR("unexpected end of space");

		/* The until key reached - stop update. */
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

		/* Update the batch if collected enough. */
		if (keys_size == batch_size) {
			for (int i = 0; i < batch_size; i++) {
				if (box_update(space_id, write_index_id,
					       keys[i], key_ends[i],
					       ops, ops_end, 1, &tuple) != 0) {
					return ERROR("couldn't update a tuple");
				}
			}
			keys_size = 0;
			box_region_truncate(region_keys_svp);
		}
	}

	/* Handle the remained collected keys not forming a full batch. */
	for (int i = 0; i < keys_size; i++) {
		if (box_update(space_id, write_index_id, keys[i], key_ends[i],
			       ops, ops_end, 1, &tuple) != 0) {
			return ERROR("couldn't update a tuple in tail");
		}
	}
	return 0;
}

extern "C" int
process_until_c(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	/* Parse the arguments. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("args not array");
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 5)
		return ERROR("invalid argument count: %d", arg_count);

	/* Space ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("space ID not uint");
	uint32_t space_id = mp_decode_uint(&args);

	/* Search index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Search index key_def. */
	box_key_def_t *kd;
	if (args_parse_key_def(&args, &kd) != 0)
		return -1;
	auto kd_guard = make_scoped_guard([kd]() { box_key_def_delete(kd); });

	/* The from key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *from_key = args;
	mp_next(&args); /* Skip the key. */
	const char *from_key_end = args;

	/* The until key. */
	if (mp_typeof(*args) != MP_ARRAY)
		return ERROR("key is not array");
	const char *until_key = args;
	mp_next(&args); /* Skip the key. */

	/* That's it. */
	if (args != args_end)
		return ERROR("bigger input than expected");

	/* Create the search index iterator. */
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_GE,
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
	return 0;
}
