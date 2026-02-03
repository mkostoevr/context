#include "module.h"
#include "msgpuck.h"
#include <stdlib.h>
#include <stdio.h>

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

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
args_parse_index_parts(const char **args, struct key_def **kd)
{
	if (mp_typeof(**args) != MP_ARRAY)
		return ERROR("index parts not array");
	uint32_t parts_len = mp_decode_array(args);
	if (parts_len % 2 != 0)
		return ERROR("non-even c_parts");
	uint32_t part_count = parts_len / 2;
	if (part_count > 8)
		return ERROR("more than 8 parts?");
	uint32_t fields[part_count];
	uint32_t types[part_count];
	for (uint32_t i = 0; i < part_count; i++) {
		if (mp_typeof(**args) != MP_UINT)
			return ERROR("part fieldno not uint");
		fields[i] = mp_decode_uint(args);
		if (mp_typeof(**args) != MP_STR)
			return ERROR("part type not str");
		uint32_t len;
		const char *str = mp_decode_str(args, &len);
		types[i] = strnindex(field_type_strs, str, len, field_type_MAX);
		if (types[i] == field_type_MAX)
			return ERROR("unknown part type: %.*s", len, str);
	}
	*kd = box_key_def_new(fields, types, 1);
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

	/* Index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Index parts. */
	box_key_def_t *kd;
	if (args_parse_index_parts(&args, &kd) != 0)
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

	/* Index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Index parts. */
	box_key_def_t *kd;
	if (args_parse_index_parts(&args, &kd) != 0)
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

	/* Index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Index parts. */
	box_key_def_t *kd;
	if (args_parse_index_parts(&args, &kd) != 0)
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
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_ALL,
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
			return ERROR("couldn't update a the tuple");
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

	/* Index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Write index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("write index ID not uint");
	uint32_t write_index_id = mp_decode_uint(&args);

	/* Index parts. */
	box_key_def_t *kd;
	if (args_parse_index_parts(&args, &kd) != 0)
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
	box_iterator_t *it = box_index_iterator(space_id, index_id, ITER_ALL,
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

	/* Index ID. */
	if (mp_typeof(*args) != MP_UINT)
		return ERROR("index ID not uint");
	uint32_t index_id = mp_decode_uint(&args);

	/* Index parts. */
	box_key_def_t *kd;
	if (args_parse_index_parts(&args, &kd) != 0)
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
	return 0;
}
