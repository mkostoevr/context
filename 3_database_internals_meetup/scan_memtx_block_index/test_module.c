#include "module.h"
#include "msgpuck/msgpuck.h"

int test_module(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	uint32_t arg_count = mp_decode_array(&args);
	if (arg_count != 2) {
		fprintf(stderr, "@@@ not 2 args.\n");
		return -1;
	}

	uint32_t space_id = mp_decode_uint(&args);
	uint32_t index_id = mp_decode_uint(&args);
	char key[8];
	char *key_end = mp_encode_array(key, 0);
	box_iterator_t *iter = box_index_iterator(space_id, index_id, ITER_ALL,
						  key, key_end);
	if (iter == NULL) {
		fprintf(stderr, "@@@ can't create an iterator.\n");
		return -1;
	}

	int rc = 0;
	uint64_t sum = 0;
	uint64_t last_offset = 0;
	const char *data;
	box_tuple_t *tuple;
	while (true) {
		rc = box_iterator_next(iter, &tuple);
		if (rc != 0) {
			fprintf(stderr, "@@@ can't advance an iterator.\n");
			break;
		}
		if (tuple == NULL)
			break;

		data = box_tuple_field(tuple, 0);
		last_offset = mp_decode_uint(&data);

		data = box_tuple_field(tuple, 1);
		uint32_t len;
		uint64_t *block = (uint64_t *)mp_decode_bin(&data, &len);
		assert(len == 8192 * 8);
		for (int i = 0; i < 8192; i++)
			sum += block[i];
	}
	//fprintf(stderr, "@@@ last offset: %lu\n", last_offset);
	//fprintf(stderr, "@@@ sum: %lu\n", sum);
	box_iterator_free(iter);
	return rc;
}
