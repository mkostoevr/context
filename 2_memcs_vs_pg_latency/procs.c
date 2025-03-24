#include "module.h"
#include <stdlib.h>
#include <stdio.h>

void (*libc_exit)(int) = exit;

int counter_2(box_function_ctx_t *ctx, const char *args, const char *args_end)
{
	return 0;
}
