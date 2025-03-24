#define _GNU_SOURCE

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>

#include <netdb.h>
#include <unistd.h>

#include "msgpuck/msgpuck.h"

#define lengthof(array) (sizeof(array) / sizeof(array[0]))

#define ERROR_SYS(msg) do { perror(msg); exit(1); } while (0)
#define ERROR_FATAL(fmt, ...) do { printf(fmt "\n", ## __VA_ARGS__); exit(1); } while (0)
#define BUG_ON(x) do { if (x) ERROR_FATAL("Bug at %s:%d: %s\n", __FILE__, __LINE__, #x); } while (0)

#define IPROTO_REQUEST_TYPE	0x00
#define IPROTO_SYNC		0x01
#define IPROTO_TUPLE		0x21
#define IPROTO_FUNCTION_NAME	0x22

#define IPROTO_call_16	0x06
#define IPROTO_call	0x0a
#define IPROTO_ping	0x40

struct Data {
	uint8_t *raw_req;
	size_t raw_req_size;
};

uint64_t
nsecs(struct timespec t0, struct timespec t1)
{
	return (t1.tv_sec * 1000000000llu + t1.tv_nsec) -
		(t0.tv_sec * 1000000000llu + t0.tv_nsec);
}

void DumpHex(const void* data, size_t size) {
	char ascii[17];
	size_t i, j;
	ascii[16] = '\0';
	for (i = 0; i < size; ++i) {
		printf("%02X ", ((unsigned char*)data)[i]);
		if (((unsigned char*)data)[i] >= ' ' && ((unsigned char*)data)[i] <= '~') {
			ascii[i % 16] = ((unsigned char*)data)[i];
		} else {
			ascii[i % 16] = '.';
		}
		if ((i+1) % 8 == 0 || i+1 == size) {
			printf(" ");
			if ((i+1) % 16 == 0) {
				printf("|  %s \n", ascii);
			} else if (i+1 == size) {
				ascii[(i+1) % 16] = '\0';
				if ((i+1) % 16 <= 8) {
					printf(" ");
				}
				for (j = (i+1) % 16; j < 16; ++j) {
					printf("   ");
				}
				printf("|  %s \n", ascii);
			}
		}
	}
}

int
bench_connect(const char *hostname, uint16_t port)
{
	/* Create the connection socket. */
	int fd = -1;
	if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		ERROR_SYS("Couldn't create a socket");
	/* Set socket options. */
	{
		struct timeval tmout_send = {};
		struct timeval tmout_recv = {};
		if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tmout_send, sizeof(tmout_send)) == -1)
			ERROR_SYS("Couldn't set socket send timeout");
		if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tmout_recv, sizeof(tmout_recv)) == -1)
			ERROR_SYS("Couldn't set socket recv timeout");
	}
	/* Get Tarantool address. */
	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(port),
	};
	{
		struct addrinfo *addr_info = NULL;
		if (getaddrinfo(hostname, NULL, NULL, &addr_info) != 0)
			ERROR_SYS("Couldn't resolve the Tarantool address");
		memcpy(&addr.sin_addr,
		       (void*)&((struct sockaddr_in *)addr_info->ai_addr)->sin_addr,
		       sizeof(addr.sin_addr));
		freeaddrinfo(addr_info);
	}
	/* Connect to the Tarantool. */
	if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1)
		ERROR_SYS("Couldn't connect to Tarantool.");

	/* Read the greeting (Tarantool 1.6+). */
	if (port == 3301) {
		uint8_t greeting[128];
		read(fd, greeting, sizeof(greeting));
	}

	return fd;
}

struct timespec
bench_start()
{
	struct timespec t0;
	clock_gettime(CLOCK_MONOTONIC, &t0);
	return t0;
}

uint64_t
bench_finish(struct timespec t0)
{
	struct timespec t1;
	clock_gettime(CLOCK_MONOTONIC, &t1);
	return nsecs(t0, t1);
}

uint64_t
get_unsigned(uint8_t *buf, int bytes)
{
	uint64_t result = 0;
	for (int i = 0; i < bytes; i++)
		result |= buf[i] >> (((bytes - 1) - i) * 8);
	return result;
}

uint64_t
get_uint64(uint8_t *buf)
{
	return get_unsigned(buf, 8);
}

uint32_t
get_uint32(uint8_t *buf)
{
	return get_unsigned(buf, 4);
}

uint64_t
bench_raw_request(int fd, size_t req_size, const uint8_t *req, size_t res_size, const uint8_t *res)
{
	uint8_t *buf = res_size  == 0 ? NULL : calloc(1, res_size);
	struct timespec t0 = bench_start();
	uint64_t result;
	write(fd, req, req_size);
	if (res_size != 0) {
		recv(fd, buf, res_size, MSG_WAITALL);
		result = bench_finish(t0);
		if (memcmp(buf, res, res_size)) {
			printf("Got:\n");
			DumpHex(buf, res_size);
			printf("Expected:\n");
			DumpHex(res, res_size);
			ERROR_FATAL("Unexpected response.");
		}
		free(buf);
	} else {
		size_t data_size = -1;
		size_t data_size_size = -1;
		uint8_t data_size_and_possibly_data[9];
		read(fd, data_size_and_possibly_data, sizeof(data_size_and_possibly_data));
		if (data_size_and_possibly_data[0] == 0xce) {
			data_size = get_uint32(&data_size_and_possibly_data[1]);
			data_size_size = 5;
		} else if (data_size_and_possibly_data[0] == 0xcf) {
			data_size = get_uint64(&data_size_and_possibly_data[1]);
			data_size_size = 9;
		} else {
			ERROR_FATAL("Unexpected packet data_size encoding: %02hhx\n", data_size_and_possibly_data[0]);
		}
		size_t data_bytes_read = sizeof(data_size_and_possibly_data) - data_size_size;
		size_t data_bytes_remained = data_size - data_bytes_read;
		static uint8_t data[1024];
		if (data_bytes_remained > sizeof(data))
			ERROR_FATAL("Couldn't read the packet into the static buffer.\n");
		if (read(fd, data, data_bytes_remained) != data_bytes_remained)
			ERROR_FATAL("Read less than epected.\n");
		result = bench_finish(t0);
	}
	return result;
}

#define IPROTO_ENCODE_WHATEVER(what, data, ...) \
	if (*data == NULL) { \
		return mp_sizeof_ ## what(__VA_ARGS__); \
	} else { \
		size_t _size = mp_encode_ ## what(*data, ## __VA_ARGS__) - (char *)*data; \
		*data += _size; \
		return _size; \
	} \

size_t
iproto_encode_map(uint8_t **data, uint32_t size)
{
	IPROTO_ENCODE_WHATEVER(map, data, size);
}

size_t
iproto_encode_uint(uint8_t **data, uint64_t value)
{
	IPROTO_ENCODE_WHATEVER(uint, data, value);
}

size_t
iproto_encode_array(uint8_t **data, uint32_t size)
{
	IPROTO_ENCODE_WHATEVER(array, data, size);
}

size_t
iproto_encode_str0(uint8_t **data, const char *str)
{
	if (*data == NULL) {
		return mp_sizeof_str(strlen(str));
	} else {
		size_t size = mp_encode_str0(*data, str) - (char *)*data;
		*data += size;
		return size;
	}
}

#undef IPROTO_ENCODE_WHATEVER

size_t
iproto_encode_header(uint8_t **data, int request_type, int sync)
{
	size_t result = 0;
	result += iproto_encode_map(data, 2);
	result += iproto_encode_uint(data, IPROTO_REQUEST_TYPE);
	result += iproto_encode_uint(data, request_type);
	result += iproto_encode_uint(data, IPROTO_SYNC);
	result += iproto_encode_uint(data, sync);
	return result;
}

size_t
iproto_encode_ping_body(uint8_t **data)
{
	return iproto_encode_map(data, 0);
}

size_t
iproto_encode_call_body(uint8_t **data, const char *function_name)
{
	size_t result = 0;
	result += iproto_encode_map(data, 2);
	result += iproto_encode_uint(data, IPROTO_FUNCTION_NAME);
	result += iproto_encode_str0(data, function_name);
	result += iproto_encode_uint(data, IPROTO_TUPLE);
	result += iproto_encode_array(data, 0);
	return result;
}

#define IPROTO_WRITE_WHATEVER(what, data, sync, ...) \
	size_t result = 0; \
	result += iproto_encode_header(&data, IPROTO_ ## what, sync); \
	result += iproto_encode_ ## what ## _body(&data, ## __VA_ARGS__); \
	return result;

size_t
iproto_write_ping(uint8_t *data, int sync)
{
	IPROTO_WRITE_WHATEVER(ping, data, sync);
}

size_t
iproto_write_call(uint8_t *data, int sync, const char *function_name)
{
	IPROTO_WRITE_WHATEVER(call, data, sync, function_name);
}

#undef IPROTO_WRITE_WHATEVER

#define BENCH_CREATE_WHATEVER(what, sync, ...) \
	size_t packet_size = iproto_write_ ## what(NULL, sync, ## __VA_ARGS__); \
	size_t packet_size_size = mp_sizeof_uint(packet_size); \
	size_t request_size = packet_size_size + packet_size; \
	uint8_t *request = calloc(request_size, 1); \
	uint8_t *packet = mp_encode_uint(request, packet_size); \
	iproto_write_ ## what(packet, sync, ## __VA_ARGS__); \
	return (struct Data){ request, request_size };

struct Data
bench_create_call(int sync, const char *function_name)
{
	BENCH_CREATE_WHATEVER(call, sync, function_name);
}

struct Data
bench_create_ping(int sync)
{
	BENCH_CREATE_WHATEVER(ping, sync);
}

#undef BENCH_CREATE_WHATEVER

uint64_t
bench_exec_nocheck(int fd, struct Data data)
{
	return bench_raw_request(fd, data.raw_req_size, data.raw_req, 0, NULL);
}

void
bench(int fd, struct Data request, int count)
{
	uint64_t ns_first = bench_exec_nocheck(fd, request);
	uint64_t ns_min = ns_first;
	uint64_t ns_max = ns_first;
	uint64_t ns_sum = 0;
	for (int i = 1; i < count; i++) {
		size_t ns = bench_exec_nocheck(fd, request);
		if (ns_max < ns) {
			ns_max = ns;
		}
		if (ns_min > ns) {
			ns_min = ns;
		}
		ns_sum += ns;
	}
	uint64_t ns_avg = ns_sum / count;
	printf("First: %lu\n", ns_first);
	printf("Max: %lu\n", ns_max);
	printf("Avg: %lu\n", ns_avg);
	printf("Min: %lu\n", ns_min);
}

int
main(int argc, char **argv)
{
	int fd = bench_connect("localhost", 3301);

	struct Data ping = bench_create_ping(0);
	struct Data call_lua = bench_create_call(0, "lua_func");
	struct Data call_c = bench_create_call(0, "counter_2");

	//bench_exec_nocheck(fd, ping);
	bench(fd, ping, 1000000);

	return 0;
}
