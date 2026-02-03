LUA_H_INCLUDE_DIR=/home/magomed/Sources/work/tarantool-ee/tarantool/third_party/luajit/src
MODULE_H_INCLUDE_DIR=/home/magomed/Sources/work/tarantool-ee/build_rwdi/tarantool/src
THIRD_PARTY_DIR=/home/magomed/Sources/work/tarantool-ee/tarantool/third_party

.PHONY: all

all:
	g++ -shared -o procs.so -fPIC procs.cc -ggdb -O2 \
		../common/msgpuck/hints.c \
		../common/msgpuck/msgpuck.c \
		-I "${LUA_H_INCLUDE_DIR}" \
		-I "${MODULE_H_INCLUDE_DIR}" \
		-I "${THIRD_PARTY_DIR}" \
		-I ../common/msgpuck
