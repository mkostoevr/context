LUA_H_INCLUDE_DIR=/home/magomed/Sources/work/tarantool-ee/tarantool/third_party/luajit/src
MODULE_H_INCLUDE_DIR=/home/magomed/Sources/work/tarantool-ee/build_rwdi/tarantool/src

.PHONY: all

all:
	gcc -shared -o procs.so -fPIC procs.c -I "${LUA_H_INCLUDE_DIR}" -I "${MODULE_H_INCLUDE_DIR}"
	gcc ../common/msgpuck/hints.c ../common/msgpuck/msgpuck.c test.c -o test.exe -ggdb -Os -I "../common"
