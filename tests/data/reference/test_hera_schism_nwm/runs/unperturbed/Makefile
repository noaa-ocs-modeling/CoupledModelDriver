# Makefile driver generated by PySCHISM.
MAKEFILE_PATH:=$(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR:=$(dir $(MAKEFILE_PATH))
MPI_LAUNCHER=mpiexec -n 12
SCHISM_BINARY=pschism_TVD-VL

run:
	@set -e;\
	rm -rf outputs/mirror.out outputs/fatal.error;\
	touch outputs/mirror.out outputs/fatal.error;\
	eval 'tail -f outputs/mirror.out  outputs/fatal.error &';\
	tail_pid=$${!};\
	${MPI_LAUNCHER} ${NPROC} ${SCHISM_BINARY};\
	err_code=$${?};\
	kill "$${tail_pid}";\
	exit $${err_code}


tail:
	tail -f outputs/mirror.out  outputs/fatal.error
