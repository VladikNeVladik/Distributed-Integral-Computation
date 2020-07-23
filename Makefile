#================#
# COMPILER FLAGS #
#================#

CCFLAGS += -std=c11 -Werror -Wall -pthread -ggdb

#================#
# DEFAULT TRAGET #
#================#

default : compile

#==============#
# INSTALLATION #
#==============#

install: 
	@- mkdir bin log
	@ printf "\033[1;33mInstallation complete!\033[0m\n"

clean:
	@- rm -rf bin log
	@ printf "\033[1;33mCleaning complete!\033[0m\n"

#=============#
# COMPILATION #
#=============#

HEADERS = src/Logging.h src/Values.h

bin/computation-master.out : src/computation-master.c src/Master.h ${HEADERS}
	gcc -D=LOG_TO_STDOUT ${CCFLAGS} $< -o $@

bin/computation-worker.out : src/computation-worker.c src/Worker.h ${HEADERS}
	gcc -D=LOG_TO_STDOUT ${CCFLAGS} $< -o $@

#======================#
# INTEGRAL COMPUTATION #
#======================#

run_master : bin/computation-master.out
	@ printf "\033[1;33mMaster Running!\033[0m\n"
	@ bin/computation-master.out 1
	@ printf "\033[1;33mMaster Finished!\033[0m\n"

run_worker : bin/computation-worker.out 
	@ printf "\033[1;33mWorker Running!\033[0m\n"
	@ bin/computation-worker.out 5
	@ printf "\033[1;33mWorker Finished!\033[0m\n"


