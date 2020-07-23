// No copyright. Vladislav Aleinik 2020
#include "Master.h"

#include <stdlib.h>
#include <stdio.h>

int main(int argc, char** argv)
{
	if (argc != 2)
	{
		fprintf(stderr, "Usage: computation-master <num-workers>");
		exit(EXIT_FAILURE);
	}

	// Parse number of workers:
	char* endptr = argv[1];
	long max_workers = strtol(argv[1], &endptr, 10);
	if (*argv[1] == '\0' || *endptr != '\0')
	{
		fprintf(stderr, "Unable to parse number of workers!\n");
		exit(EXIT_FAILURE);
	}

	// Set log file:
	set_log_file("log/MASTER-LOG.log");

	// Initialise handle:
	struct MasterHandle master =
	{
		.max_workers = max_workers
	};

	init_connection_management(&master);

	// Perform discovery:
	perform_discovery_broadcast(&master);

	// Set timer for worker collection:
	arm_not_enough_workers_timeout(&master);

	// Collect all the workers:
	for (unsigned worker_i = 0; worker_i < max_workers; ++worker_i)
	{
		accept_incoming_connection_request(&master);
	}

	// Disarm the timer for worker collection:
	disarm_not_enough_workers_timeout(&master);

	// Computation parameters:
	struct Task global_task =
	{
		.from =   0.0,
		.to   = 100.0,	
		.step = 0.00000002
	};

	// Give out computational tasks:
	for (unsigned worker_i = 0; worker_i < max_workers; ++worker_i)
	{
		recv_num_threads(&master, worker_i);
	}
	
	for (unsigned worker_i = 0; worker_i < max_workers; ++worker_i)
	{
		send_computation_tasks(&master, worker_i, &global_task);
	}

	// Wait for results:
	double sum = 0.0;
	for (unsigned worker_i = 0; worker_i < max_workers; ++worker_i)
	{
		sum += recv_computation_results(&master, worker_i);
	}

	// Finish:
	stop_connection_management(&master);

	// Print result:
	LOG("Computation result is: %7.3lf", sum);

	return EXIT_SUCCESS;
}