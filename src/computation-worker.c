// No copyright. Vladislav Aleinik 2020
#include "Worker.h"

#include <stdlib.h>
#include <stdio.h>

int main(int argc, char** argv)
{
	if (argc != 2)
	{
		fprintf(stderr, "Usage: computation-worker <num-threads>");
		exit(EXIT_FAILURE);
	}

	// Parse number of threads:
	char* endptr = argv[1];
	long num_threads = strtol(argv[1], &endptr, 10);
	if (*argv[1] == '\0' || *endptr != '\0')
	{
		fprintf(stderr, "Unable to parse number of threads!\n");
		exit(EXIT_FAILURE);
	}

	// Set log file:
	set_log_file("log/WORKER-LOG.log");

	// Initialise worker handle:
	struct WorkerHandle worker =
	{
		.num_threads = num_threads
	};

	// Connect to master:
	do
	{
		discover_server(&worker);
	}
	while (connect_to_master(&worker) == -1);

	// Manage computations:
	send_num_threads(&worker);

	recv_computation_tasks(&worker);

	perform_computations(&worker);

	send_computation_results(&worker);

	// Wait for server to kill connection:
	shutdown_connection(&worker);

	return EXIT_SUCCESS;
}
