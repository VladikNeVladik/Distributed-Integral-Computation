// No copyright. Vladislav Aleinik 2020
//=======================================================
// Computing Cluster Master                          
//=======================================================
// - Performs discovery broadcast
// - Accepts the needed amount of worker connections
//     - Dies in case there is not enough workers 
// - Gives out computational tasks
// - Waits for computation results
// - Quits once all results are accumulated
// 
// - Dies on worker death or disconnection
//=======================================================
#ifndef COMPUTING_CLUSTER_DOWNGRADED_MASTER_HPP_INCLUDED
#define COMPUTING_CLUSTER_DOWNGRADED_MASTER_HPP_INCLUDED

// Feature test macros:
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif

// Calloc:
#include <stdlib.h>
// Socket:
#include <sys/types.h> 
#include <sys/socket.h>
// TCP keepalive options:
#include <netinet/in.h>
#include <netinet/tcp.h>
// Alarm:
#include <unistd.h>
// Signals:
#include <signal.h>
// Byteswapping:
#include <endian.h>
// Errno:
#include <errno.h>

// Logging facility:
#include "Logging.h"

// Constants && stuff:
#include "Values.h"

//---------------
// Master Handle 
//---------------

struct WorkerInfo
{
	int conn_sock_fd;

	uint32_t num_threads;
	uint32_t task_i;
};

struct MasterHandle
{
	int accept_sock_fd;

	unsigned max_workers;
	unsigned num_workers;
	
	struct WorkerInfo* workers;
	uint32_t total_threads;
};

//-----------
// Discovery 
//-----------

void perform_discovery_broadcast()
{
	// Acquire discovery socket:
	int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock_fd == -1)
	{
		LOG_ERROR("[perform_discovery_broadcast] Unable to create discovery socket");
		exit(EXIT_FAILURE);
	}

	// Enable broadcast on a socket:
	int setsockopt_yes = 1;
	if (setsockopt(sock_fd, SOL_SOCKET, SO_BROADCAST, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[perform_discovery_broadcast] Unable to set SO_BROADCAST socket option");
		exit(EXIT_FAILURE);
	}

	// Disable the TIME-WAIT state of a socket:
	if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[perform_discovery_broadcast] Unable to set SO_REUSEADDR socket option");
		exit(EXIT_FAILURE);
	}

	// Connect to broadcast address:
	struct sockaddr_in master_addr =
	{
		.sin_family      = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_BROADCAST),
		.sin_port        = htons(CONNECTION_PORT)
	};

	if (bind(sock_fd, (struct sockaddr*) &master_addr, sizeof(master_addr)) == -1)
	{
		LOG_ERROR("[perform_discovery_broadcast] Unable to bind()");
		exit(EXIT_FAILURE);
	}

	struct sockaddr_in broadcast_addr =
	{
		.sin_family      = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_BROADCAST),
		.sin_port        = htons(DISCOVERY_WORKER_PORT)
	};

	// Perform discovery sendout:
	if (sendto(sock_fd, MASTERS_DISCOVERY_DATAGRAM, MASTERS_DISCOVERY_DATAGRAM_SIZE, MSG_NOSIGNAL, &broadcast_addr, sizeof(broadcast_addr)) == -1)
	{
		LOG_ERROR("[perform_discovery_broadcast] Unable to send() discovery datagram");
		exit(EXIT_FAILURE);
	}

	LOG("Performed discovery sendout");
}

//-----------------------
// Connection Management 
//-----------------------

void init_connection_management(struct MasterHandle* handle)
{
	// Check prerequisites:
	if (handle->max_workers == 0)
	{
		LOG_ERROR("[init_connection_management] Number of workers is zero");
		exit(EXIT_FAILURE);
	}

	// Create listening socket:
	handle->accept_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (handle->accept_sock_fd == -1)
	{
		LOG_ERROR("[init_connection_management] Unable to create socket()");
		exit(EXIT_FAILURE);
	}

	// Acquire address && bind:
	struct sockaddr_in broadcast_addr;
	broadcast_addr.sin_family      = AF_INET;
	broadcast_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	broadcast_addr.sin_port        = htons(CONNECTION_PORT);

	if (bind(handle->accept_sock_fd, (struct sockaddr*) &broadcast_addr, sizeof(broadcast_addr)) == -1)
	{
		LOG_ERROR("[init_connection_management] Unable to bind()");
		exit(EXIT_FAILURE);
	}

	// Disable the TIME-WAIT state of a socket:
	int setsockopt_yes = 1;
	if (setsockopt(handle->accept_sock_fd, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[init_connection_management] Unable to set SO_REUSEADDR socket option");
		exit(EXIT_FAILURE);
	}

	// Listen for incoming connections:
	if (listen(handle->accept_sock_fd, handle->max_workers) == -1)
	{
		LOG_ERROR("[init_connection_management] Unable to listen() on a socket");
		exit(EXIT_FAILURE);
	}

	// Allocate array for connection sockets:
	handle->workers = (struct WorkerInfo*) calloc(handle->max_workers, sizeof(*handle->workers));
	if (handle->workers == NULL)
	{
		LOG_ERROR("[init_connection_management] Unable to allocate memory for workers");
		exit(EXIT_FAILURE);
	}

	// Set current amount of workers:
	handle->num_workers = 0;

	LOG("Initialised connection management");
}

void stop_connection_management(struct MasterHandle* handle)
{
	free(handle->workers);
}

void connection_loss_handler(int signal, siginfo_t* info, void* arg)
{
	if (signal == SIGIO && info->si_code == POLL_ERR)
	{
		LOG("Worker is dead. Shutdown");
		exit(EXIT_FAILURE);
	}
}

void accept_incoming_connection_request(struct MasterHandle* handle)
{
	// Accept connection on listening socket:
	int worker_sock_fd = accept(handle->accept_sock_fd, NULL, NULL);
	if (worker_sock_fd == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to accept() connection on a socket");
		exit(EXIT_FAILURE);
	}

	// Block all signals for signal handling:
	sigset_t block_all_signals;
	if (sigfillset(&block_all_signals) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to fill signal mask");
		exit(EXIT_FAILURE);
	}

	// Set SIGIO handler:
	struct sigaction act = 
	{
		.sa_sigaction = connection_loss_handler,
		.sa_mask      = block_all_signals,
		.sa_flags     = SA_SIGINFO|SA_RESTART
	};
	if (sigaction(SIGIO, &act, NULL) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set SIGIO handler");
		exit(EXIT_FAILURE);
	}

	// Enable generation of signals on the socket:
	if (fcntl(worker_sock_fd, F_SETFL, O_ASYNC) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set O_ASYNC flag via fcntl()");
		exit(EXIT_FAILURE);
	}

	if (fcntl(worker_sock_fd, F_SETSIG, SIGIO) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set enable additional info for SIGIO");
		exit(EXIT_FAILURE);
	}

	// Set this process as owner of SIGIO:
	if (fcntl(worker_sock_fd, F_SETOWN, getpid()) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set change the owner of SIGIO");
		exit(EXIT_FAILURE);
	}

	// Disable the TIME-WAIT state of a socket:
	int setsockopt_yes = 1;
	if (setsockopt(worker_sock_fd, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set SO_REUSEADDR socket option");
		exit(EXIT_FAILURE);
	}

	// Enable TCP-keepalive:
	if (setsockopt(worker_sock_fd, SOL_SOCKET, SO_KEEPALIVE, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set SO_KEEPALIVE socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_KEEPIDLE, &TCP_KEEPALIVE_IDLE_TIME, sizeof(TCP_KEEPALIVE_IDLE_TIME)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set TCP_KEEPIDLE socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_KEEPINTVL, &TCP_KEEPALIVE_INTERVAL, sizeof(TCP_KEEPALIVE_INTERVAL)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set TCP_KEEPINTVL socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_KEEPCNT, &TCP_KEEPALIVE_NUM_PROBES, sizeof(TCP_KEEPALIVE_NUM_PROBES)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set TCP_KEEPCNT socket option");
		exit(EXIT_FAILURE);
	}

	// Set timeout to wait for unaknowledged sends:
	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &TCP_NO_SEND_ACKS_TIMEOUT, sizeof(TCP_NO_SEND_ACKS_TIMEOUT)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to set TCP_USER_TIMEOUT socket option");
		exit(EXIT_FAILURE);
	}

	// Disable socket lingering:
	struct linger linger_params =
	{
		.l_onoff  = 1,
		.l_linger = 0
	};
	if (setsockopt(worker_sock_fd, SOL_SOCKET, SO_LINGER, &linger_params, sizeof(linger_params)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to disable SO_LINGER socket option");
		exit(EXIT_FAILURE);
	}

	int setsockopt_arg = 0;
	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_LINGER2, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to disable TCP_LINGER2 socket option");
		exit(EXIT_FAILURE);
	}

	// Disable Nagle's algorithm:
	setsockopt_arg = 0;
	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_NODELAY, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to disable TCP_NODELAY socket option");
		exit(EXIT_FAILURE);
	}

	// Disable corking:
	setsockopt_arg = 0;
	if (setsockopt(worker_sock_fd, IPPROTO_TCP, TCP_CORK, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[accept_incoming_connection_request] Unable to disable TCP_CORK socket option");
		exit(EXIT_FAILURE);
	}

	// Update state variables:
	handle->workers[handle->num_workers].conn_sock_fd = worker_sock_fd;
	handle->num_workers += 1;

	// Kill listening socket in case all the requested workers are collected
	if (handle->num_workers == handle->max_workers)
	{
		if (close(handle->accept_sock_fd) == -1)
		{
			LOG_ERROR("[accept_incoming_connection_request] Unable to close() accept-socket");
			exit(EXIT_FAILURE);
		}
	}

	LOG("Accepted %u/%u connections", handle->num_workers, handle->max_workers);
}

void shutdown_connection(struct MasterHandle* handle, unsigned worker_i)
{
	if (close(handle->workers[worker_i].conn_sock_fd) == -1)
	{
		LOG_ERROR("[shutdown_connection_success] Unable to close() connection socket");
		exit(EXIT_FAILURE);
	}

	LOG("Shut down connection #%u", worker_i);
}

void not_enough_workers_handler(int signal)
{
	(void) signal;

	LOG("Didn't collect enough workers in %d seconds. Shutdown", NOT_ENOUGH_WORKERS_TIMEOUT);
	exit(EXIT_FAILURE);
}

void arm_not_enough_workers_timeout(struct MasterHandle* handle)
{
	// Block all signals for signal-handling:
	sigset_t block_all_signals;
	if (sigfillset(&block_all_signals) == -1)
	{
		LOG_ERROR("[arm_not_enough_workers_timeout] Unable to set signal mask");
		exit(EXIT_FAILURE);
	}

	// Set SIGALRM handler:
	struct sigaction act = 
	{
		.sa_handler = not_enough_workers_handler,
		.sa_mask    = block_all_signals,
		.sa_flags   = 0
	};
	if (sigaction(SIGALRM, &act, NULL) == -1)
	{
		LOG_ERROR("[arm_not_enough_workers_timeout] Unable to set SIGALRM handler");
		exit(EXIT_FAILURE);
	}

	// Set the alarm:
	alarm(NOT_ENOUGH_WORKERS_TIMEOUT);

	LOG("Set \"Not Enough Workers\" Timeout");
}

void disarm_not_enough_workers_timeout(struct MasterHandle* handle)
{
	// Disarm the timer:
	alarm(0);

	LOG("Disarmed \"Not Enough Workers\" Timeout");
}

//-----------------
// Task Management 
//-----------------

struct Task
{
	double from;
	double to;
	double step;
}  __attribute__((packed));

struct Result
{
	double sum;
}  __attribute__((packed));

void recv_num_threads(struct MasterHandle* handle, unsigned worker_i)
{
	uint32_t num_threads = 0;
	int bytes_read = recv(handle->workers[worker_i].conn_sock_fd, &num_threads, sizeof(num_threads), MSG_WAITALL);
	// if (bytes_read == 0)
	// {
	// 	LOG("Worker is dead. Shutdown");
	// 	exit(EXIT_FAILURE);
	// }
	if (bytes_read != sizeof(num_threads))
	{
		LOG_ERROR("[recv_num_threads] Unable to recv() number of worker threads");
		exit(EXIT_FAILURE);
	}

	handle->workers[worker_i].num_threads = be32toh(num_threads);
	handle->total_threads                += be32toh(num_threads);

	if (worker_i == 0)
	{
		handle->workers[worker_i].task_i = 0;
	}
	else
	{
		handle->workers[worker_i].task_i = handle->workers[worker_i - 1].task_i +
		                                   handle->workers[worker_i - 1].num_threads;
	}

	LOG("Worker #%u has %u threads", worker_i, handle->workers[worker_i].num_threads);
}

void send_computation_tasks(struct MasterHandle* handle, unsigned worker_i, struct Task* global_task)
{
	double thread_interval = (global_task->to - global_task->from)/handle->total_threads;

	struct Task worker_task =
	{
		.from = global_task->from + thread_interval *  handle->workers[worker_i].task_i,
		.to   = global_task->from + thread_interval * (handle->workers[worker_i].task_i + handle->workers[worker_i].num_threads),
		.step = global_task->step
	};

	uint64_t* from = (uint64_t*) &worker_task.from;
	uint64_t* to   = (uint64_t*) &worker_task.to;
	uint64_t* step = (uint64_t*) &worker_task.step;

	*from = htobe64(*from);
	*to   = htobe64(*to  );
	*step = htobe64(*step);		

	if (send(handle->workers[worker_i].conn_sock_fd, &worker_task, sizeof(worker_task), MSG_NOSIGNAL) != sizeof(worker_task))
	{
		// if (errno == ECONNRESET || errno == EPIPE)
		// {
		// 	LOG("Master is dead. Shutdown");
		// 	exit(EXIT_FAILURE);
		// }

		LOG_ERROR("[send_computation_tasks] Unable to send() computation tasks");
		exit(EXIT_FAILURE);
	}

	LOG("Sent computation tasks to worker #%u", worker_i);
}

double recv_computation_results(struct MasterHandle* handle, unsigned worker_i)
{
	// Get computation result:
	uint64_t result;
	int bytes_read = recv(handle->workers[worker_i].conn_sock_fd, &result, sizeof(result), MSG_WAITALL);
	// if (bytes_read == 0)
	// {
	// 	LOG("Worker is dead. Shutdown");
	// 	exit(EXIT_FAILURE);
	// }
	if (bytes_read != sizeof(result))
	{
		LOG_ERROR("[recv_computation_results] Unable to recv() computation results");
		exit(EXIT_FAILURE);
	}

	result = be64toh(result);
	double toReturn = *((double*) &result);

	// Shut down the connection afterwards:
	if (close(handle->workers[worker_i].conn_sock_fd) == -1)
	{
		LOG_ERROR("[recv_computation_results] Unable to close() socket");
		exit(EXIT_FAILURE);
	}

	LOG("Recieved results from worker #%u", worker_i);

	return toReturn;
}


#endif // COMPUTING_CLUSTER_DOWNGRADED_MASTER_HPP_INCLUDED