// No copyright. Vladislav Aleinik 2020
//===========================================================================================
// Computing Cluster Worker     
//===========================================================================================
// - Waits for discovery datagram from master
//     - Returns to initial waiting state in case there is not enough workers for the master
// - Acquires a bunch of tasks from master
// - Performs computation
// - Sends results back to master
//
// - Dies on master death or disconnection
//===========================================================================================
#ifndef COMPUTING_CLUSTER_DOWNGRADED_WORKER_HPP_INCLUDED
#define COMPUTING_CLUSTER_DOWNGRADED_WORKER_HPP_INCLUDED

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

// Socket:
#include <sys/types.h> 
#include <sys/socket.h>
#include <netdb.h>
// TCP keepalive options:
#include <netinet/in.h>
#include <netinet/tcp.h>
// Signals:
#include <signal.h>
// Byteswapping:
#include <endian.h>
// Threads:
#include <pthread.h>
// Errno:
#include <errno.h>
// Getnprocs:
#include <sys/sysinfo.h>
// sched_yield():
#include <sched.h>

// Logging facility:
#include "Logging.h"

// Constants && stuff:
#include "Values.h"

//---------------
// Worker Handle
//---------------

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

#define CACHE_LINE_SIZE 128

struct ThreadInfo
{
	struct Task task;
	struct Result res;
	pthread_t thread_id;
	char cache_line_padding[CACHE_LINE_SIZE];
};

#undef CACHE_LINE_SIZE

struct WorkerHandle
{
	// Discovery:
	struct sockaddr_in master_addr;

	// Connection management:
	int conn_sock_fd;

	// Task management:
	uint32_t num_threads;
	struct ThreadInfo* threads;
};

//-------------------
// Discovery process
//-------------------

void discover_server(struct WorkerHandle* handle)
{
	// Create broadcast datagram socket:
	int disovery_sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (disovery_sock_fd == -1)
	{
		LOG_ERROR("[discover_server] Unable to create discovery socket");
		exit(EXIT_FAILURE);
	}

	uint64_t setsockopt_yes = 1;
	if (setsockopt(disovery_sock_fd, SOL_SOCKET, SO_BROADCAST, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[discover_server] setsockopt() failed");
		exit(EXIT_FAILURE);
	}

	// Needed to recieve datagrams from master:
	struct sockaddr_in worker_addr =
	{
		.sin_family      = AF_INET,
		.sin_addr.s_addr = htonl(INADDR_ANY),
		.sin_port        = htons(DISCOVERY_WORKER_PORT)
	};
	if (bind(disovery_sock_fd, &worker_addr, sizeof(worker_addr)) == -1)
	{
		LOG_ERROR("[discover_server] Unable to bind()");
		exit(EXIT_FAILURE);
	}

	// Catch master's discovery datagram:
	struct sockaddr_in peer_addr;
	socklen_t peer_addr_len = sizeof(peer_addr);
	char buffer[MASTERS_DISCOVERY_DATAGRAM_SIZE];

	LOG("Waiting for discovery datagram");

	int bytes_read;
	do
	{
		bytes_read = recvfrom(disovery_sock_fd, buffer, MASTERS_DISCOVERY_DATAGRAM_SIZE, 0, &peer_addr, &peer_addr_len);
		if (bytes_read == -1)
		{
			LOG_ERROR("[discover_server] Unable to recieve discovery datagram");
			exit(EXIT_FAILURE);
		}

		buffer[MASTERS_DISCOVERY_DATAGRAM_SIZE - 1] = '\0';
	}
	while (bytes_read != MASTERS_DISCOVERY_DATAGRAM_SIZE || strcmp(buffer, MASTERS_DISCOVERY_DATAGRAM) != 0);

	handle->master_addr = peer_addr;

	// Log discovery:
	char server_host[32];
	char server_port[32];
	if (getnameinfo((struct sockaddr*) &peer_addr, sizeof(peer_addr),
		server_host, 32, server_port, 32, NI_NUMERICHOST|NI_NUMERICSERV) != 0)
	{
		LOG_ERROR("[discover_server] Unable to call getnameinfo()");
		exit(EXIT_FAILURE);
	}

	LOG("Discovered server at %s:%s", server_host, server_port);

}

//-----------------------
// Connection management 
//-----------------------

static int computation_finished = 0;
void shutdown_connection(struct WorkerHandle* handle)
{
	computation_finished = 1;
	while (1) sched_yield();
}

void connection_loss_handler(int signal, siginfo_t* info, void* arg)
{
	if (signal == SIGIO && info->si_code == POLL_ERR)
	{
		if (computation_finished)
		{
			LOG("Computation successful!");
			exit(EXIT_SUCCESS);
		}
		else
		{
			LOG("Master is dead. Shutdown");
			exit(EXIT_FAILURE);
		}
	}
}

int connect_to_master(struct WorkerHandle* handle)
{
	// Create socket:
	handle->conn_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (handle->conn_sock_fd == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to create socket()");
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
	if (fcntl(handle->conn_sock_fd, F_SETFL, O_ASYNC) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set O_ASYNC flag via fcntl()");
		exit(EXIT_FAILURE);
	}

	if (fcntl(handle->conn_sock_fd, F_SETSIG, SIGIO) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to enable additional info for SIGIO");
		exit(EXIT_FAILURE);
	}

	// Set this process as owner of SIGIO:
	if (fcntl(handle->conn_sock_fd, F_SETOWN, getpid()) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set change the owner of SIGIO");
		exit(EXIT_FAILURE);
	}

	// Disable the TIME-WAIT state of a socket:
	int setsockopt_yes = 1;
	if (setsockopt(handle->conn_sock_fd, SOL_SOCKET, SO_REUSEADDR, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set SO_REUSEADDR socket option");
		exit(EXIT_FAILURE);
	}

	// Enable TCP-keepalive:
	if (setsockopt(handle->conn_sock_fd, SOL_SOCKET, SO_KEEPALIVE, &setsockopt_yes, sizeof(setsockopt_yes)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set SO_KEEPALIVE socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_KEEPIDLE, &TCP_KEEPALIVE_IDLE_TIME, sizeof(TCP_KEEPALIVE_IDLE_TIME)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set TCP_KEEPIDLE socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_KEEPINTVL, &TCP_KEEPALIVE_INTERVAL, sizeof(TCP_KEEPALIVE_INTERVAL)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set TCP_KEEPINTVL socket option");
		exit(EXIT_FAILURE);
	}

	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_KEEPCNT, &TCP_KEEPALIVE_NUM_PROBES, sizeof(TCP_KEEPALIVE_NUM_PROBES)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set TCP_KEEPCNT socket option");
		exit(EXIT_FAILURE);
	}

	// Set timeout to wait for unaknowledged sends:
	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &TCP_NO_SEND_ACKS_TIMEOUT, sizeof(TCP_NO_SEND_ACKS_TIMEOUT)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to set TCP_USER_TIMEOUT socket option");
		exit(EXIT_FAILURE);
	}

	// Disable socket lingering:
	struct linger linger_params =
	{
		.l_onoff  = 1,
		.l_linger = 0
	};
	if (setsockopt(handle->conn_sock_fd, SOL_SOCKET, SO_LINGER, &linger_params, sizeof(linger_params)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to disable SO_LINGER socket option");
		exit(EXIT_FAILURE);
	}

	int setsockopt_arg = 0;
	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_LINGER2, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to disable TCP_LINGER2 socket option");
		exit(EXIT_FAILURE);
	}

	// Disable Nagle's algorithm:
	setsockopt_arg = 0;
	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_NODELAY, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to disable TCP_NODELAY socket option");
		exit(EXIT_FAILURE);
	}

	// Disable corking:
	setsockopt_arg = 0;
	if (setsockopt(handle->conn_sock_fd, IPPROTO_TCP, TCP_CORK, &setsockopt_arg, sizeof(setsockopt_arg)) == -1)
	{
		LOG_ERROR("[connect_to_master] Unable to disable TCP_CORK socket option");
		exit(EXIT_FAILURE);
	}

	// Connect to master:
	if (connect(handle->conn_sock_fd, &handle->master_addr, sizeof(handle->master_addr)) == -1)
	{
		if (errno == ECONNREFUSED)
		{
			if (close(handle->conn_sock_fd) == -1)
			{
				LOG_ERROR("[connect_to_master] Unable to close() socket");
				exit(EXIT_FAILURE);
			}

			LOG("Connection to master failed");

			return -1;
		}

		LOG_ERROR("[connect_to_master] Unable to connect() to master");
		exit(EXIT_FAILURE);
	}

	LOG("Connected to master");

	return 0;
}

//-----------------
// Task management 
//-----------------

void send_num_threads(struct WorkerHandle* handle)
{
	uint32_t num_threads = htobe32(handle->num_threads);
	if (send(handle->conn_sock_fd, &num_threads, sizeof(num_threads), MSG_NOSIGNAL) != sizeof(num_threads))
	{
		// if (errno == ECONNRESET || errno == EPIPE)
		// {
		// 	LOG("Master is dead. Shutdown");
		// 	exit(EXIT_FAILURE);
		// }

		LOG_ERROR("[send_num_threads] Unable to send() number of threads");
		exit(EXIT_FAILURE);
	}
}

void recv_computation_tasks(struct WorkerHandle* handle)
{
	// Recieve tasks:
	struct Task tasks;
	int bytes_read = recv(handle->conn_sock_fd, &tasks, sizeof(tasks), MSG_WAITALL);
	// if (bytes_read == 0)
	// {
	// 	LOG("Master is dead. Shutdown");
	// 	exit(EXIT_FAILURE);
	// }
	if (bytes_read != sizeof(tasks))
	{
		LOG_ERROR("[recv_computation_tasks] Unable to recv() number of worker threads");
		exit(EXIT_FAILURE);
	}

	uint64_t* from = (uint64_t*) &tasks.from;
	uint64_t* to   = (uint64_t*) &tasks.to;
	uint64_t* step = (uint64_t*) &tasks.step;

	*from = be64toh(*from);
	*to   = be64toh(*to  );
	*step = be64toh(*step);	

	// Divide tasks evenly among threads:
	handle->threads = (struct ThreadInfo*) calloc(handle->num_threads, sizeof(*handle->threads));
	if (handle->threads == NULL)
	{
		LOG_ERROR("[recv_computation_tasks] Unable to allocate memory for thread information");
		exit(EXIT_FAILURE);
	}

	double thread_interval = (tasks.to - tasks.from)/handle->num_threads;
	for (unsigned thread_i = 0; thread_i < handle->num_threads; ++thread_i)
	{
		handle->threads[thread_i].task.from = tasks.from + (thread_i + 0) * thread_interval;
		handle->threads[thread_i].task.to   = tasks.from + (thread_i + 1) * thread_interval;
		handle->threads[thread_i].task.step = tasks.step;
	}
}

double func(double x)
{
	return x*x;
}

void* computation_task(void* arg)
{
	struct ThreadInfo* info = arg;

	info->res.sum = 0.0;
	for (double x = info->task.from + info->task.step; x < info->task.to; x += info->task.step)
	{
		info->res.sum += func(x) * info->task.step;
	}

	return NULL;
}

void* parasite_task(void* arg)
{
	double trash = 1.0;

	int ever = 1;
	for (;ever;)
	{
		if (((uint64_t) trash) & 0x1)
		{
			trash += trash;
		}
		else
		{
			trash *= trash;
			trash -= trash;
		}
	}

	return NULL;
}

void perform_computations(struct WorkerHandle* handle)
{
	unsigned num_procs = get_nprocs();

	 // Set thread attributes:
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0)
	{
		LOG_ERROR("[perform_computations] Unable to call pthread_attr_init()");
		exit(EXIT_FAILURE);
	}

	for (unsigned i = 0; i < handle->num_threads; ++i)
	{
		cpu_set_t cpu_to_run_on;
		CPU_ZERO(&cpu_to_run_on);
		CPU_SET(i % num_procs, &cpu_to_run_on);
		if (pthread_attr_setaffinity_np(&attr, sizeof(cpu_to_run_on), &cpu_to_run_on) != 0)
		{
			LOG_ERROR("[perform_computations] Unable to call pthread_attr_setaffinity_np()");
			exit(EXIT_FAILURE);
		}

		if (pthread_create(&handle->threads[i].thread_id, &attr, &computation_task, &handle->threads[i]) != 0)
		{
			LOG_ERROR("[perform_computations] Unable to create computation thread");
			exit(EXIT_FAILURE);
		}
	}

	if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0)
	{
		LOG_ERROR("[perform_computations] Unable to set \"detached\" thread attribute");
		exit(EXIT_FAILURE);
	}

	unsigned num_parasites = 0;
	if (handle->num_threads % num_procs != 0)
	{
		for (unsigned i = handle->num_threads % num_procs; i < num_procs; ++i, ++num_parasites)
		{
			cpu_set_t cpu_to_run_on;
			CPU_ZERO(&cpu_to_run_on);
			CPU_SET(i % num_procs, &cpu_to_run_on);
			if (pthread_attr_setaffinity_np(&attr, sizeof(cpu_to_run_on), &cpu_to_run_on) != 0)
			{
				LOG_ERROR("[perform_computations] Unable to call pthread_attr_setaffinity_np()");
				exit(EXIT_FAILURE);
			}

			pthread_t thread_id;
			if (pthread_create(&thread_id, &attr, &parasite_task, NULL) != 0)
			{
				LOG_ERROR("[perform_computations] Unable to create pasrasite thread");
				exit(EXIT_FAILURE);
			}
		}
	}
	
	LOG("Started %u computations and %u parasites", handle->num_threads, num_parasites);

	for (unsigned i = 0; i < handle->num_threads; ++i)
	{
		if (pthread_join(handle->threads[i].thread_id, NULL) == -1)
		{
			LOG_ERROR("[perform_computations] Unable to join() computation thread");
			exit(EXIT_FAILURE);
		}

		LOG("Task on thread #%u finished", i);
	}
}

void send_computation_results(struct WorkerHandle* handle)
{
	double sum = 0.0;
	for (unsigned i = 0; i < handle->num_threads; ++i)
	{
		sum += handle->threads[i].res.sum;
	}

	uint64_t* val = (uint64_t*) &sum;
	*val = htobe64(*val);

	if (send(handle->conn_sock_fd, &sum, sizeof(sum), MSG_NOSIGNAL) != sizeof(sum))
	{
		// if (errno == ECONNRESET || errno == EPIPE)
		// {
		// 	LOG("Master is dead. Shutdown");
		// 	exit(EXIT_FAILURE);
		// }

		LOG_ERROR("[send_computation_results] Unable to send() computation results");
		exit(EXIT_FAILURE);
	}
}


#endif // COMPUTING_CLUSTER_DOWNGRADED_WORKER_HPP_INCLUDED