// No Copyright. Vladislav Aleinik 2020
//=====================================
// Logging Utilities
//=====================================
#ifndef COMPUTING_CLUSTER_DOWNGRADED_LOGGING_HPP_INCLUDED
#define COMPUTING_CLUSTER_DOWNGRADED_LOGGING_HPP_INCLUDED

#include <stdlib.h>
// Dprinf:
 #include <stdio.h>
// Time:
#include <sys/time.h>
#include <time.h>
// Strcmp:
#include <string.h>
// Open:
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// Close:
#include <unistd.h>

//----------------------
// Log Levels 
//----------------------
// 0 - Logging Disabled
// 1 - Logging Enabled
//----------------------

#define LOG_LEVEL 1

//------------------------
// Log File Manipulations 
//------------------------

const char* LOG_FILE = "LOG.txt";

__attribute__((unused)) static FILE* acquire_log_file(const char* input_format)
{
#ifndef LOG_TO_STDOUT
	FILE* log_file = NULL;

	// Get current time:
	struct timeval cur_time;

	if (gettimeofday(&cur_time, NULL) == -1)
	{
		fprintf(stderr, "[ERROR] Unable to get time of day\n");
		exit(EXIT_FAILURE);
	}

	struct tm* broken_down_time = localtime(&cur_time.tv_sec);
	if (broken_down_time == NULL)
	{
		fprintf(stderr, "[ERROR] Unable to get broken-down time\n");
		exit(EXIT_FAILURE);
	}

	// Get a nice readable time string:
	char time_str_buf[128];
	if (strftime(time_str_buf, sizeof(time_str_buf), "%Y-%m-%d %H:%M:%S", broken_down_time) == 0)
	{
       fprintf(stderr, "[ERROR] Unable to get a nice readable time string\n");
       exit(EXIT_FAILURE);
   	}

	if (log_file == NULL)
	{
		log_file = fopen(LOG_FILE, "w");
		if (log_file == NULL)
		{
			fprintf(stderr, "[ERROR %s:%06ld] Unable to open log file %s\n", time_str_buf, cur_time.tv_usec, LOG_FILE);
			exit(EXIT_FAILURE);
		}

		fprintf(log_file, "[LOG %s:%06ld] Opened log file %s\n", time_str_buf, cur_time.tv_usec, LOG_FILE);
	}
	
	if (strcmp(input_format, "Closed log file") == 0 && log_file != NULL)
	{
		fprintf(log_file, "[LOG %s:%06ld] Closed log file %s\n", time_str_buf, cur_time.tv_usec, LOG_FILE);
		fclose(log_file);
		log_file = NULL;
	}

	return log_file;
#else
	return stdout;
#endif
}

// Ultra-super-duper hack to allow semicolon after macro:
// LOG_ERROR("BRUH"); <- like this
__attribute__((unused)) static void nop() {}

//----------------
// Logging Macros 
//----------------

#if LOG_LEVEL == 0
#define LOG(format, ...)

#elif LOG_LEVEL == 1 
#define LOG(format, ...)																					\
{																											\
	FILE* __log_file = acquire_log_file(format);															\
																											\
	struct timeval __cur_time;																				\
																											\
	if (gettimeofday(&__cur_time, NULL) == -1)																\
	{																										\
		fprintf(stderr, "[ERROR] Unable to get time of day\n");												\
		exit(EXIT_FAILURE);																					\
	}																										\
																											\
	struct tm* __broken_down_time = localtime(&__cur_time.tv_sec);											\
	if (__broken_down_time == NULL)																			\
	{																										\
		fprintf(stderr, "[ERROR] Unable to get broken-down time\n");										\
		exit(EXIT_FAILURE);																					\
	}																										\
																											\
	char __time_str_buf[128];																				\
	if (strftime(__time_str_buf, sizeof(__time_str_buf), "%Y-%m-%d %H:%M:%S", __broken_down_time) == 0)		\
	{																										\
       fprintf(stderr, "[ERROR] Unable to get a nice readable time string\n");								\
       exit(EXIT_FAILURE);																					\
   	}																										\
																											\
	fprintf(__log_file, "[LOG %s:%06ld] "format"\n", __time_str_buf, __cur_time.tv_usec, ##__VA_ARGS__);\
} nop()

#endif

#define LOG_ERROR(format, ...)																					\
{																												\
	struct timeval __cur_time;																					\
																												\
	if (gettimeofday(&__cur_time, NULL) == -1)																	\
	{																											\
		fprintf(stderr, "[ERROR] Unable to get time of day\n");													\
		exit(EXIT_FAILURE);																						\
	}																											\
																												\
	struct tm* __broken_down_time = localtime(&__cur_time.tv_sec);												\
	if (__broken_down_time == NULL)																				\
	{																											\
		fprintf(stderr, "[ERROR] Unable to get broken-down time\n");											\
		exit(EXIT_FAILURE);																						\
	}																											\
																												\
	char __time_str_buf[128];																					\
	if (strftime(__time_str_buf, sizeof(__time_str_buf), "%Y-%m-%d %H:%M:%S", __broken_down_time) == 0)			\
	{																											\
       fprintf(stderr, "[ERROR] Unable to get a nice readable time string\n");									\
       exit(EXIT_FAILURE);																						\
   	}																											\
																												\
	fprintf(stderr, "[ERROR %s:%06ld] "format"\n", __time_str_buf, __cur_time.tv_usec, ##__VA_ARGS__);			\
} nop()

#define BUG_ON(condition, format, ...)																			\
{																												\
	if (condition)																								\
	{																											\
		struct timeval __cur_time;																				\
																												\
		if (gettimeofday(&__cur_time, NULL) == -1)																\
		{																										\
			fprintf(stderr, "[ERROR] Unable to get time of day\n");												\
			exit(EXIT_FAILURE);																					\
		}																										\
																												\
		struct tm* __broken_down_time = localtime(&__cur_time.tv_sec);											\
		if (__broken_down_time == NULL)																			\
		{																										\
			fprintf(stderr, "[ERROR] Unable to get broken-down time\n");										\
			exit(EXIT_FAILURE);																					\
		}																										\
																												\
		char __time_str_buf[128];																				\
		if (strftime(__time_str_buf, sizeof(__time_str_buf), "%Y-%m-%d %H:%M:%S", __broken_down_time) == 0)		\
		{																										\
	       fprintf(stderr, "[ERROR] Unable to get a nice readable time string\n");								\
	       exit(EXIT_FAILURE);																					\
	   	}																										\
																												\
		fprintf(stderr, "[BUG %s:%06ld] "format"\n", __time_str_buf, __cur_time.tv_usec, ##__VA_ARGS__);		\
																												\
		exit(EXIT_FAILURE);																						\
	}																											\
} nop()

//-----------------------------
// Dynamically update log file
//-----------------------------

int set_log_file(const char* log_file)
{
	if (log_file == NULL)
	{
		time_t cur_time = time(NULL);
		fprintf(stderr, "[ERROR %s\r][set_log_file] Null log file name\n", ctime(&cur_time));
		return -1;
	}

	LOG_FILE = log_file;

	LOG("Changed log file to %s", log_file);

	return 0;
}

#endif // COMPUTING_CLUSTER_DOWNGRADED_LOGGING_HPP_INCLUDED