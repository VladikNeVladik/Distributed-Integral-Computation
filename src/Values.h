// No copyright. Vladislav Aleinik 2020
//======================================================
// Config File                                     
//======================================================
// - All system configuration is defined here 
//======================================================
#ifndef COMPUTING_CLUSTER_DOWNGRADED_VALUES_HPP_INCLUDED
#define COMPUTING_CLUSTER_DOWNGRADED_VALUES_HPP_INCLUDED

//-------------------
// Discovery Process 
//-------------------

const int DISCOVERY_WORKER_PORT = 9799;

const char   MASTERS_DISCOVERY_DATAGRAM[]    = "I am here, CLUSTER-WORKER!";
const size_t MASTERS_DISCOVERY_DATAGRAM_SIZE = sizeof(MASTERS_DISCOVERY_DATAGRAM)/sizeof(char);

//-----------------------
// Connection Management 
//-----------------------

const int CONNECTION_PORT = 9798;

// TCP-keepalive attributes:
const int TCP_KEEPALIVE_IDLE_TIME  = 1; // sec
const int TCP_KEEPALIVE_INTERVAL   = 1; // sec
const int TCP_KEEPALIVE_NUM_PROBES = 4;

// TCP user-timeout:
const unsigned TCP_NO_SEND_ACKS_TIMEOUT = 5000; // ms

// "Not Enough Workers" Timeout:
const unsigned NOT_ENOUGH_WORKERS_TIMEOUT = 5; // sec


#endif // COMPUTING_CLUSTER_DOWNGRADED_VALUES_HPP_INCLUDED