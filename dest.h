#ifndef DEST_H
#define DEST_H

#include <pthread.h>

typedef struct destination {
	struct destination *next;
	const char *arg, *name, *port, *result;
	int fd;
	pthread_t thread;
} dest_t;

#endif
