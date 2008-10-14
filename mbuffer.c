/*
 *  Copyright (C) 2000-2008, Thomas Maier-Komor
 *
 *  This is the source code of mbuffer.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"
#define _GNU_SOURCE 1
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <malloc.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <termios.h>
#include <unistd.h>

#ifdef HAVE_SENDFILE
#ifdef HAVE_SENDFILE_H
#include <sys/sendfile.h>
#endif
/* if this sendfile implementation does not support sending from buffers,
   disable sendfile support */
	#ifndef SFV_FD_SELF
	#ifdef __GNUC__
	#warning sendfile is unable to send from buffers
	#endif
	#undef HAVE_SENDFILE
	#endif
#endif

#ifdef HAVE_LIBMHASH
#include <mhash.h>
#elif defined HAVE_LIBMD5
#include <md5.h>
#elif defined HAVE_LIBCRYPTO
#include <openssl/md5.h>
#endif

/*
 * _POSIX_THREADS is only defined if full thread support is available.
 * We don't need full thread support, so we skip this test...
 * #ifndef _POSIX_THREADS
 * #error posix threads are required
 * #endif
 */

#ifndef _POSIX_SEMAPHORES
#error posix sempahores are required
#endif

/* headers needed for networking */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#ifdef O_LARGEFILE
#define LARGEFILE O_LARGEFILE
#else
#define LARGEFILE 0
#endif

#ifdef O_DIRECT
#define DIRECT O_DIRECT
#else
#define DIRECT 0
#endif

static pthread_t Reader;
static long Verbose = 3, In = 0, Tmp = 0, Pause = 0, Memmap = 0,
	Status = 1, Nooverwrite = O_EXCL, Outblocksize = 0,
	Autoloader = 0, Autoload_time = 0, Sock = 0, OptSync = 0,
	ErrorOccured = 0;
static unsigned long Outsize = 10240;
#if defined HAVE_LIBCRYPTO || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
static long  Hash = 0;
#endif
static volatile int
	Finish = -1,	/* this is for graceful termination */
	Terminate = 0,	/* abort execution, because of error or signal */
	EmptyCount = 0,	/* counter incremented when buffer runs empty */
	FullCount = 0,	/* counter incremented when buffer gets full */
	NumSenders = -1,/* number of sender threads */
	MainOutOK = 1;	/* is the main outputThread still writing or just coordinating senders */
static volatile unsigned long long Rest = 0;
static unsigned long long Blocksize = 10240, MaxReadSpeed = 0,
		     MaxWriteSpeed = 0, OutVolsize = 0;
static volatile unsigned long long Numin = 0, Numout = 0;
static double StartWrite = 0, StartRead = 1;
static char *Tmpfile = 0, **Buffer, *Prefix;
static const char *Infile = 0, *Autoload_cmd = 0;
static int Multivolume = 0, Memlock = 0, Sendout = 0, PrefixLen = 0,
	   Direct = 0, Numblocks = 512, SetOutsize = 0;
#ifdef HAVE_LIBMHASH
static MHASH MD5hash;
#elif defined HAVE_LIBMD5
static MD5_CTX md5ctxt;
#elif defined HAVE_LIBCRYPTO
static MD5_CTX md5ctxt;
#endif

#ifdef __sun
#include <synch.h>
#define sem_t sema_t
#define sem_init(a,b,c) sema_init(a,c,USYNC_THREAD,0)
#define sem_wait sema_wait
#define sem_post sema_post
#define sem_getvalue(a,b) ((*(b) = (a)->count), 0)
#endif

static sem_t Dev2Buf, Buf2Dev, Done;
static pthread_cond_t
	PercLow = PTHREAD_COND_INITIALIZER,	/* low watermark */
	PercHigh = PTHREAD_COND_INITIALIZER,	/* high watermark */
	SendCond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t
#if !defined(__sun) || defined(__linux)
	LogMut = PTHREAD_MUTEX_INITIALIZER,
#endif
	TermMut = PTHREAD_MUTEX_INITIALIZER,	/* prevents statusThread from interfering with request*Volume */
	LowMut = PTHREAD_MUTEX_INITIALIZER,
	HighMut = PTHREAD_MUTEX_INITIALIZER,
	SendMut = PTHREAD_MUTEX_INITIALIZER;
static int Log = -1, Terminal = 1;
static struct timeval Starttime;

typedef struct destination {
	const char *arg, *name, *result;
	int port, fd;
	pthread_t thread;
	struct destination *next;
} dest_t;

static dest_t *Dest = 0;
static char *volatile SendAt = 0;
static volatile int SendSize = 0, ActSenders = 0;

#ifdef __CYGWIN__
#undef assert
#define assert(x) ((x) || (*(char *) 0 = 1))
#endif


#ifdef DEBUG
static void logdebug(const char *msg, ...)
{
	va_list val;
	char buf[256];
	int b = PrefixLen;

	va_start(val,msg);
	(void) memcpy(buf,Prefix,b);
	b += vsnprintf(buf + b,sizeof(buf)-b,msg,val);
	assert(b < sizeof(buf));
#if defined(__sun) || defined(__linux)
	(void) write(Log,buf,b);
#else
	{
		int err;
		err = pthread_mutex_lock(&LogMut);
		assert(err == 0);
		(void) write(Log,buf,b);
		err = pthread_mutex_unlock(&LogMut);
		assert(err == 0);
	}
#endif
	va_end(val);
}
#define debugmsg if (Verbose >= 5) logdebug
#else
#define debugmsg
#endif


static void infomsg(const char *msg, ...)
{
	if (Verbose >= 4) {
		va_list val;
		char buf[256], *b = buf + PrefixLen;

		va_start(val,msg);
		(void) memcpy(buf,Prefix,PrefixLen);
		b += vsnprintf(b,sizeof(buf)-(b-buf),msg,val);
		assert(b < buf + sizeof(buf));
#if defined(__sun) || defined(__linux)
		(void) write(Log,buf,b - buf);
#else
		{
			int err;
			err = pthread_mutex_lock(&LogMut);
			assert(err == 0);
			(void) write(Log,buf,b - buf);
			err = pthread_mutex_unlock(&LogMut);
			assert(err == 0);
		}
#endif
		va_end(val);
	}
}


static void warningmsg(const char *msg, ...)
{
	if (Verbose >= 3) {
		va_list val;
		char buf[256], *b = buf + PrefixLen;

		va_start(val,msg);
		(void) memcpy(buf,Prefix,PrefixLen);
		(void) memcpy(b,"warning: ",9);
		b += 9;
		b += vsnprintf(b,sizeof(buf)-(b-buf),msg,val);
		assert(b < buf + sizeof(buf));
#if defined(__sun) || defined(__linux)
		(void) write(Log,buf,b - buf);
#else
		{
			int err;
			err = pthread_mutex_lock(&LogMut);
			assert(err == 0);
			(void) write(Log,buf,b - buf);
			err = pthread_mutex_unlock(&LogMut);
			assert(err == 0);
		}
#endif
		va_end(val);
	}
}


static void errormsg(const char *msg, ...)
{
	ErrorOccured = 1;
	if (Verbose >= 2) {
		va_list val;
		char buf[256], *b = buf + PrefixLen;

		va_start(val,msg);
		(void) memcpy(buf,Prefix,PrefixLen);
		(void) memcpy(b,"error: ",7);
		b += 7;
		b += vsnprintf(b,sizeof(buf)-(b-buf),msg,val);
		assert(b < buf + sizeof(buf));
#if defined(__sun) || defined(__linux)
		(void) write(Log,buf,b - buf);
#else
		{
			int err;
			err = pthread_mutex_lock(&LogMut);
			assert(err == 0);
			(void) write(Log,buf,b - buf);
			err = pthread_mutex_unlock(&LogMut);
			assert(err == 0);
		}
#endif
		va_end(val);
	}
}


static void fatal(const char *msg, ...)
{
	if (Verbose >= 1) {
		va_list val;
		char buf[256], *b = buf + PrefixLen;

		va_start(val,msg);
		(void) memcpy(buf,Prefix,PrefixLen);
		(void) memcpy(b,"fatal: ",7);
		b += 7;
		b += vsnprintf(b,sizeof(buf)-(b-buf),msg,val);
		assert(b < buf + sizeof(buf));
#if defined(__sun) || defined(__linux)
		(void) write(Log,buf,b - buf);
#else
		{
			int err;
			err = pthread_mutex_lock(&LogMut);
			assert(err == 0);
			(void) write(Log,buf,b - buf);
			err = pthread_mutex_unlock(&LogMut);
			assert(err == 0);
		}
#endif
		va_end(val);
	}
	exit(EXIT_FAILURE);
}



static char *kb2str(double v)
{
	/* NOT THREAD SAFE - only used by statusThread */
	const char *dim = "kMGT", *f;
	static char s[8];

	while (v > 10000) {
		v /= 1024.0;
		++dim;
		if (*dim == 0) {
			v *= 1024.0*1024.0*1024.0*1024.0;
			break;
		}
	}
	if (v < 0)
		f = " ??? ";
	else if (v < 100)
		f = "%4.1f %c";
	else if (v < 10000) {
		v = rint(v);
		f = "%4.0f %c";
	} else
		f = "%5.lg ";
	sprintf(s,f,v,*dim);
	return s;
}



static void summary(unsigned long long numb, double secs, int numthreads)
{
	int h = (int) secs/3600,
	    m = (int) (secs - h * 3600)/60;
	double av = (double)(numb >>= 10)/secs*numthreads;
	char buf[256], *msg = buf;
	
	if ((Terminate == 1) || (Status == 0)) {
		if (Status)
			write(STDERR_FILENO,"\n",1);
		return;
	}
	secs -= m * 60 + h * 3600;
	if (numthreads > 1)
		msg += sprintf(msg,"\nsummary: %d x %sByte in ",numthreads,kb2str(numb));
	else
		msg += sprintf(msg,"\nsummary: %sByte in ",kb2str(numb));
	if (h)
		msg += sprintf(msg,"%d h %02d min ",h,m);
	else if (m)
		msg += sprintf(msg,"%2d min ",m);
	msg += sprintf(msg,"%02.1f sec - average of %sB/s",secs,kb2str(av));
	if (EmptyCount)
		msg += sprintf(msg,", %dx empty",EmptyCount);
	if (FullCount)
		msg += sprintf(msg,", %dx full",FullCount);
	*msg++ = '\n';
#ifdef HAVE_LIBMHASH
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		mhash_deinit(MD5hash,hashvalue);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#elif defined HAVE_LIBMD5
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5Final(hashvalue,&md5ctxt);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#elif defined HAVE_LIBCRYPTO
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5_Final(hashvalue,&md5ctxt);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#endif
	*msg = 0;
	if (Log != STDERR_FILENO)
		(void) write(Log,buf,msg-buf);
	(void) write(STDERR_FILENO,buf,msg-buf);
}



static void cancelAll(void)
{
	dest_t *d = Dest;
	do {
		(void) pthread_cancel(d->thread);
		if (d->result == 0)
			d->result = "canceled";
		d = d->next;
	} while (d);
	pthread_cancel(Reader);
}



static RETSIGTYPE sigHandler(int signr)
{
	int err;

	switch (signr) {
	case SIGHUP:
	case SIGINT:
		ErrorOccured = 1;
		Terminate = 1;
		err = sem_post(&Done);
		assert(err == 0);
		break;
	default:
		(void) raise(SIGABRT);
	}
}



/* Thread-safe replacement for usleep. Argument must be a whole
 * number of microseconds to sleep.
 */
static int mt_usleep(unsigned long sleep_usecs)
{
	struct timespec tv;
	tv.tv_sec = sleep_usecs / 1000000;
	tv.tv_nsec = (sleep_usecs % 1000000) * 1000;

	do {
		/* Sleep for the time specified in tv. If interrupted by a
		 * signal, place the remaining time left to sleep back into tv.
		 */
		if (0 == nanosleep(&tv, &tv)) 
			return 0;
	} while (errno == EINTR);
	return -1;
}



static void statusThread(void) 
{
	struct timeval last, now;
	double in = 0, out = 0, total, diff, fill;
	unsigned long long lin = 0, lout = 0;
	int unwritten = 1;	/* assumption: initially there is at least one unwritten block */
	int ret, numthreads = 0;
	dest_t *d;

	last = Starttime;
#ifdef __alpha
	(void) mt_usleep(1000);	/* needed on alpha (stderr fails with fpe on nan) */
#endif
	while (((Finish == -1) || (unwritten != 0)) && (Terminate == 0)) {
		int err,nw, numsender;
		char buf[256], *b = buf;
		(void) mt_usleep(500000);
		(void) gettimeofday(&now,0);
		diff = now.tv_sec - last.tv_sec + (double) (now.tv_usec - last.tv_usec) / 1000000;
		err = pthread_mutex_lock(&TermMut);
		assert(0 == err);
		err = sem_getvalue(&Buf2Dev,&unwritten);
		assert(0 == err);
		fill = (double)unwritten / (double)Numblocks * 100.0;
		in = ((Numin - lin) * Blocksize) >> 10;
		in /= diff;
		out = ((Numout - lout) * Blocksize) >> 10;
		out /= diff;
		lin = Numin;
		lout = Numout;
		last = now;
		total = (Numout * Blocksize) >> 10;
		fill = (fill < 0) ? 0 : fill;
		b += sprintf(b,"\rin @ %sB/s",kb2str(in));
		numsender = NumSenders + MainOutOK;
		b += sprintf(b,", out @ %sB/s",kb2str(out * numsender));
		if (numsender != 1)
			b += sprintf(b,", %d x %sB total, buffer %3.0f%% full",numsender,kb2str(total),fill);
		else
			b += sprintf(b,", %sB total, buffer %3.0f%% full",kb2str(total),fill);
#ifndef __sun
		if (Log == STDERR_FILENO) {
			int e;
			e = pthread_mutex_lock(&LogMut);
			assert(e == 0);
			nw = write(STDERR_FILENO,buf,strlen(buf));
			e = pthread_mutex_unlock(&LogMut);
			assert(e == 0);
		} else
#endif
			nw = write(STDERR_FILENO,buf,strlen(buf));
		err = pthread_mutex_unlock(&TermMut);
		assert(0 == err);
		if (nw == -1)	/* stop trying to print status messages after a write error */
			break;
	}
	d = Dest;
	if (Terminate)
		cancelAll();
	infomsg("statusThread: joining to terminate...\n");
	d = Dest;
	do {
		if (d->name) {
			void *status;
			debugmsg("joining %s\n",d->arg);
			ret = pthread_join(d->thread,&status);
			if (ret != 0)
				fatal("error joining %s(%s): %s\n",d->arg,d->name,strerror(errno));
			if ((int)status == 0)
				++numthreads;
		}
		d = d->next;
	} while (d);
	ret = pthread_join(Reader,0);
	assert(0 == ret);
	if (Memmap) {
		int ret = munmap(Buffer[0],Blocksize*Numblocks);
		assert(ret == 0);
	}
	(void) close(Tmp);
	(void) gettimeofday(&now,0);
	diff = now.tv_sec - Starttime.tv_sec + (double) now.tv_usec / 1000000 - (double) Starttime.tv_usec / 1000000;
	summary(Numout * Blocksize + Rest, diff, numthreads);
}


static inline long long timediff(struct timeval *restrict t1, struct timeval *restrict t2)
{
	long long tdiff;
	tdiff = (t1->tv_sec - t2->tv_sec) * 1000000;
	tdiff += t1->tv_usec - t2->tv_usec;
	return tdiff;
}


static long long enforceSpeedLimit(unsigned long long limit, long long num, struct timeval *last)
{
	static long long minwtime = 0, ticktime;
	struct timeval now;
	long long tdiff;
	double dt;
	
	if (minwtime == 0) {
		ticktime = 1000000 / sysconf(_SC_CLK_TCK);
	}
	num += Blocksize;
	(void) gettimeofday(&now,0);
	tdiff = timediff(&now,last);
	assert(tdiff >= 0);
	if (num < 0)
		return num;
	dt = (double)tdiff * 1E-6;
	if (((double)num/dt) > (double)limit) {
		double req = (double)num/limit - dt;
		long long w = (long long) (req * 1E6);
		if (w > ticktime) {
			long long slept, stime;
			stime = w / ticktime;
			stime *= ticktime;
			debugmsg("enforceSpeedLimit(%lld,%lld): sleeping for %lld usec\n",limit,num,stime);
			(void) mt_usleep(stime);
			(void) gettimeofday(last,0);
			slept = timediff(last,&now);
			assert(slept >= 0);
			debugmsg("enforceSpeedLimit(): slept for %lld usec\n",slept);
			slept -= stime;
			if (slept >= 0)
				return -(long long)(limit * (double)slept * 1E-6);
			return 0;
		} else {
			/* 
			 * Sleeping now would cause too much of a slowdown. So
			 * we defer this sleep until the sleeping time is
			 * longer than the tick time. Like this we can stay as
			 * close to the speed limit as possible.
			 */
			return num;
		}
	} else if (tdiff > minwtime) {
		(void) gettimeofday(last,0);
		return 0;
	}
	return num;
}


static void requestInputVolume(void)
{
	char cmd_buf[15+strlen(Infile)];
	const char *cmd;

	debugmsg("requesting new volume for input\n");
	if (-1 == close(In))
		errormsg("error closing input: %s\n",strerror(errno));
	do {
		if ((Autoloader) && (Infile)) {
			int ret;
			infomsg("requesting change of volume...\n");
			if (Autoload_cmd) {
				cmd = Autoload_cmd;
			} else {
				(void) snprintf(cmd_buf, sizeof(cmd_buf), "mt -f %s offline", Infile);
				cmd = cmd_buf;
			}
			ret = system(cmd);
			if (0 < ret) {
				errormsg("error running \"%s\" to change volume in autoloader: exitcode %d\n",cmd,ret);
				Terminate = 1;
				pthread_exit((void *) -1);
			} else if (0 > ret) {
				errormsg("error starting \"%s\" to change volume in autoloader: %s\n", cmd, strerror(errno));
				Terminate = 1;
				pthread_exit((void *) -1);
			}
			if (Autoload_time) {
				infomsg("waiting for drive to get ready...\n");
				(void) sleep(Autoload_time);
			}
		} else {
			int err;
			char c = 0, msg[] = "\ninsert next volume and press return to continue...";
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			if (-1 == write(STDERR_FILENO,msg,sizeof(msg))) {
				errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
				Terminate = 1;
				pthread_exit((void *) -1);
			}
			do {
				if (-1 == read(STDERR_FILENO,&c,1) && (errno != EINTR)) {
					errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
					Terminate = 1;
					pthread_exit((void *) -1);
				}
			} while (c != '\n');
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		In = open(Infile, O_RDONLY | LARGEFILE | (Direct ? DIRECT : 0));
		if ((-1 == In) && (errno == EINVAL))
			In = open(Infile, O_RDONLY | (Direct ? DIRECT : 0));
		if (-1 == In)
			errormsg("could not reopen input: %s\n",strerror(errno));
#ifdef __sun
		if (-1 == directio(In,DIRECTIO_ON))
			infomsg("direct I/O hinting failed for input: %s\n",strerror(errno));
#endif
	} while (In == -1);
	Multivolume--;
	if (Terminal && ! Autoloader) {
		char msg[] = "\nOK - continuing...\n";
		(void) write(STDERR_FILENO,msg,sizeof(msg));
	}
}



static void releaseLock(void *l)
{
	int err = pthread_mutex_unlock((pthread_mutex_t *)l);
	assert(err == 0);
}



static void *inputThread(void *ignored)
{
	int fill = 0;
	unsigned long long num;
	int at = 0;
	long long xfer = 0;
	const double startread = StartRead, startwrite = StartWrite;
	struct timeval last;

	(void) gettimeofday(&last,0);
	assert(ignored == 0);
	infomsg("inputThread: starting...\n");
	for (;;) {
		int err;

		err = pthread_mutex_lock(&LowMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if ((startread < 1) && (fill == Numblocks - 1)) {
			debugmsg("inputThread: buffer full, waiting for it to drain.\n");
			pthread_cleanup_push(releaseLock,&LowMut);
			err = pthread_cond_wait(&PercLow,&LowMut);
			assert(err == 0);
			pthread_cleanup_pop(0);
			++FullCount;
			debugmsg("inputThread: low watermark reached, continuing...\n");
		}
		err = pthread_mutex_unlock(&LowMut);
		assert(err == 0);
		err = sem_wait(&Dev2Buf); /* Wait for one or more buffer blocks to be free */
		assert(err == 0);
		if (Terminate) {	/* for async termination requests */
			debugmsg("inputThread: terminating early upon request...\n");
			if (-1 == close(In))
				errormsg("error closing input: %s\n",strerror(errno));
			err = sem_post(&Done);
			assert(err == 0);
			pthread_exit((void *)1);
			return 0;	/* just to make lint happy... */
		}
		num = 0;
		do {
			int in;
			in = read(In,Buffer[at] + num,Blocksize - num);
			debugmsg("inputThread: read(In, Buffer[%d] + %llu, %llu) = %d\n", at, num, Blocksize - num, in);
			if ((0 == in) && (Terminal||Autoloader) && (Multivolume)) {
				requestInputVolume();
			} else if (-1 == in) {
				errormsg("inputThread: error reading at offset 0x%llx: %s\n",Numin*Blocksize,strerror(errno));
				if (num) {
					Finish = at;
					Rest = num;
				} else if (at > 0) {
					Finish = at-1;
					Rest = 0;
				} else {
					Finish = Numblocks;
					Rest = 0;
				}
				err = sem_post(&Buf2Dev);
				assert(err == 0);
				err = pthread_mutex_lock(&HighMut);
				assert(err == 0);
				err = pthread_cond_signal(&PercHigh);
				assert(err == 0);
				err = pthread_mutex_unlock(&HighMut);
				assert(err == 0);
				infomsg("inputThread: exiting...\n");
				err = sem_post(&Done);
				assert(err == 0);
				pthread_exit((void *) -1);
			} else if (0 == in) {
				Rest = num;
				Finish = at;
				debugmsg("inputThread: last block has %llu bytes\n",num);
				#ifdef HAVE_LIBMHASH
				if (Hash)
					mhash(MD5hash,Buffer[at],num);
				#elif defined HAVE_LIBMD5
				if (Hash)
					MD5Update(&md5ctxt,(unsigned char *)Buffer[at],(unsigned int)num);
				#elif defined HAVE_LIBCRYPTO
				if (Hash)
					MD5_Update(&md5ctxt,Buffer[at],num);
				#endif
				err = pthread_mutex_lock(&HighMut);
				assert(err == 0);
				err = sem_post(&Buf2Dev);
				assert(err == 0);
				err = pthread_cond_signal(&PercHigh);
				assert(err == 0);
				err = pthread_mutex_unlock(&HighMut);
				assert(err == 0);
				infomsg("inputThread: exiting...\n");
				err = sem_post(&Done);
				assert(err == 0);
				pthread_exit(0);
			}
			num += in;
		} while (num < Blocksize);
		#ifdef HAVE_LIBMHASH
		if (Hash)
			mhash(MD5hash,Buffer[at],num);
		#elif defined HAVE_LIBMD5
		if (Hash)
			MD5Update(&md5ctxt,(unsigned char *)Buffer[at],num);
		#elif defined HAVE_LIBCRYPTO
		if (Hash)
			MD5_Update(&md5ctxt,Buffer[at],num);
		#endif
		if (MaxReadSpeed)
			xfer = enforceSpeedLimit(MaxReadSpeed,xfer,&last);
		err = pthread_mutex_lock(&HighMut);
		assert(err == 0);
		err = sem_post(&Buf2Dev);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if (((double) fill / (double) Numblocks) >= startwrite) {
			err = pthread_cond_signal(&PercHigh);
			assert(err == 0);
		}
		err = pthread_mutex_unlock(&HighMut);
		assert(err == 0);
		at++;
		if (at == Numblocks)
			at = 0;
		Numin++;
	}
}


static inline int syncSenders(char *b, int s)
{
	static volatile int size = 0;
	static char *volatile buf = 0;
	int err;

	err = pthread_mutex_lock(&SendMut);
	assert(err == 0);
	if (b) {
		buf = b;
		size = s;
	}
	if (s < 0)
		--NumSenders;
	if (--ActSenders) {
		//debugmsg("syncSenders(%p,%d): ActSenders = %d\n",b,s,ActSenders);
		pthread_cleanup_push(releaseLock,&SendMut);
		err = pthread_cond_wait(&SendCond,&SendMut);
		assert(err == 0);
		pthread_cleanup_pop(1);
		//debugmsg("syncSenders(): continue\n");
		return 0;
	} else {
		ActSenders = NumSenders + 1;
		assert(buf);
		SendAt = buf;
		SendSize = size;
		buf = 0;
		err = pthread_mutex_unlock(&SendMut);
		assert(err == 0);
		err = sem_post(&Dev2Buf);
		assert(err == 0);
		/*debugmsg("syncSenders(): send %d@%p, BROADCAST\n",SendSize,SendAt);*/
		err = pthread_cond_broadcast(&SendCond);
		assert(err == 0);
		return 1;
	}
}


static inline void terminateSender(int fd, dest_t *d, int ret)
{
	int err;
	debugmsg("terminating operation on %s\n",d->arg);
	if (-1 != fd) {
		if (0 != fsync(fd)) {
			if (errno == EINVAL) {
				infomsg("syncing unsupported on %s: omitted.\n",d->arg);
			} else {
				errormsg("error syncing %s: %s\n",d->arg,strerror(errno));
			}
		} else {
			infomsg("syncing %s: done.\n",d->arg);
		}
		if (-1 == close(fd))
			errormsg("error closing file %s: %s\n",d->arg,strerror(errno));
	}
	if (ret != 0) {
		int ret = syncSenders(0,-1);
		debugmsg("terminateSender(%s): sendSender(0,-1) = %d\n",d->arg,ret);
	}
	err = sem_post(&Done);
	assert(err == 0);
	pthread_exit((void *) ret);
}



static void openNetworkOutput(dest_t *dest)
{
	struct sockaddr_in saddr;
	struct hostent *h = 0;
	int out;

	debugmsg("creating socket for output to %s:%d...\n",dest->name,dest->port);
	out = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > out) {
		errormsg("could not create socket for network output: %s\n",strerror(errno));
		return;
	}
	bzero((void *) &saddr, sizeof(saddr));
	saddr.sin_port = htons(dest->port);
	infomsg("resolving host %s...\n",dest->name);
	if (0 == (h = gethostbyname(dest->name))) {
#ifdef HAVE_HSTRERROR
		dest->result = hstrerror(h_errno);
		errormsg("could not resolve hostname %s: %s\n",dest->name,dest->result);
#else
		dest->result = "unable to resolve hostname";
		errormsg("could not resolve hostname %s: error code %d\n",dest->name,h_errno);
#endif
		dest->fd = -1;
		(void) close(out);
		return;
	}
	saddr.sin_family = h->h_addrtype;
	assert(h->h_length <= sizeof(saddr.sin_addr));
	(void) memcpy(&saddr.sin_addr,h->h_addr_list[0],h->h_length);
	infomsg("connecting to server at %s...\n",inet_ntoa(saddr.sin_addr));
	if (0 > connect(out, (struct sockaddr *) &saddr, sizeof(saddr))) {
		dest->result = strerror(errno);
		errormsg("could not connect to %s:%d: %s\n",dest->name,dest->port,dest->result);
		(void) close(out);
		out = -1;
	}
	dest->fd = out;
}




static void *senderThread(void *arg)
{
	unsigned long long outsize = Blocksize;
	dest_t *dest = (dest_t *)arg;
	int out = dest->fd, sendout = Sendout;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	struct stat st;
#endif

	debugmsg("sender(%s): starting...\n",dest->arg);
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	debugmsg("checking output device...\n");
	if (-1 == fstat(out,&st))
		warningmsg("could not stat output %s: %s\n",dest->arg,strerror(errno));
	else if (S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)) {
		infomsg("blocksize is %d bytes on output device\n",st.st_blksize);
		if ((Blocksize < st.st_blksize) || (Blocksize % st.st_blksize != 0)) {
			warningmsg("Blocksize should be a multiple of the blocksize of the output device!\n"
				"This can cause problems with some device/OS combinations...\n"
				"Blocksize on output device %s is %d (transfer block size is %lld)\n", dest->arg, st.st_blksize, Blocksize);
			if (SetOutsize) {
				errormsg("unable to set output blocksize\n");
				dest->result = strerror(errno);
				terminateSender(out,dest,1);
			}
		} else {
			if (SetOutsize) {
				infomsg("setting output blocksize to %d\n",st.st_blksize);
				outsize = st.st_blksize;
			}
		}
	} else
		infomsg("no device on output stream %s\n",dest->arg);
#endif
	for (;;) {
		int size, num = 0;
		syncSenders(0,0);
		size = SendSize;
		if (0 == size) {
			debugmsg("senderThread(\"%s\"): done.\n",dest->arg);
			terminateSender(out,dest,0);
			return 0;	/* for lint */
		}
		if (Terminate) {
			dest->result = "canceled";
			terminateSender(out,dest,1);
		}
		do {
			unsigned long long rest = size - num;
			int ret;
			assert(size >= num);
#ifdef HAVE_SENDFILE
			if (sendout) {
				off_t baddr = (off_t) (SendAt+num);
				ret = sendfile(out,SFV_FD_SELF,&baddr,rest > outsize ? outsize : rest);
				debugmsg("sender(%s): sendfile(Out, SFV_FD_SELF, &%p, %llu) = %d\n", dest->arg, (void*)baddr, (unsigned long long) (rest > outsize ? outsize : rest), ret);
				if ((ret == -1) && ((errno == EINVAL) || (errno == EOPNOTSUPP))) {
					sendout = 0;
					continue;
				}
			} else
#endif
			{
				char *baddr = SendAt+num;
				ret = write(out,baddr,rest > outsize ? outsize :rest);
				debugmsg("sender(%s): writing %llu@%p - ret = %d\n",dest->arg,rest,(void*)baddr,ret);
			}
			if (-1 == ret) {
				errormsg("error writing to %s: %s\n",dest->arg,strerror(errno));
				dest->result = strerror(errno);
				terminateSender(out,dest,1);
			}
			num += ret;
		} while (num != size);
	}
}



static int requestOutputVolume(int out, const char *outfile)
{
	if (!outfile) {
		errormsg("End of volume, but not end of input:\n"
			"Output file must be given (option -o) for multi volume support!\n");
		return -1;
	}
	debugmsg("requesting new output volume\n");
	if (-1 == close(out))
		errormsg("error closing output %s: %s\n",outfile,strerror(errno));
	do {
		mode_t mode;
		if (Autoloader) {
			const char default_cmd[] = "mt -f %s offline";
			char cmd_buf[sizeof(default_cmd)+strlen(outfile)];
			const char *cmd = Autoload_cmd;
			int err;

			infomsg("requesting change of volume...\n");
			if (cmd == 0) {
				(void) snprintf(cmd_buf, sizeof(cmd_buf), default_cmd, Infile);
				cmd = cmd_buf;
			}
			err = system(cmd);
			if (0 < err) {
				errormsg("error running \"%s\" to change volume in autoloader - exitcode %d\n", cmd, err);
				Autoloader = 0;
				return -1;
			} else if (0 > err) {
				errormsg("error starting \"%s\" to change volume in autoloader: %s\n", cmd, strerror(errno));
				Autoloader = 0;
				return -1;
			}
			if (Autoload_time) {
				infomsg("waiting for drive to get ready...\n");
				(void) sleep(Autoload_time);
			}
		} else {
			int err;
			char c = 0, msg[] = "\nvolume full - insert new media and press return when ready...\n";
			if (Terminal == 0) {
				errormsg("End of volume, but not end of input.\n"
					"Specify an autoload command, if you are working without terminal.\n");
				return -1;
			}
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			if (-1 == write(STDERR_FILENO,msg,sizeof(msg))) {
				errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
				return -1;
			}
			do {
				if (-1 == read(STDERR_FILENO,&c,1) && (errno != EINTR)) {
					errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
					return -1;
				}
			} while (c != '\n');
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		mode = O_WRONLY|O_TRUNC|OptSync|LARGEFILE;
		if (strncmp(outfile,"/dev/",5))
			mode |= Nooverwrite|O_CREAT;
		if (Direct)
			mode |= DIRECT;
		out = open(outfile,mode,0666);
		if (-1 == out)
			errormsg("error reopening output file: %s\n",strerror(errno));
#ifdef __sun
		if (-1 == directio(out,DIRECTIO_ON))
			infomsg("direct I/O hinting failed for output: %s\n",strerror(errno));
#endif
	} while (-1 == out);
	infomsg("continuing with next volume\n");
	if (Terminal && ! Autoloader) {
		char msg[] = "\nOK - continuing...\n";
		(void) write(STDERR_FILENO,msg,sizeof(msg));
	}
	return out;
}



static int checkIncompleteOutput(int out, const char *outfile)
{
	static int mulretry = 0;	/* well this isn't really good design,
					   but better than a global variable */
	
	debugmsg("Outblocksize = %d, mulretry = %d\n",Outblocksize,mulretry);
	if ((0 != mulretry) || (0 == Outblocksize)) {
		out = requestOutputVolume(out,outfile);
		debugmsg("resetting outputsize to normal\n");
		if (0 != mulretry) {
			Outsize = mulretry;
			mulretry = 0;
		}
	} else {
		debugmsg("setting to new outputsize (end of device)\n");
		mulretry = Outsize;
		Outsize = Outblocksize;
	}
	return out;
}



static void terminateOutputThread(dest_t *d)
{
	infomsg("outputThread: syncing...\n");
	while (0 != fsync(d->fd)) {
		if (errno == EINTR) {
			continue;
		} else if (errno == EINVAL) {
			infomsg("syncing unsupported on %s: omitted.\n",d->arg);
			break;
		} else {
			errormsg("error syncing %s: %s\n",d->arg,strerror(errno));
			break;
		}
	}
	infomsg("outputThread: finished - exiting...\n");
	if (-1 == close(d->fd))
		errormsg("error closing %s: %s\n",d->arg,strerror(errno));
	pthread_exit(0);
}



static void *outputThread(void *arg)
{
	dest_t *dest = (dest_t *) arg;
	unsigned at = 0;
	int fill = 0, haderror = 0, out, ret, sendout = Sendout, multipleSenders = NumSenders > 0;
	const double startwrite = StartWrite, startread = StartRead;
	unsigned long long blocksize = Blocksize;
	long long xfer = 0;
	struct timeval last;

	/* initialize last to 0, because we don't want to wait initially */
	(void) gettimeofday(&last,0);
	assert(NumSenders >= 0);
	if (dest->next) {
		dest_t *d = dest->next;
		debugmsg("NumSenders = %d\n",NumSenders);
		ActSenders = NumSenders + 1;
		ret = pthread_mutex_init(&SendMut,0);
		assert(ret == 0);
		ret = pthread_cond_init(&SendCond,0);
		assert(ret == 0);
		do {
			if (d->fd != -1) {
				debugmsg("creating sender for %s\n",d->arg);
				ret = pthread_create(&d->thread,0,senderThread,d);
				assert(ret == 0);
			} else  {
				debugmsg("outputThread: ignoring destination %s\n",d->arg);
				d->name = 0;
			}
			d = d->next;
		} while (d);
	}
	dest->result = 0;
	out = dest->fd;
	infomsg("outputThread: starting output on %s...\n",dest->arg);
	for (;;) {
		unsigned long long rest = blocksize;
		int err;

		err = pthread_mutex_lock(&HighMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if ((fill == 0) && (startwrite > 0)) {
			debugmsg("outputThread: buffer empty, waiting for it to fill\n");
			pthread_cleanup_push(releaseLock,&HighMut);
			err = pthread_cond_wait(&PercHigh,&HighMut);
			assert(err == 0);
			pthread_cleanup_pop(0);
			++EmptyCount;
			debugmsg("outputThread: high watermark reached, continuing...\n");
		}
		err = pthread_mutex_unlock(&HighMut);
		assert(err == 0);
		err = sem_wait(&Buf2Dev);
		assert(err == 0);
		if (Terminate) {
			debugmsg("outputThread: terminating upon termination request...\n");
			if (-1 == close(dest->fd)) 
				errormsg("error closing output %s: %s\n",dest->arg,strerror(errno));
			dest->result = "canceled";
			pthread_exit((void*)1);
		}
		if (Finish == at) {
			if (0 == Rest) {
				if (multipleSenders)
					syncSenders((char*)0xdeadbeef,0);
				infomsg("outputThread: finished - exiting...\n");
				pthread_exit((void*)haderror);
			} else {
				blocksize = rest = Rest;
				debugmsg("outputThread: last block has %llu bytes\n",Rest);
			}
		}
		if (multipleSenders)
			syncSenders(Buffer[at],blocksize);
		/* switch output volume if -D <size> has been reached */
		if ( (OutVolsize != 0) && (Numout > 0) && (Numout % (OutVolsize/Blocksize)) == 0 ) {
			/* Sleep to let status thread "catch up" so that the displayed total is a multiple of OutVolsize */
			(void) mt_usleep(500000);
			out = requestOutputVolume(out,dest->name);
			if (out == -1) {
				haderror = 1;
				dest->result = strerror(errno);
			}
		}
		do {
			/* use Outsize which could be the blocksize of the device (option -d) */
			int num;
			if (haderror) {
				if (NumSenders == 0)
					Terminate = 1;
				num = (int)rest;
			} else
#ifdef HAVE_SENDFILE
			if (sendout) {
				off_t baddr = (off_t) (Buffer[at] + blocksize - rest);
				num = sendfile(out,SFV_FD_SELF,&baddr,(size_t)(rest > Outsize ? Outsize : rest));
				debugmsg("outputThread: sendfile(out, SFV_FD_SELF, &(Buffer[%d] + %llu), %llu) = %d\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, num);
				if ((num == -1) && ((errno == EOPNOTSUPP) || (errno == EINVAL))) {
					sendout = 0;
					continue;
				}
			} else
#endif
			{
				num = write(out,Buffer[at] + blocksize - rest, rest > Outsize ? Outsize : rest);
				debugmsg("outputThread: writing %lld@%p - ret = %d\n",rest > Outsize ? Outsize : rest,Buffer[at] + blocksize - rest,num);
			}
			if ((-1 == num) && (Terminal||Autoloader) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				/* request a new volume - but first check
				 * whether we are really at the
				 * end of the device */
				out = checkIncompleteOutput(out,dest->name);
				if (out == -1)
					haderror = 1;
				continue;
			} else if (-1 == num) {
				dest->result = strerror(errno);
				errormsg("outputThread: error writing to %s at offset 0x%llx: %s\n",dest->arg,(long long)Blocksize*Numout+blocksize-rest,strerror(errno));
				MainOutOK = 0;
				if (NumSenders == 0) {
					debugmsg("outputThread: terminating...\n");
					Terminate = 1;
					err = sem_post(&Dev2Buf);
					assert(err == 0);
					pthread_exit((void *) -1);
				}
				debugmsg("outputThread: %d senders remaining - continuing...\n",NumSenders);
				haderror = 1;
			}
			rest -= num;
		} while (rest > 0);
		if (Dest->next == 0) {
			err = sem_post(&Dev2Buf);
			assert(err == 0);
		}
		if (MaxWriteSpeed) {
			xfer = enforceSpeedLimit(MaxWriteSpeed,xfer,&last);
		}
		if (Pause)
			(void) mt_usleep(Pause);
		if (Finish == at) {
			if (multipleSenders)
				syncSenders((char*)0xdeadbeef,0);
			terminateOutputThread(dest);
			return 0;	/* make lint happy */
		}
		at++;
		if (Numblocks == at)
			at = 0;
		err = pthread_mutex_lock(&LowMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if ((startread < 1) && ((double)fill / (double)Numblocks) < startread) {
			err = pthread_cond_signal(&PercLow);
			assert(err == 0);
		}
		err = pthread_mutex_unlock(&LowMut);
		assert(err == 0);
		Numout++;
	}
}



static void openNetworkInput(const char *host, unsigned short port)
{
	struct sockaddr_in saddr;
	struct sockaddr_in caddr;
	socklen_t clen = sizeof(caddr);
	struct hostent *h = 0, *r = 0;
	const int reuse_addr = 1;

	debugmsg("openNetworkInput(\"%s\",%hu)\n",host,port);
	infomsg("creating socket for network input...\n");
	Sock = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Sock)
		fatal("could not create socket for network input: %s\n",strerror(errno));
	if (-1 == setsockopt(Sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr)))
		warningmsg("cannot set socket to reuse address: %s\n",strerror(errno));
	if (host[0]) {
		debugmsg("resolving hostname of input interface...\n");
		if (0 == (h = gethostbyname(host)))
#ifdef HAVE_HSTRERROR
			fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
#else
			fatal("could not resolve server hostname: error code %d\n",h_errno);
#endif
	}
	bzero((void *) &saddr, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(port);
	infomsg("binding socket to port %d...\n",port);
	if (0 > bind(Sock, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not bind to socket for network input: %s\n",strerror(errno));
	infomsg("listening on socket...\n");
	if (0 > listen(Sock,1))		/* accept only 1 incoming connection */
		fatal("could not listen on socket for network input: %s\n",strerror(errno));
	for (;;) {
		char **p;
		infomsg("waiting to accept connection...\n");
		In = accept(Sock, (struct sockaddr *)&caddr, &clen);
		if (0 > In)
			fatal("could not accept connection for network input: %s\n",strerror(errno));
		if (host[0] == 0) {
			infomsg("accepted connection from %s\n",inet_ntoa(caddr.sin_addr));
			return;
		}
		for (p = h->h_addr_list; *p; ++p) {
			if (0 == memcmp(&caddr.sin_addr,*p,h->h_length)) {
				infomsg("accepted connection from %s\n",inet_ntoa(caddr.sin_addr));
				return;
			}
		}
		r = gethostbyaddr((char *)&caddr.sin_addr,sizeof(caddr.sin_addr.s_addr),AF_INET);
		if (r)
			warningmsg("rejected connection from %s (%s)\n",r->h_name,inet_ntoa(caddr.sin_addr));
		else
			warningmsg("rejected connection from %s\n",inet_ntoa(caddr.sin_addr));
		if (-1 == close(In))
			warningmsg("error closing rejected input: %s\n",strerror(errno));
	}
}



static void getNetVars(const char **argv, int *c, const char **host, unsigned short *port)
{
	char *tmphost;
	
	tmphost = malloc(strlen(argv[*c] + 1));
	if (0 == tmphost)
		fatal("out of memory\n");
	if (1 < sscanf(argv[*c],"%[0-9a-zA-Z_.-]:%hu",tmphost,port)) {
		*host = tmphost;
		return;
	}
	free((void *) tmphost);
	if (1 == sscanf(argv[*c],":%hu",port)) {
		return;
	}
	if (0 != (*port = atoi(argv[*c])))
		return;
	*host = argv[*c];
	*port = atoi(argv[*c]);
	if (*port)
		(*c)++;
}



static void version(void)
{
	(void) fprintf(stderr,
		"mbuffer version "VERSION"\n"\
		"Copyright 2001-2008 - T. Maier-Komor\n"\
		"License: GPLv3 - see file LICENSE\n"\
		"This program comes with ABSOLUTELY NO WARRANTY!!!\n"
		"Donations via PayPal to thomas@maier-komor.de are welcome and support this work!\n"
		"\n"
		);
	exit(EXIT_SUCCESS);
}



static void usage(void)
{
	const char *dim = "bkMGTP";
	unsigned long long m = Numblocks * Blocksize;
	while (m >= 10000) {
		m >>= 10;
		++dim;
	}
	(void) fprintf(stderr,
		"usage: mbuffer [Options]\n"
		"Options:\n"
		"-b <num>   : use <num> blocks for buffer (default: %d)\n"
		"-s <size>  : use block of <size> bytes for buffer (default: %llu)\n"
#if defined(_SC_AVPHYS_PAGES) && defined(_SC_PAGESIZE) && !defined(__CYGWIN__)
		"-m <size>  : memory <size> of buffer in b,k,M,G,%% (default: 2%% = %llu%c)\n"
#else
		"-m <size>  : memory <size> of buffer in b,k,M,G,%% (default: %llu%c)\n"
#endif
#ifdef _POSIX_MEMLOCK_RANGE
		"-L         : lock buffer in memory (unusable with file based buffers)\n"
#endif
		"-d         : use blocksize of device for output\n"
		"-D <size>  : assumed device size (default: infinite)\n"
		"-P <num>   : start writing after buffer has been filled more than <num>%%\n"
		"-p <num>   : start reading after buffer has been filled less than <num>%%\n"
		"-i <file>  : use <file> for input\n"
		"-o <file>  : use <file> for output (this option can be passed MULTIPLE times)\n"
		"-I <h>:<p> : use network port <port> as input, allow only host <h> to connect\n"
		"-I <p>     : use network port <port> as input\n"
		"-O <h>:<p> : output data to host <h> and port <p> (MUTLIPLE outputs supported)\n"
		"-n <num>   : <num> volumes for input\n"
		"-t         : use memory mapped temporary file (for huge buffer)\n"
		"-T <file>  : as -t but uses <file> as buffer\n"
		"-l <file>  : use <file> for logging messages\n"
		"-u <num>   : pause <num> milliseconds after each write\n"
		"-r <rate>  : limit read rate to <rate> B/s, where <rate> can be given in k,M,G\n"
		"-R <rate>  : same as -r for writing; use eiter one, if your tape is too fast\n"
		"-f         : overwrite existing files\n"
		"-a <time>  : autoloader which needs <time> seconds to reload\n"
		"-A <cmd>   : issue command <cmd> to request new volume\n"
		"-v <level> : set verbose level to <level> (valid values are 0..5)\n"
		"-q         : quiet - do not display the status on stderr\n"
		"-c         : write with synchronous data integrity support\n"
#ifdef O_DIRECT
		"--direct   : open input and output with O_DIRECT\n"
#endif
#if defined HAVE_LIBCRYPTO || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
		"-H\n"
		"--md5      : generate md5 hash of transfered data\n"
#endif
		"-V\n"
		"--version  : print version information\n"
		"Unsupported buffer options: -t -Z -B\n"
		,Numblocks
		,Blocksize
		,m,*dim
		);
	exit(EXIT_SUCCESS);
}



static unsigned long long calcint(const char **argv, int c, unsigned long long d)
{
	char ch;
	unsigned long long i;
	
	switch (sscanf(argv[c],"%llu%c",&i,&ch)) {
	default:
		assert(0);
		break;
	case 2:
		switch (ch) {
		case 'k':
			i <<= 10;
			return i;
		case 'M':
			i <<= 20;
			return i;
		case 'G':
			i <<= 30;
			return i;
		case 'T':
			i <<= 40;
			return i;
		case '%':
			if (i >= 100)
				fatal("invalid value for percentage (must be < 100)\n");
			return i;
		case 'b':
		case 'B':
			if (i < 128)
				fatal("invalid value for number of bytes\n");
			return i;
		default:
			errormsg("unrecognized size charakter \"%c\" for option \"%s\"\n",ch,argv[c-1]);
			return d;
		}
	case 1:
		return i;
	case 0:
		break;
	}
	errormsg("unrecognized argument \"%s\" for option \"%s\"\n",argv[c],argv[c-1]);
	return d;
}



static int argcheck(const char *opt, const char **argv, int *c, int argc)
{
	if (strncmp(opt,argv[*c],2)) 
		return 1;
	if (strlen(argv[*c]) > 2)
		argv[*c] += 2;
	else {
		(*c)++;
		if (*c == argc)
			fatal("missing argument to option %s\n",opt);
	}
	return 0;
}



int main(int argc, const char **argv)
{
	unsigned long long totalmem = 0;
	long mxnrsem;
	int err, optMset = 0, optSset = 0, optBset = 0, c, fl, numstdout = 0;
	sigset_t       signalSet;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	struct stat st;
#endif
	unsigned short netPortIn = 0;
	const char *client = 0;
	unsigned short netPortOut = 0;
	char *argv0 = strdup(argv[0]), *progname;
	const char *outfile = 0;
	struct sigaction sig;
	dest_t *dest = 0;
#if defined(_SC_AVPHYS_PAGES) && defined(_SC_PAGESIZE) && !defined(__CYGWIN__)
	long pgsz, nump;

	pgsz = sysconf(_SC_PAGESIZE);
	assert(pgsz > 0);
	nump = sysconf(_SC_AVPHYS_PAGES);
	assert(nump > 0);
	Blocksize = pgsz;
	Numblocks = nump/50;
#endif
	progname = basename(argv0);
	PrefixLen = strlen(progname) + 2;
	Prefix = malloc(PrefixLen);
	(void) strcpy(Prefix,progname);
	Prefix[PrefixLen - 2] = ':';
	Prefix[PrefixLen - 1] = ' ';
	Log = STDERR_FILENO;
	for (c = 1; c < argc; c++) {
		if (!argcheck("-s",argv,&c,argc)) {
			Blocksize = Outsize = calcint(argv,c,Blocksize);
			optSset = 1;
			debugmsg("Blocksize = %llu\n",Blocksize);
			if (Blocksize < 100)
				fatal("cannot set blocksize as percentage of total physical memory\n");
		} else if (!argcheck("-m",argv,&c,argc)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			if (totalmem < 100) {
#if defined(_SC_AVPHYS_PAGES) && defined(_SC_PAGESIZE) && !defined(__CYGWIN__)
				totalmem = ((unsigned long long) nump * pgsz * totalmem) / 100 ;
#else
				fatal("Unable to determine page size or amount of available memory - please specify an absolute amount of memory.\n");
#endif
			}
			debugmsg("totalmem = %llu\n",totalmem);
		} else if (!argcheck("-b",argv,&c,argc)) {
			Numblocks = (atoi(argv[c])) ? ((unsigned long long) atoll(argv[c])) : Numblocks;
			optBset = 1;
			debugmsg("Numblocks = %llu\n",Numblocks);
		} else if (!argcheck("-d",argv,&c,argc)) {
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
			SetOutsize = 1;
			debugmsg("setting output size according to the blocksize of the device\n");
#else
			fatal("cannot determine blocksize of device (unsupported by OS)\n");
#endif
		} else if (!argcheck("-v",argv,&c,argc)) {
			if (c == argc)
				fatal("missing argument for option -v");
			Verbose = (atoi(argv[c])) ? (atoi(argv[c])) : Verbose;
			debugmsg("Verbose = %d\n",Verbose);
#if defined(_SC_AVPHYS_PAGES) && defined(_SC_PAGESIZE) && !defined(__CYGWIN__)
			debugmsg("total # of phys pages: %li (pagesize %li)\n",nump,pgsz);
#endif
			debugmsg("default buffer set to %d blocks of %lld bytes\n",Numblocks,Blocksize);
		} else if (!argcheck("-u",argv,&c,argc)) {
			Pause = (atoi(argv[c])) ? (atoi(argv[c])) * 1000 : Pause;
			debugmsg("Pause = %d\n",Pause);
		} else if (!argcheck("-r",argv,&c,argc)) {
			MaxReadSpeed = calcint(argv,c,0);
			debugmsg("MaxReadSpeed = %lld\n",MaxReadSpeed);
		} else if (!argcheck("-R",argv,&c,argc)) {
			MaxWriteSpeed = calcint(argv,c,0);
			debugmsg("MaxWriteSpeed = %lld\n",MaxWriteSpeed);
		} else if (!argcheck("-n",argv,&c,argc)) {
			Multivolume = atoi(argv[c]) - 1;
			if (Multivolume <= 0)
				fatal("argument for number of volumes must be > 0\n");
			debugmsg("Multivolume = %d\n",Multivolume);
		} else if (!argcheck("-i",argv,&c,argc)) {
			if (strcmp(argv[c],"-")) {
				Infile = argv[c];
				debugmsg("Infile = %s\n",Infile);
			} else {
				Infile = STDIN_FILENO;
				debugmsg("Infile is stdin\n");
			}
		} else if (!argcheck("-o",argv,&c,argc)) {
			dest_t *dest = malloc(sizeof(dest_t));
			if (strcmp(argv[c],"-")) {
				debugmsg("output file: %s\n",argv[c]);
				dest->arg = argv[c];
				dest->name = argv[c];
				dest->fd = -1;
			} else {
				if (numstdout++) 
					fatal("cannot output multiple times to stdout");
				debugmsg("output to stdout\n",argv[c]);
				dest->fd = dup(STDOUT_FILENO);
				err = dup2(STDOUT_FILENO,STDERR_FILENO);
				assert(err != -1);
				dest->arg = "<stdout>";
				dest->name = "<stdout>";
			}
			dest->port = -1;
			dest->result = 0;
			bzero(&dest->thread,sizeof(dest->thread));
			dest->next = Dest;
			Dest = dest;
			if (outfile == 0)
				outfile = argv[c];
			++NumSenders;
		} else if (!argcheck("-I",argv,&c,argc)) {
			getNetVars(argv,&c,&client,&netPortIn);
			if (client == 0)
				client = "";
			debugmsg("Network input set to %s:%hu\n",client,netPortIn);
		} else if (!argcheck("-O",argv,&c,argc)) {
			const char *s = 0;
			unsigned short p = 0;
			getNetVars(argv,&c,&s,&p);
			debugmsg("Output: server = %s, port = %hu\n",s,p);
			if ((0 == p) ^ (0 == s))
				fatal("When sending data to a network destination, both hostname and port must be set!\n");
			debugmsg("network output: %s:%d\n",s,p);
			dest_t *dest = malloc(sizeof(dest_t));
			dest->arg = argv[c];
			dest->name = s;
			dest->port = p;
			dest->result = 0;
			dest->next = Dest;
			bzero(&dest->thread,sizeof(dest->thread));
			Dest = dest;
			++NumSenders;
			Sendout = 1;
		} else if (!argcheck("-T",argv,&c,argc)) {
			Tmpfile = malloc(strlen(argv[c]) + 1);
			if (!Tmpfile)
				fatal("out of memory\n");
			(void) strcpy(Tmpfile, argv[c]);
			Memmap = 1;
			debugmsg("Tmpfile = %s\n",Tmpfile);
		} else if (!strcmp("-t",argv[c])) {
			Memmap = 1;
			debugmsg("mm = 1\n");
		} else if (!argcheck("-l",argv,&c,argc)) {
			Log = open(argv[c],O_WRONLY|O_APPEND|O_TRUNC|O_CREAT|LARGEFILE,0666);
			if (-1 == Log) {
				Log = STDERR_FILENO;
				errormsg("error opening log file: %s\n",strerror(errno));
			}
			debugmsg("logFile set to %s\n",argv[c]);
		} else if (!strcmp("-f",argv[c])) {
			Nooverwrite = 0;
			debugmsg("Nooverwrite = 0\n");
		} else if (!strcmp("-q",argv[c])) {
			debugmsg("disabling display of status\n");
			Status = 0;
		} else if (!strcmp("-c",argv[c])) {
			debugmsg("enabling full synchronous I/O\n");
			OptSync = O_DSYNC;
		} else if (!argcheck("-a",argv,&c,argc)) {
			Autoloader = 1;
			Autoload_time = atoi(argv[c]);
			debugmsg("Autoloader time = %d\n",Autoload_time);
		} else if (!argcheck("-A",argv,&c,argc)) {
			Autoloader = 1;
			Autoload_cmd = argv[c];
			debugmsg("Autoloader command = \"%s\"\n", Autoload_cmd);
		} else if (!argcheck("-P",argv,&c,argc)) {
			if (1 != sscanf(argv[c],"%lf",&StartWrite))
				StartWrite = 0;
			StartWrite /= 100;
			if ((StartWrite > 1) || (StartWrite <= 0))
				fatal("error in argument -P: must be bigger than 0 and less or equal 100");
			debugmsg("StartWrite = %1.2lf\n",StartWrite);
		} else if (!argcheck("-p",argv,&c,argc)) {
			if (1 == sscanf(argv[c],"%lf",&StartRead))
				StartRead /= 100;
			else
				StartRead = 1.0;
			if ((StartRead >= 1) || (StartRead < 0))
				fatal("error in argument -p: must be bigger or equal to 0 and less than 100");
			debugmsg("StartRead = %1.2lf\n",StartRead);
#ifdef _POSIX_MEMLOCK_RANGE
		} else if (!strcmp("-L",argv[c])) {
			Memlock = 1;
#endif
		} else if (!strcmp("--direct",argv[c])) {
#ifdef O_DIRECT
			debugmsg("using O_DIRECT to open file descriptors\n");
			Direct = 1;
#else
			warningmsg("--direct is unsupported on this system\n");
#endif
		} else if (!strcmp("--help",argv[c]) || !strcmp("-h",argv[c])) {
			usage();
		} else if (!strcmp("--version",argv[c]) || !strcmp("-V",argv[c])) {
			version();
		} else if (!strcmp("--md5",argv[c]) || !strcmp("-H",argv[c])) {
#ifdef HAVE_LIBMHASH
			Hash = 1;
			MD5hash = mhash_init(MHASH_MD5);
			if (MHASH_FAILED == MD5hash) {
				errormsg("error initializing md5 hasing - will not generate hash...\n");
				Hash = 0;
			}
#elif defined HAVE_LIBMD5
			Hash = 1;
			MD5Init(&md5ctxt);
#elif defined HAVE_LIBCRYPTO
			Hash = 1;
			MD5_Init(&md5ctxt);
#else
			warningmsg("md5 hash support has not been compiled in!");
#endif
		} else if (!argcheck("-D",argv,&c,argc)) {
			OutVolsize = calcint(argv,c,0);
			debugmsg("OutVolsize = %llu\n",OutVolsize);
		} else
			fatal("unknown option \"%s\"\n",argv[c]);
	}

	/* consistency check for options */
	if (optBset&optSset&optMset) {
		if (Numblocks * Blocksize != totalmem)
			fatal("inconsistent options: blocksize * number of blocks != totalsize!\n");
	} else if ((!optBset&optSset&optMset) || (optMset&!optBset&!optSset)) {
		Numblocks = totalmem / Blocksize;
		infomsg("Numblocks = %llu, Blocksize = %llu, totalmem = %llu\n",Numblocks,Blocksize,totalmem);
	} else if (optBset&!optSset&optMset) {
		Blocksize = totalmem / Numblocks;
		infomsg("blocksize = %llu\n",Blocksize);
	}
	if ((StartRead < 1) && (StartWrite > 0))
		fatal("setting both low watermark and high watermark doesn't make any sense...\n");
	if ((NumSenders > 0) && (Autoloader || OutVolsize))
		fatal("multi-volume support is unsupported with multiple outputs\n");
	if (Autoloader) {
		if ((!outfile) && (!Infile))
			fatal("Setting autoloader time without using a device doesn't make any sense!\n");
		if (outfile && Infile) {
			fatal("Which one is your autoloader? Input or output? Replace input or output with a pipe.\n");
		}
	}
	if (Infile && netPortIn)
		fatal("Setting both network input port and input file doesn't make sense!\n");
	if (outfile && netPortOut)
		fatal("Setting both network output and output file doesn't make sense!\n");
	if ((0 != client) && (0 == netPortIn))
		fatal("You need to set a network port for network input!\n");

	/* multi volume input consistency checking */
	if ((Multivolume) && (!Infile))
		fatal("multi volume support for input needs an explicit given input device (option -i)\n");

	/* SPW: Volsize consistency checking */
	if (OutVolsize && !outfile)
		fatal("Setting OutVolsize without an output device doesn't make sense!\n");
	if ( (OutVolsize != 0) && (OutVolsize < Blocksize) )
		/* code assumes we can write at least one block */
		fatal("If non-zero, OutVolsize must be at least as large as the buffer blocksize (%llu)!\n",Blocksize);
	/* SPW END */

	/* check that we stay within system limits */
	mxnrsem = sysconf(_SC_SEM_VALUE_MAX);
	if (-1 == mxnrsem) {
		warningmsg("unable to determine maximum value of semaphores\n");
	} else if (Numblocks > (unsigned long long) mxnrsem) {
		fatal("cannot allocate more than %d blocks.\nThis is a system dependent limit, depending on the maximum semaphore value.\nPlease choose a bigger block size.\n",mxnrsem);
	}

	if ((Blocksize * (long long)Numblocks) > (long long)SSIZE_MAX)
		fatal("Cannot address so much memory (%lld*%d=%lld>%lld).\n",Blocksize,Numblocks,Blocksize*(long long)Numblocks,(long long)SSIZE_MAX);
	/* create buffer */
	Buffer = (char **) memalign(sysconf(_SC_PAGESIZE),Numblocks * sizeof(char *));
	if (!Buffer)
		fatal("Could not allocate enough memory (%lld requested): %s\n",Numblocks * sizeof(char *),strerror(errno));
	if (Memmap) {
		infomsg("mapping temporary file to memory with %llu blocks with %llu byte (%llu kB total)...\n",Numblocks,Blocksize,(Numblocks*Blocksize) >> 10);
		if (!Tmpfile) {
			char tmplname[] = "mbuffer-XXXXXX";
			char *tmpdir = getenv("TMPDIR") ? getenv("TMPDIR") : "/var/tmp";
			char tfilename[sizeof(tmplname) + strlen(tmpdir) + 1];
			(void) strcpy(tfilename,tmpdir);
			(void) strcat(tfilename,"/");
			(void) strcat(tfilename,tmplname);
			Tmp = mkstemp(tfilename);
			Tmpfile = malloc(strlen(tfilename));
			if (!Tmpfile)
				fatal("out of memory: %s\n",strerror(errno));
			(void) strcpy(Tmpfile,tfilename);
			infomsg("tmpfile is %s\n",Tmpfile);
		} else {
			mode_t mode = O_RDWR | LARGEFILE;
			if (strncmp(Tmpfile,"/dev/",5))
				mode |= O_CREAT|O_EXCL;
			Tmp = open(Tmpfile,mode,0600);
		}
		if (-1 == Tmp)
			fatal("could not create temporary file (%s): %s\n",Tmpfile,strerror(errno));
		if (strncmp(Tmpfile,"/dev/",5))
			(void) unlink(Tmpfile);
		/* resize the file. Needed - at least under linux, who knows why? */
		if (-1 == lseek(Tmp,Numblocks * Blocksize - sizeof(int),SEEK_SET))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		if (-1 == write(Tmp,&c,sizeof(int)))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		Buffer[0] = mmap(0,Blocksize*Numblocks,PROT_READ|PROT_WRITE,MAP_SHARED,Tmp,0);
		if (MAP_FAILED == Buffer[0])
			fatal("could not map buffer-file to memory: %s\n",strerror(errno));
		debugmsg("temporary file mapped to address %p\n",Buffer[0]);
	} else {
		infomsg("allocating memory for %d blocks with %llu byte (%llu kB total)...\n",Numblocks,Blocksize,(Numblocks*Blocksize) >> 10);
		Buffer[0] = (char *) valloc(Blocksize * Numblocks);
		if (Buffer[0] == 0)
			fatal("Could not allocate enough memory (%lld requested): %s\n",strerror(errno));
	}
	for (c = 1; c < Numblocks; c++) {
		Buffer[c] = Buffer[0] + Blocksize * c;
		*Buffer[c] = 0;	/* touch every block before locking */
	}

#ifdef _POSIX_MEMLOCK_RANGE
	if (Memlock) {
		uid_t uid;
#ifndef HAVE_SETEUID
#define seteuid setuid
#endif
		uid = geteuid();
		if (0 != seteuid(0))
			warningmsg("could not change to uid 0 to lock memory (is mbuffer setuid root?)\n");
		else if ((0 != mlock((char *)Buffer,Numblocks * sizeof(char *))) || (0 != mlock(Buffer[0],Blocksize * Numblocks)))
			warningmsg("could not lock buffer in memory: %s\n",strerror(errno));
		else
			infomsg("memory locked successfully\n");
		err = seteuid(uid);	/* don't give anyone a chance to attack this program, so giveup uid 0 after locking... */
		assert(err == 0);
	}
#endif

	debugmsg("creating semaphores...\n");
	if (0 != sem_init(&Done,0,0))
		fatal("Error creating semaphore Done: %s\n",strerror(errno));
	if (0 != sem_init(&Buf2Dev,0,0))
		fatal("Error creating semaphore Buf2Dev: %s\n",strerror(errno));
	if (0 != sem_init(&Dev2Buf,0,Dest ? Numblocks - 1 : Numblocks))
		fatal("Error creating semaphore Dev2Buf: %s\n",strerror(errno));

	debugmsg("opening input...\n");
	if (Infile) {
		int flags = O_RDONLY | LARGEFILE;
		if (Direct)
			flags |= DIRECT;
		In = open(Infile,flags);
		if (-1 == In) {
			if (errno == EINVAL) {
				flags &= ~LARGEFILE;
				In = open(Infile,flags);
			}
			if (-1 == In)
				fatal("could not open input file: %s\n",strerror(errno));
		}
	} else if (netPortIn) {
		openNetworkInput(client,netPortIn);
	} else {
		In = STDIN_FILENO;
	}
#ifdef __sun
	if (0 == directio(In,DIRECTIO_ON))
		infomsg("direct I/O hinting enabled for input\n");
	else
		infomsg("direct I/O hinting failed for input: %s\n",strerror(errno));
#endif
	if (Dest) {
		dest_t *d = Dest;
		do {
			if (d->port != -1) {
				openNetworkOutput(d);
			} else if (d->fd == -1) {
				mode_t mode = O_WRONLY|O_TRUNC|OptSync|LARGEFILE;
				if (Direct)
					mode |= DIRECT;
				if (strncmp(outfile,"/dev/",5))
					mode |= Nooverwrite|O_CREAT;
				d->fd = open(outfile,mode,0666);
				if ((-1 == d->fd) && (errno == EINVAL)) {
					mode &= ~LARGEFILE;
					d->fd = open(outfile,mode,0666);
				}
				if ((-1 == d->fd) && (errno == EINVAL)) {
					mode &= ~O_TRUNC;
					d->fd = open(outfile,mode,0666);
				}
				if (-1 == d->fd) {
					d->result = strerror(errno);
					errormsg("unable to open output %s: %s\n",d->arg,strerror(errno));
				}
			}
			if (-1 == d->fd) {
				d->name = 0;	/* tag destination as unstartable */
				--NumSenders;
			}
#ifdef __sun
			else if (0 == directio(d->fd,DIRECTIO_ON))
				infomsg("direct I/O hinting enabled for output to %s\n",d->arg);
			else
				infomsg("direct I/O hinting failed for output to %s: %s\n",d->arg,strerror(errno));
#endif
			d = d->next;
		} while (d);
		if (NumSenders == -1) {
			debugmsg("no output left - nothing to do\n");
			exit(EXIT_FAILURE);
		}
	} else {
		Dest = malloc(sizeof(dest_t));
		Dest->fd = dup(STDOUT_FILENO);
		err = dup2(STDOUT_FILENO,STDERR_FILENO);
		assert(err != -1);
		Dest->name = "<stdout>";
		Dest->arg = "<stdout>";
		Dest->port = -1;
		Dest->result = 0;
		Dest->next = 0;
		bzero(&Dest->thread,sizeof(Dest->thread));
		NumSenders = 0;
	}

	debugmsg("checking if we have a controlling terminal...\n");
	fl = fcntl(STDERR_FILENO,F_GETFL);
	err = fcntl(STDERR_FILENO,F_SETFL,fl | O_NONBLOCK);
	assert(err == 0);
	if ((read(STDERR_FILENO,&c,1) == -1) && (errno != EAGAIN)) {
		int tty = open("/dev/tty",O_RDWR);
		if (-1 == tty) {
			Terminal = 0;
			if (Autoloader == 0)
				warningmsg("No controlling terminal and no autoloader command specified.\n");
		} else {
			Terminal = 1;
			err = dup2(tty,STDERR_FILENO);
			assert(err != -1);
		}
	}
	err = fcntl(STDERR_FILENO,F_SETFL,fl);
	assert(err == 0);

	debugmsg("registering signals...\n");
	sig.sa_handler = sigHandler;
	sigemptyset(&sig.sa_mask);
	sigaddset(&sig.sa_mask,SIGINT);
	sig.sa_flags = SA_RESTART;
	if (0 != sigaction(SIGINT,&sig,0))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));
	sigemptyset(&sig.sa_mask);
	sigaddset(&sig.sa_mask,SIGTERM);
	if (0 != sigaction(SIGTERM,&sig,0))
		warningmsg("error registering new SIGTERM handler: %s\n",strerror(errno));

	debugmsg("starting threads...\n");
	(void) gettimeofday(&Starttime,0);
	err = sigfillset(&signalSet);
	assert(0 == err);
	(void) pthread_sigmask(SIG_BLOCK, &signalSet, NULL);

	/* select destination for output thread */
	dest = Dest;
	while (dest->fd == -1) {
		dest->name = 0;
		debugmsg("skipping destination %s\n",dest->arg);
		assert(dest->next);
		dest = dest->next;
	}

#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	debugmsg("checking output device...\n");
	if (-1 == fstat(dest->fd,&st))
		errormsg("could not stat output: %s\n",strerror(errno));
	else if (S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)) {
		infomsg("blocksize is %d bytes on output device\n",st.st_blksize);
		if ((Blocksize < st.st_blksize) || (Blocksize % st.st_blksize != 0)) {
			warningmsg("Blocksize should be a multiple of the blocksize of the output device!\n"
				"This can cause problems with some device/OS combinations...\n"
				"Blocksize on output device is %d (transfer block size is %lld)\n", st.st_blksize, Blocksize);
			if (SetOutsize)
				fatal("unable to set output blocksize\n");
		} else {
			if (SetOutsize) {
				infomsg("setting output blocksize to %d\n",st.st_blksize);
				Outsize = st.st_blksize;
			}
		}
	} else
		infomsg("no device on output stream\n");
#else
	warningmsg("Could not stat output device (unsupported by system)!\n"
		   "This can result in incorrect written data when\n"
		   "using multiple volumes. Continue at your own risk!\n");
#endif

	err = pthread_create(&dest->thread,0,&outputThread,dest);
	assert(0 == err);
	err = pthread_create(&Reader,0,&inputThread,0);
	assert(0 == err);
	(void) pthread_sigmask(SIG_UNBLOCK, &signalSet, NULL);
	if (Status) {
		statusThread();
	} else {
		struct timeval now;
		double diff;
		int numthreads = 0;
		dest_t *d = Dest;
		err = sem_wait(&Done);
		assert(err == 0);
		if (Terminate)
			cancelAll();
		(void) pthread_join(Reader,0);
		do {
			dest_t *n = d->next;
			void *status;
			if (d->name) {
				(void) pthread_join(d->thread,&status);
				if (status == 0)
					++numthreads;
			}
			free(d);
			d = n;
		} while (d);
		(void) gettimeofday(&now,0);
		diff = now.tv_sec - Starttime.tv_sec + (double) now.tv_usec / 1000000 - (double) Starttime.tv_usec / 1000000;
		summary(Numout * Blocksize + Rest, diff, numthreads);
	}
	if (Dest) {
		dest_t *d = Dest;
		do {
			if (d->result)
				warningmsg("error during output to %s: %s\n",d->arg,d->result);
			d = d->next;
		} while (d);
	}
	if (ErrorOccured)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}

/* vim:tw=0
 */
