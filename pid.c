/* -*-pgsql-c-*- */
/*
 * $Header$
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2013	PgPool Global Development Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 */
#include "pool.h"
#include "pool_config.h"
#include "pool_process_context.h"

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static BackendStatusRecord backend_rec;	/* Backend status record */

/*
* read the pid file
*/
int read_pid_file(void)
{
	int fd;
	int readlen;
	char pidbuf[128];

	fd = open(pool_config->pid_file_name, O_RDONLY);
	if (fd == -1)
	{
		return -1;
	}
	if ((readlen = read(fd, pidbuf, sizeof(pidbuf))) == -1)
	{
		pool_error("could not read pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		close(fd);
		return -1;
	}
	else if (readlen == 0)
	{
		pool_error("EOF detected while reading pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		close(fd);
		return -1;
	}
	close(fd);
	return(atoi(pidbuf));
}

/*
* write the pid file
*/
void write_pid_file(void)
{
	int fd;
	char pidbuf[128];

	fd = open(pool_config->pid_file_name, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
	if (fd == -1)
	{
		pool_error("could not open pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		pool_shmem_exit(1);
		exit(1);
	}
	snprintf(pidbuf, sizeof(pidbuf), "%d", (int)getpid());
	if (write(fd, pidbuf, strlen(pidbuf)+1) == -1)
	{
		pool_error("could not write pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		close(fd);
		pool_shmem_exit(1);
		exit(1);
	}
	if (fsync(fd) == -1)
	{
		pool_error("could not fsync pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		close(fd);
		pool_shmem_exit(1);
		exit(1);
	}	
	if (close(fd) == -1)
	{
		pool_error("could not close pid file as %s. reason: %s",
				   pool_config->pid_file_name, strerror(errno));
		pool_shmem_exit(1);
		exit(1);
	}
}

/*
* Read the status file
*/
int read_status_file(bool discard_status)
{
	FILE *fd;
	char fnamebuf[POOLMAXPATHLEN];
	int i;
	bool someone_wakeup = false;

	snprintf(fnamebuf, sizeof(fnamebuf), "%s/%s", pool_config->logdir, STATUS_FILE_NAME);
	fd = fopen(fnamebuf, "r");
	if (!fd)
	{
		pool_log("Backend status file %s does not exist", fnamebuf);
		return -1;
	}

	/*
	 * If discard_status is true, unlink pgpool_status and
	 * do not restore previous status.
	 */
	if (discard_status)
	{
		fclose(fd);
		if (unlink(fnamebuf) == 0)
		{
			pool_log("Backend status file %s discarded", fnamebuf);
		}
		else
		{
			pool_error("Failed to discard backend status file %s reason:%s", fnamebuf, strerror(errno));
		}
		return 0;
	}

	if (fread(&backend_rec, 1, sizeof(backend_rec), fd) != sizeof(backend_rec))
	{
		pool_error("Could not read backend status file as %s. reason: %s",
				   fnamebuf, strerror(errno));
		fclose(fd);
		return -1;
	}
	fclose(fd);

	for (i=0;i< pool_config->backend_desc->num_backends;i++)
	{
		if (backend_rec.status[i] == CON_DOWN)
		{
			BACKEND_INFO(i).backend_status = CON_DOWN;
			pool_log("read_status_file: %d th backend is set to down status", i);
		}
		else
		{
			BACKEND_INFO(i).backend_status = CON_CONNECT_WAIT;
			someone_wakeup = true;
		}
	}

	/*
	 * If no one woke up, we regard the status file bogus
	 */
	if (someone_wakeup == false)
	{
		for (i=0;i< pool_config->backend_desc->num_backends;i++)
		{
			BACKEND_INFO(i).backend_status = CON_CONNECT_WAIT;
		}
	}

	return 0;
}

/*
* Write the pid file
*/
int write_status_file(void)
{
	FILE *fd;
	char fnamebuf[POOLMAXPATHLEN];
	int i;

	snprintf(fnamebuf, sizeof(fnamebuf), "%s/%s", pool_config->logdir, STATUS_FILE_NAME);
	fd = fopen(fnamebuf, "w");
	if (!fd)
	{
		pool_error("Could not open status file %s", fnamebuf);
		return -1;
	}

	memset(&backend_rec, 0, sizeof(backend_rec));

	for (i=0;i< pool_config->backend_desc->num_backends;i++)
	{
		backend_rec.status[i] = BACKEND_INFO(i).backend_status;
	}

	if (fwrite(&backend_rec, 1, sizeof(backend_rec), fd) != sizeof(backend_rec))
	{
		pool_error("Could not write backend status file as %s. reason: %s",
				   fnamebuf, strerror(errno));
		fclose(fd);
		return -1;
	}
	fclose(fd);
	return 0;
}


