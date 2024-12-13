#include "operations.h"
#include <bits/pthreadtypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <threads.h>
#include <time.h>
#include <unistd.h>
#include <sys/wait.h>
#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void write_to_open_file(int fd, const char *content) {
  size_t len = strlen(content);
  size_t done = 0;

  while (len > done) {
    ssize_t bytes_written = write(fd, content + done, len - done);

    if (bytes_written < 0) {
      fprintf(stderr, "Write error");
      return;
    }

    done += (size_t) bytes_written;
  }
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // lock relevant locks for writting
  lock_table_in_order(kvs_table, keys, num_pairs, 1);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  // unlock relevant locks
  unlock_table_in_order(kvs_table, keys, num_pairs);

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  char key[MAX_WRITE_SIZE];  

  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  
  write_to_open_file(fd, "[");

  // lock relevant locks for reading
  lock_table_in_order(kvs_table, keys, num_pairs, 0);

  // read the pairs
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      snprintf(key, sizeof(key), "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(key, sizeof(key), "(%s,%s)", keys[i], result);
    }
    write_to_open_file(fd, key);
    free(result);
  }

  // unlock relevant locks
  unlock_table_in_order(kvs_table, keys, num_pairs);

  write_to_open_file(fd, "]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // lock relevant locks for writting
  lock_table_in_order(kvs_table, keys, num_pairs, 1);

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      char error_message[MAX_WRITE_SIZE];
      snprintf(error_message, sizeof(error_message), "[(%s,KVSMISSING)]\n", keys[i]);
      write_to_open_file(fd, error_message);
    } 
  }

  // unlock relevant locks
  unlock_table_in_order(kvs_table, keys, num_pairs);

  return 0;
}

void kvs_show(int fd) {
  // block every lock for reading
  for (int i = 0; i < TABLE_SIZE; i++) {
    pthread_rwlock_rdlock(&kvs_table->table_locks[i]);
  }

  // write the show command to the file
  for (int i = 0; i < TABLE_SIZE; i++) {
    char current_key[MAX_WRITE_SIZE];
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      snprintf(current_key, sizeof(current_key), "(%s, %s)\n", keyNode->key, keyNode->value);
      write_to_open_file(fd, current_key);
      keyNode = keyNode->next; // Move to the next node
    }
  }

  // unlock every lock
  for (int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_unlock(&kvs_table->table_locks[i]);
  }
}

void start_backup(int *total_backups, char* filename) {
  // create the file <job-name>-<backupnum>.bck
  // just need to fix the name
  char temp_filename[MAX_JOB_FILE_NAME_SIZE];

  strncpy(temp_filename, filename, sizeof(temp_filename) - 1);
  
  sprintf(filename, "%s-%d.bck", temp_filename, *total_backups);
  
  int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

  if (fd == -1) {
    fprintf(stderr, "Error opening the file\n");
    return;
  }

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      
      // add the current pair to a buffer
      char buffer[MAX_WRITE_SIZE] = {0};

      sprintf(buffer, "(%s, %s)\n", keyNode->key, keyNode->value);


      size_t done = 0;
      size_t len = strlen(buffer);

      while (len > done) {
        ssize_t written = write(fd, buffer + done, len - done);

        if (written < 0) {
          fprintf(stderr, "Error writing");
          close(fd);
          return;
        }

        done += (size_t) written;
      }
      keyNode = keyNode->next;
    }
  }
  close(fd);
  return;
}

int kvs_backup(int max_backups, int *active_backups, int *total_backups, char* filename, pthread_mutex_t* active_backups_mutex) {
  kvs_wait_backup(max_backups, active_backups, active_backups_mutex);

  // verify if we can afford to start another backup
  pid_t pid = fork();
  
  // child process code
  if (pid == 0) {
    start_backup(total_backups, filename);
    kvs_terminate();
    exit(0);

  // parent process code
  } else if (pid > 1) {
    pthread_mutex_lock(active_backups_mutex);
    (*total_backups)++;
    (*active_backups)++;
    pthread_mutex_unlock(active_backups_mutex);
    
  } else {
    fprintf(stderr, "Fork Error");
    exit(1);
  }

  return 0;
}

void kvs_wait_backup(int max_backups, int *active_backups, pthread_mutex_t* active_backups_mutex) {
  // when the limit is reached the parent will be blocked until a child ends
  while (*active_backups >= max_backups) {
    int terminated_pid = wait(NULL); 
    if (terminated_pid > 0) {
      pthread_mutex_lock(active_backups_mutex);
      (*active_backups)--; 
      pthread_mutex_unlock(active_backups_mutex);
    } else {
      kvs_wait(1);
    } 

  }  
  return;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
