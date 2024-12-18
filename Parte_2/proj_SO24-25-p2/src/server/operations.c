#include "operations.h"
#include <pthread.h>
#include <sched.h>
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

void swap(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], int i, int j) {
  char temp[MAX_STRING_SIZE];

  memcpy(temp, keys[i], MAX_STRING_SIZE); 
  memcpy(keys[i], keys[j], MAX_STRING_SIZE); 
  memcpy(keys[j], temp, MAX_STRING_SIZE); 

  if (keys != values) {
    memcpy(temp, values[i], MAX_STRING_SIZE); 
    memcpy(values[i], values[j], MAX_STRING_SIZE); 
    memcpy(values[j], temp, MAX_STRING_SIZE); 
  }
}

void sortByHash(char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE], size_t size) {
  for (size_t i = 0; i < size - 1; ++i) {
    for (size_t j = 0; j < size - i - 1; ++j) {
      if (hash(keys[j]) > hash(keys[j + 1])) {
          swap(keys, values, (int) j, (int) j + 1);
      }
    }
  }
}


int* lock_all_keys(HashTable *ht, char key[][MAX_STRING_SIZE], size_t size, char type) {
  int *locks = calloc(TABLE_SIZE, sizeof(int) * TABLE_SIZE);

  for (size_t i = 0; i < size; i++) {
    int index = hash(key[i]);
    if (locks[index] == 0) {
      if (type == 'r') {
        pthread_rwlock_rdlock(&ht->rwlock[index]);
      } else {
        pthread_rwlock_wrlock(&ht->rwlock[index]);
      }
    }
      locks[index] = 1;
  }
  
  return locks;
}

void unlock_all_keys(HashTable *ht, int *locks) {
  for (size_t i = 0; i < TABLE_SIZE ; i++) {
    if (locks[i] == 1) {
      pthread_rwlock_unlock(&ht->rwlock[i]);
    }
  }

  free(locks);
  return;
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

  // sort the pair and aquire the locks in order to avoid deadlocks
  sortByHash(keys, values, num_pairs);
  int *locks = lock_all_keys(kvs_table, keys, num_pairs, 'w');
  
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  unlock_all_keys(kvs_table, locks);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
    char key[MAX_WRITE_SIZE];  
    size_t key_len = 0;

    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    sortByHash(keys, keys, num_pairs);
    int *locks = lock_all_keys(kvs_table, keys, num_pairs, 'r');

    snprintf(key, sizeof(key), "[");
    key_len = strlen(key);

    for (size_t i = 0; i < num_pairs; i++) {
        char* result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            key_len += (size_t)snprintf(key + key_len, sizeof(key) - key_len, "(%s,KVSERROR)", keys[i]);
        } else {
            key_len += (size_t)snprintf(key + key_len, sizeof(key) - key_len, "(%s,%s)", keys[i], result);
        }
        free(result);
    }

    snprintf(key + key_len, sizeof(key) - key_len, "]\n");

    write_to_open_file(fd, key);

    unlock_all_keys(kvs_table, locks);

    return 0;
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    sortByHash(keys, keys, num_pairs);
    int *locks = lock_all_keys(kvs_table, keys, num_pairs, 'w');

    int swt = 0;
    char error_message[MAX_WRITE_SIZE];
    size_t error_len = 0;

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (swt == 0) {
                write_to_open_file(fd, "[");
                swt = 1;
            }
            error_len += (size_t) snprintf(error_message + error_len, sizeof(error_message) - error_len, "(%s,KVSMISSING)", keys[i]);
            write_to_open_file(fd, error_message);
            error_len = 0; // Reset error_len for the next message
        }
    }

    if (swt == 1) {
        write_to_open_file(fd, "]\n");
    }

    unlock_all_keys(kvs_table, locks);
    return 0;
}


void kvs_show(int fd) {
    for (int j = 0; j < TABLE_SIZE; j++) {
        pthread_rwlock_rdlock(&kvs_table->rwlock[j]);
    }

    char buffer[MAX_WRITE_SIZE * TABLE_SIZE];
    size_t buffer_len = 0;

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = kvs_table->table[i];

        while (keyNode != NULL) {
            buffer_len += (size_t) snprintf(buffer + buffer_len, sizeof(buffer) - buffer_len, "(%s, %s)\n", keyNode->key, keyNode->value);
            keyNode = keyNode->next; // Move to the next node
        }
    }

    write_to_open_file(fd, buffer);

    for (int j = 0; j < TABLE_SIZE; j++) {
        pthread_rwlock_unlock(&kvs_table->rwlock[j]);
    }
}



void start_backup(int *total_backups, char* filename) {
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
  
  if (max_backups == 0) {
    return 0;
  } 

  kvs_wait_backup(max_backups, active_backups, active_backups_mutex);

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
    pid_t terminated_pid = wait(NULL); 
    if (terminated_pid > 0) {
      pthread_mutex_lock(active_backups_mutex);
      (*active_backups)--; 
      pthread_mutex_unlock(active_backups_mutex);
    } else {
    } 

  }  
  return;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
