#include "operations.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
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

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  printf("[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      printf("(%s,KVSERROR)", keys[i]);
    } else {
      printf("(%s,%s)", keys[i], result);
    }
    free(result);
  }
  printf("]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        printf("[");
        aux = 1;
      }
      printf("(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    printf("]\n");
  }

  return 0;
}

void kvs_show() {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
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
    printf("Error opening the file");
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
          printf("Error writing");
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

int kvs_backup(int max_backups, int *active_backups, int *total_backups, char* filename) {

  kvs_wait_backup(max_backups, active_backups);

  // verify if we can afford to start another backup

  pid_t pid = fork();
  
  // child process code
  if (pid == 0) {
    start_backup(total_backups, filename);
    exit(0);

  // parent process code
  } else if (pid > 1) {
    (*total_backups)++;
    (*active_backups)++;
  } else {
    printf("Fork Error");
    exit(1);
  }

  return 0;
}

void kvs_wait_backup(int max_backups, int *active_backups) {
  // when the limit is reached the parent will be blocked until a child ends
  while ((*active_backups) >= max_backups) {
    int status;
    pid_t terminated_pid = waitpid(-1, &status, 0); 
    if (terminated_pid > 0) {
      (*active_backups)--; 
    }
  }

  return;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}