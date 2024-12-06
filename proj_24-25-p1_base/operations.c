#include "operations.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

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

void start_backup(int *total_backups) {
  // create the file <job-name>-<backupnum>.bck
  // just need to fix the name
  char filename[MAX_JOB_FILE_NAME_SIZE];
  
  sprintf(filename, "nameOfTheFile-%d.bck", *total_backups);
  
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
      size_t len = strlen(buffer) - 1;

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

int kvs_backup(int max_backups, int *active_backups, int *total_backups) {

  kvs_wait_backup(max_backups, active_backups);

  // verify if we can afford to start another backup

  (*active_backups)++;
  pid_t pid = fork();

  // child process code
  if (pid == 0) {
    (*active_backups)++;
    start_backup(total_backups);
    (*active_backups)--;
    exit(0);

  // parent process code
  } else if (pid > 1) {
    (*total_backups)++;
    // probably will need a way to verify if the cilds are all done
  } else {
    printf("Fork Error");
    exit(1);
  }

  //(*active_backups)--;
  return 0;
}

void kvs_wait_backup(int max_backups, int *active_backups) {
  // for now i'll do an actve wait, later i can do it more efficient with a passive, one
  // need to talk with the professor about that
  while ((*active_backups) >= max_backups) {
    sleep(1);
  }

  return;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}