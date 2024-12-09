#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include "constants.h"
#include "kvs.h"
#include "parser.h"
#include "operations.h"

stack* s;
int max_backups;
int max_threads;
char* dir;
int* active_backups;
int still_running = 1;

// struct to pass in the threads
typedef struct {
  char* file_path_no_extension;
  int fd;
  int out_fd;
  int max_backups;
  int* active_backups;
} thread_args;

// function to pass in the threads
void* handle_job() {
  char* f;
  
  // the other funciton didnt end to span the dir and the stack is not empty
  while (still_running || !is_empty(s)) {
    while (1) {
      f = pop(s);
      if (f != NULL) {
        break;
      }
    }

    int stop = 1;
    int total_backups = 1;

    char file_path[PATH_MAX];
    char filename[FILENAME_MAX];
    char file_path_no_extension[FILENAME_MAX];

    snprintf(file_path, sizeof(file_path), "%s/%s.job", dir, f);
    snprintf(filename, sizeof(filename), "%s/%s.out", dir, f);
    snprintf(file_path_no_extension, sizeof(file_path_no_extension), "%s/%s", dir, f);

    int fd = open(file_path, O_RDONLY, S_IRUSR | S_IWUSR);
    int out_fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);

    if (fd == -1) {
      fprintf(stderr, "Error opening file\n");
    }


    while (stop) {
      char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      unsigned int delay;
      size_t num_pairs;

      switch (get_next(fd)) {
        case CMD_WRITE:
          num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_write(num_pairs, keys, values)) {
            fprintf(stderr, "Failed to write pair\n");
          }

          break;

        case CMD_READ:
          num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_read(num_pairs, keys, out_fd)) {
            fprintf(stderr, "Failed to read pair\n");
          }
          break;

        case CMD_DELETE:
          num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

          if (num_pairs == 0) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (kvs_delete(num_pairs, keys, out_fd)) {
            fprintf(stderr, "Failed to delete pair\n");
          }
          break;

        case CMD_SHOW:

          kvs_show(out_fd);
          break;

        case CMD_WAIT:
          if (parse_wait(fd, &delay, NULL) == -1) {
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            continue;
          }

          if (delay > 0) {
            write_to_open_file(out_fd, "Waiting...\n");
            kvs_wait(delay);
          }
          break;

        case CMD_BACKUP:

          if (kvs_backup(max_backups, active_backups, &total_backups, file_path_no_extension)) {
            fprintf(stderr, "Failed to perform backup.\n");
          }
          break;

        case CMD_INVALID:
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          break;

        case CMD_HELP: {
          char help_info[MAX_WRITE_SIZE];
          
          snprintf(help_info, sizeof(help_info), 
          "Available commands:\n"
              "  WRITE [(key,value)(key2,value2),...]\n"
              "  READ [key,key2,...]\n"
              "  DELETE [key,key2,...]\n"
              "  SHOW\n"
              "  WAIT <delay_ms>\n"
              "  BACKUP\n" 
              "  HELP\n");

          write_to_open_file(out_fd, help_info);

          break;
          }             
        case CMD_EMPTY:
          break;

        case EOC:
          stop--;
          break;
      }
    }

    close(fd);
    close(out_fd);
    }
    return NULL;
}


int main(int argc, char *argv[]) {

  if (argc == 4) {

    max_backups = (*argv[2]) - '0';  

    max_threads = (*argv[3]) - '0';

    pthread_t threads[max_threads];

    dir = argv[1];

    if (kvs_init()) {
      fprintf(stderr, "Failed to initialize KVS\n");
      return 1;
    }

    s = create_stack();
    if (create_stack() == NULL) {
      fprintf(stderr, "Failed to create stack\n");
      return 1;
    }

    struct dirent* d;

    DIR* folder = opendir(argv[1]);

    if (folder == NULL) {
      fprintf(stderr, "Error Opening Directory");
      return 0;
    }

    // create the number of threads specified in the input
    for (int i = 0; i < max_threads; i++) {
      int* thread_id = malloc(sizeof(int)); 
      if (!thread_id) {
          fprintf(stderr, "Failed to allocate memory for thread ID\n");
          exit(EXIT_FAILURE);
      }

      *thread_id = i; 

      if (pthread_create(&threads[i], NULL, handle_job(), thread_id) != 0) {
          fprintf(stderr, "Failed to create thread %d\n", i);
          exit(EXIT_FAILURE);
      } 
    }

    while ((d = readdir(folder)) != NULL) {
      char* f;
      if ((f = is_job(d->d_name, d->d_type)) != NULL) {
        // push the job into the stack, so that the threads can get him
        push(s, f);
      }
    }

    // notify the threads that there won't be more jobs than the ones in the stack
    still_running = 0;

    // wait for the threads to end
    for (int i = 0; i < max_threads; i++) {
    pthread_join(threads[i], NULL);
    }

    kvs_terminate();
    closedir(folder);
  }
  
  }

