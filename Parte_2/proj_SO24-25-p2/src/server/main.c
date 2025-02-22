#include <dirent.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <threads.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "constants.h"
#include "kvs.h"
#include "../common/io.h" 
#include "../common/constants.h"
#include "../common/protocol.h"
#include "parser.h"
#include "operations.h"

// global variables
stack* s;
FIFOBuffer* pc_buffer;
int max_backups;
int running;
int max_threads;
char* dir;
int *active_backups;
int still_running = 1;
pthread_mutex_t active_backups_mutex;
char fifo_pathname[MAX_PIPE_PATH_LENGTH];
pthread_mutex_t clients_mutex;
Client *clients[MAX_SESSION_COUNT] = {0};
atomic_int sigurs1_mem = 0;

// function to ignore the signals SIGUSR1 and SIGPIPE
void ignore_signals() {
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGUSR1);
  sigaddset(&mask, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &mask, NULL);
  }

// function to the manager pool threads
void* manager_pool() {

  ignore_signals();

  while (1) {
    char req_pipe_path[MAX_PIPE_PATH_LENGTH] = {0};
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH] = {0};
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH] = {0};

    // dequeue the connect request
    dequeue(pc_buffer, req_pipe_path, resp_pipe_path, notif_pipe_path);

    printf("req: %s\nresp: %s\nnotif: %s\n", req_pipe_path, resp_pipe_path, notif_pipe_path);

    // open the pipes 
    int notif_fd = open(notif_pipe_path, O_WRONLY);

    if (notif_fd == -1) {
      perror("Failed to open notification pipe");
      continue;
    }

    int resp_fd = open(resp_pipe_path, O_WRONLY);

    if (resp_fd == -1) {
      perror("Failed to open response pipe");
      close(notif_fd);
      continue;
    }
    
    int req_fd = open(req_pipe_path, O_RDONLY);

    if (req_fd == -1) {
      perror("Failed to open request pipe");
      close(notif_fd);
      close(resp_fd);
      continue;
    }

    Client *client = init_client(req_fd, resp_fd, notif_fd);

    // add the client to the client list
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      if (clients[i] == NULL) {
        clients[i] = client;
        break;
      }
    }
    pthread_mutex_unlock(&clients_mutex);
    
    // write the response to the client
    write_all(resp_fd, "1|OK\0", 5);
    int client_on = 1;

    while(client_on) {
      // read the request pipe
      char buffer[MAX_REQUEST_SIZE];
      int result = read_all(req_fd, buffer, sizeof(buffer), 0);
      if (result == -1) {
        printf("Pipe is compromised, disconnecting client...\n");
        for (int i = 0; i < MAX_NUMBER_SUB; i++) {
          if (strcmp(client->keys[i], "") != 0) {
            kvs_unsubscribe(client->keys[i], notif_fd);
          }
        }
        destroy_client(client);
        client_on = 0;
        break;
      } else if (result == 0) {
        printf("client disconnected suddenly\n");
        for (int i = 0; i < MAX_NUMBER_SUB; i++) {
          if (strcmp(client->keys[i], "") != 0) {
            kvs_unsubscribe(client->keys[i], notif_fd);
          }
        }
        destroy_client(client);
        client_on = 0;
        break;
      } else {
        int op_code = 0;
        char key[MAX_STRING_SIZE] = {0};
        sscanf(buffer, "%d|%s", &op_code, key);
        int res = 0;
        switch (op_code) {
        
        case OP_CODE_SUBSCRIBE:
          if (kvs_subscribe(key, notif_fd) == 0) {
            res = write_all(resp_fd, "3|OK\0", 5);
            subscribe_client(client, key);
          }
          else {
            res = write_all(resp_fd, "3|ERROR\0", 8);
          }
          if (res == -1) {
            printf("Pipe is compromised, disconnecting client...\n");
            for (int i = 0; i < MAX_NUMBER_SUB; i++) {
              if (strcmp(client->keys[i], "") != 0) {
                kvs_unsubscribe(client->keys[i], notif_fd);
              }
            }
            destroy_client(client);
            client_on = 0;
            break;
          }
          break;
          
        case OP_CODE_UNSUBSCRIBE:
          if (kvs_unsubscribe(key, notif_fd) == 0) {
            res = write_all(resp_fd, "4|OK\0", 5);

            unsubscribe_client(client, key);
          } else {
            res = write_all(resp_fd, "4|ERROR\0", 8);
          }
          if (res == -1) {
            printf("Pipe is compromised, disconnecting client...\n");
            for (int i = 0; i < MAX_NUMBER_SUB; i++) {
              if (strcmp(client->keys[i], "") != 0) {
                kvs_unsubscribe(client->keys[i], notif_fd);
              }
            }
            destroy_client(client);
            client_on = 0;
            break;
          }

          break;

        case OP_CODE_DISCONNECT:

        res = write_all(resp_fd, "2|OK\0", 5);
        
        if (res == -1) {
          printf("Pipe is compromised, disconnecting client...\n");
          client_on = 0;
          break;
        }

        // unsubscribe all the keys
        for (int i = 0; i < MAX_NUMBER_SUB; i++) {
          if (strcmp(client->keys[i], "") != 0) {
            kvs_unsubscribe(client->keys[i], notif_fd);
          }
         }

          destroy_client(client);
          // close the pipes
          close(req_fd);
          close(resp_fd);
          close(notif_fd);
          printf("Client Disconnected Succesfully\n");
          client_on = 0;
          break;

        }
      }  
    }
  }
  return NULL;
}

// function to pass in the threads
void* handle_job() {

  // ignore the signals
  ignore_signals();

  char* f;
  
  // run until the other funciton didn't end to span the dir and the stack is not empty
  while (still_running || !is_empty(s)) {
    while (still_running || !is_empty(s)) {
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

          if (kvs_backup(max_backups, active_backups, &total_backups, file_path_no_extension, &active_backups_mutex)) {
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

    free(f);
    close(fd);
    close(out_fd);
    }
    
    return NULL;
}

// function to handle the SIGUSR1 signal
void handle_sigusr1(int signo) {

  if (signo == SIGUSR1) {

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      Client *client = clients[i];
      if (client != NULL) {
         
        // unsubscribe client keys
        for (int j = 0; j < MAX_NUMBER_SUB; j++) {
          char *key = client->keys[j];
          if (strcmp(key, "") != 0) {
            kvs_unsubscribe(key, client->notif_fd);
          }
        }

        // close client pipes
        close(client->notif_fd);
        close(client->resp_fd);
        close(client->req_fd);
      }
    }
    pthread_mutex_unlock(&clients_mutex);
  }
}

void catch_sigusr1(int signo) {
  if (signo == SIGUSR1) {
    sigurs1_mem++;
    return;
  }
  return;
}

void* host() {
    // Set up signal handling
    signal(SIGUSR1, catch_sigusr1);
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);

    // Remove and recreate FIFO
    unlink(fifo_pathname);
    if (mkfifo(fifo_pathname, 0640) != 0) {
        perror("[ERR]: mkfifo failed");
        exit(EXIT_FAILURE);
    }

    while (1) {
        char buffer[MAX_PIPE_PATH_LENGTH * 3 + 4] = {0};
        
        int fd = open(fifo_pathname, O_RDONLY);
        if (fd == -1) {
            if (errno == EINTR) {
                handle_sigusr1(SIGUSR1); // Handle the signal safely
                continue;
            }
            perror("[ERR]: open failed");
            break;
        }

        int result = read_string(fd, buffer);
        close(fd);

        if (result == -1) {
            if (sigurs1_mem > 0) {
                continue;
            } else {
                perror("[ERR]: Failed to read from FIFO");
                break;
            }
        } else if (result == 0) {
            continue;
        }

        char req_pipe_path[MAX_PIPE_PATH_LENGTH];
        char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
        char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
        sscanf(buffer, "1|%[^|]|%[^|]|%[^|]", req_pipe_path, resp_pipe_path, notif_pipe_path);

        enqueue(pc_buffer, req_pipe_path, resp_pipe_path, notif_pipe_path);
    }

    return NULL;
}

int main(int argc, char *argv[]) {

  if (argc == 5) {

    // ignore signals

    running = 1;

    max_backups = atoi(argv[2]); 

    max_threads = atoi(argv[3]);

    char* tmp = argv[4];

    snprintf(fifo_pathname, MAX_PIPE_PATH_LENGTH, "/tmp/%s", tmp);

    active_backups = malloc(sizeof(int));

    *active_backups = 0;

    pthread_mutex_init(&active_backups_mutex, NULL);

    pthread_t *threads, *manager_threads;
    threads = malloc((long unsigned int)max_threads * sizeof(pthread_t));
    manager_threads = malloc((long unsigned int)MAX_SESSION_COUNT * sizeof(pthread_t));

    dir = argv[1];

    if (kvs_init()) {
      fprintf(stderr, "Failed to initialize KVS\n");
      return 1;
    }

    s = create_stack();
    if (s == NULL) {
      fprintf(stderr, "Failed to create stack\n");
      return 1;
    }

    pc_buffer = init_FIFO_buffer();
    if (pc_buffer == NULL) {
      fprintf(stderr, "Failed to create buffer\n");
      return 1;
    }
    struct dirent* d;

    DIR* folder = opendir(argv[1]);

    if (folder == NULL) {
      fprintf(stderr, "Error Opening Directory");
      return 0;
    }

    // create the host thread
    pthread_t *host_thread = malloc(sizeof(pthread_t));
    pthread_create(host_thread, NULL, &host, NULL);

    // create the pool of manager threads
    pthread_mutex_init(&clients_mutex, NULL);
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      if (pthread_create(&manager_threads[i], NULL, &manager_pool, NULL) != 0) {
          fprintf(stderr, "Failed to create thread %d\n", i);
          exit(EXIT_FAILURE);
      }
    }

    // create the number of threads specified in the input
    for (int i = 0; i < max_threads; i++) {
      if (pthread_create(&threads[i], NULL, &handle_job, NULL) != 0) {
          fprintf(stderr, "Failed to create thread %d\n", i);
          exit(EXIT_FAILURE);
      } 
    }

    ignore_signals();
    
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

    // wait for the manager threads to end
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      pthread_join(manager_threads[i], NULL);
    }
    
    // wait for the backups to end
    while (*active_backups > 0) {
      int terminated = wait(NULL);
      if (terminated == -1) {
        fprintf(stderr, "Error waiting for backup\n");
      } else {
        pthread_mutex_lock(&active_backups_mutex);
        (*active_backups)--;
        pthread_mutex_unlock(&active_backups_mutex);
      }
    }

    // notify the host thread to close
    running = 0;

    free(threads);
    free(manager_threads);
    free(active_backups);
    free(host_thread);
    pthread_mutex_destroy(&active_backups_mutex);
    destroy_stack(s);
    destroy_FIFO_buffer(pc_buffer);
    free(d);
    kvs_terminate();
    closedir(folder);
  }
  return 0;
}

