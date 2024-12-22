#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>
#include "api.h"
#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"


int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // open pipes
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  // create the request pipe
  if (mkfifo(req_pipe_path, 0640) != 0) {
    perror("[ERR]: mkfifo failed");
    exit(EXIT_FAILURE);
  }

  // open request pipe

  int req_fd = open(req_pipe_path, O_RDONLY | O_NONBLOCK);
  if (req_fd < 0) {
    perror("Error opening request pipe");
    return 1;
  }

  // create the response pipe 
  if (mkfifo(resp_pipe_path, 0640) != 0) {
    perror("[ERR]: mkfifo failed");
    exit(EXIT_FAILURE);
  }


  // create the notifications pipe 
  if (mkfifo(notif_pipe_path, 0640) != 0) {
    perror("[ERR]: mkfifo failed");
    exit(EXIT_FAILURE);
  }

  // open notification pipe
  int notif_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
  if (notif_fd < 0) {
    perror("Error opening notification pipe");
    close(req_fd);
    return 1;
  }

  kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path);

  // open response pipe
  int resp_fd = open(resp_pipe_path, O_WRONLY);
  if (resp_fd < 0) {
    perror("Error opening response pipe");
    close(req_fd);
    return 1;
  }
  while (1) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        // TODO: end notifications thread
        printf("Disconnected from server\n");
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_subscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
}
