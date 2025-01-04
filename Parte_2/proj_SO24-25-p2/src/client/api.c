#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

// global variables for the pipe fds
int req_fd;
int resp_fd;
int notif_fd;

#define CONNECT_MESSAGE_LEN (MAX_PIPE_PATH_LENGTH * 3 + 4)

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {

  // close pipes
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  // create the request pipe
  if (mkfifo(req_pipe_path, 0640) != 0) {
    perror("[ERR]: mkfifo failed");
    exit(EXIT_FAILURE);
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
  notif_fd = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
  if (notif_fd < 0) {
    perror("Error opening notification pipe");
    return 1;
  }

  // open response pipe
  resp_fd = open(resp_pipe_path, O_RDONLY | O_NONBLOCK);
  if (resp_fd < 0) {
    perror("Error opening response pipe");
    return 1;
  }
  
  // open server pipe
  char tmp[MAX_PIPE_PATH_LENGTH];
  snprintf(tmp, MAX_PIPE_PATH_LENGTH, "/tmp/%s", server_pipe_path);
  int server_fd = open(tmp, O_WRONLY);
  if (server_fd < 0) {
    perror("Error opening server pipe");
    return 1;
  }

  // send connect message to server
  char connect_msg[CONNECT_MESSAGE_LEN];

  snprintf(connect_msg, CONNECT_MESSAGE_LEN, "1|%s|%s|%s", req_pipe_path, resp_pipe_path, notif_pipe_path);

  if (write_all(server_fd, connect_msg, CONNECT_MESSAGE_LEN) < 0) {
    perror("Error writing to server pipe");
    close(server_fd);
    return 1;
  }

  close(server_fd);

  // open request pipe
  req_fd = open(req_pipe_path, O_WRONLY);
  if (req_fd < 0) {
    perror("Error opening request pipe");
    return 1;
  }

  return 0;

}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char* key) {
  // create the request message
  char request[MAX_REQUEST_SIZE] = {0};
  snprintf(request, MAX_REQUEST_SIZE, "3|%s", key);

  printf("%s\n", key);printf("%d\n", req_fd);

  // write the message throw the request pipe
  if (write_all(req_fd, request, MAX_REQUEST_SIZE) < 0) {
    perror("Error writing to request pipe");
    return 1;
  }

  perror(key);
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  perror(key);
  return 0;
}


