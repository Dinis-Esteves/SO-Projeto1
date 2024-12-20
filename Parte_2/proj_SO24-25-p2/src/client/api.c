#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) {
  
  // open pipes
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  // open request pipe
  int req_fd = open(req_pipe_path, O_RDONLY);
  if (req_fd < 0) {
    perror("Error opening request pipe");
    return 1;
  }

  // open response pipe
  int resp_fd = open(resp_pipe_path, O_WRONLY);
  if (resp_fd < 0) {
    perror("Error opening response pipe");
    close(req_fd);
    return 1;
  }

  // open notification pipe
  int notif_fd = open(notif_pipe_path, O_RDONLY);
  if (notif_fd < 0) {
    perror("Error opening notification pipe");
    close(req_fd);
    close(resp_fd);
    return 1;
  }

  // open server pipe
  int server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd < 0) {
    perror("Error opening server pipe");
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return 1;
  }

  // send connect message to server
  


  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  return 0;
}


