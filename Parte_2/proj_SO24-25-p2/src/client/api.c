#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>


#define CONNECT_MESSAGE_LEN (MAX_PIPE_PATH_LENGTH * 3 + 4)

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  
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
  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  perror(key);
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  perror(key);
  return 0;
}


