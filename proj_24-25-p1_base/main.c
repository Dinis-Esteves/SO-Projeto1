#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

int active_backups = 0;
int total_backups = 1;

int main(int argc, char *argv[]) {

  if (argc == 3) {
    int max_backups = (*argv[2]) - '0';      

    if (kvs_init()) {
      fprintf(stderr, "Failed to initialize KVS\n");
      return 1;
    }

    struct dirent* d;

    DIR* folder = opendir(argv[1]);

    if (folder == NULL) {
      printf("Error Opening Directory");
      return 0;
    }

    while ((d = readdir(folder)) != NULL) {
      //printf("%s\n", d->d_name);

      if (is_job(d->d_name, d->d_type)) {

        char file_path[PATH_MAX];
        snprintf(file_path, sizeof(file_path), "%s/%s", argv[1], d->d_name);

        int fd = open(file_path, O_RDONLY, S_IRUSR | S_IWUSR);

        if (fd == -1) {
          printf("Error opening file\n");
        }

        int stop = 1;

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

              if (kvs_read(num_pairs, keys)) {
                fprintf(stderr, "Failed to read pair\n");
              }
              break;

            case CMD_DELETE:
              num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

              if (num_pairs == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                continue;
              }

              if (kvs_delete(num_pairs, keys)) {
                fprintf(stderr, "Failed to delete pair\n");
              }
              break;

            case CMD_SHOW:

              kvs_show();
              break;

            case CMD_WAIT:
              if (parse_wait(fd, &delay, NULL) == -1) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                continue;
              }

              if (delay > 0) {
                printf("Waiting...\n");
                kvs_wait(delay);
              }
              break;

            case CMD_BACKUP:

              if (kvs_backup(max_backups, &active_backups, &total_backups)) {
                fprintf(stderr, "Failed to perform backup.\n");
              }
              break;

            case CMD_INVALID:
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              break;

            case CMD_HELP:
              printf( 
                  "Available commands:\n"
                  "  WRITE [(key,value)(key2,value2),...]\n"
                  "  READ [key,key2,...]\n"
                  "  DELETE [key,key2,...]\n"
                  "  SHOW\n"
                  "  WAIT <delay_ms>\n"
                  "  BACKUP\n" // Not implemented
                  "  HELP\n"
              );

              break;
              
            case CMD_EMPTY:
              break;

            case EOC:
              stop--;
              break;
          }
        }
      close(fd);
      }
    }
    kvs_terminate();
    closedir(folder);
  }
}
