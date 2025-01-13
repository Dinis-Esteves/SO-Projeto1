#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#define TABLE_SIZE 26
#define MAX_FILES 10000

#include <stddef.h>
#include "constants.h"
#include "../common/constants.h"
#include <pthread.h>
#include <semaphore.h>

typedef struct KeyNode {
    char *key;
    char *value;
    int client_fds[MAX_SESSION_COUNT];
    pthread_mutex_t mutex;
    struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t rwlock[TABLE_SIZE];
} HashTable;

typedef struct stack {
    int top;
    char* arr[MAX_FILES];          
    pthread_mutex_t mutex;   
} stack;

typedef struct FIFOBuffer {
    char *buffer[MAX_SESSION_COUNT];         
    int size;
    int front;
    int rear;
    sem_t empty;
    sem_t full;
    pthread_mutex_t mutex;
} FIFOBuffer; 

typedef struct Client {
    int req_fd;
    int resp_fd;
    int notif_fd;
    char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE];
} Client;

/// Initializes the FIFO buffer.
/// @return Newly created FIFO buffer, NULL on failure
FIFOBuffer* init_FIFO_buffer();

/// Initializes a new client.
/// @param req_fd File descriptor of the request pipe.
/// @param resp_fd File descriptor of the response pipe.
/// @param notif_fd File descriptor of the notification pipe.
/// @return Newly created client, NULL on failure
Client *init_client(int req_fd, int resp_fd, int notif_fd);

/// Destroys the client.
/// @param client Client to be destroyed.
/// @return void
void destroy_client(Client *client);

/// Subscribes a client to a key.
/// @param client Client to be subscribed.
/// @param key Key to be subscribed to.
/// @return void
void subscribe_client(Client *client, const char *key);

/// Unsubscribes a client from a key.
/// @param client Client to be unsubscribed.
/// @param key Key to be unsubscribed from.
/// @return void
void unsubscribe_client(Client *client, const char *key);

/// Destroys the FIFO buffer.
/// @param buffer FIFO buffer to be destroyed.
/// @return void
void destroy_FIFO_buffer(FIFOBuffer *buffer);

/// Enqueues a new element to the FIFO buffer.
/// @param fifo FIFO buffer to be modified.
/// @param req_pipe Request pipe to be enqueued.
/// @param resp_pipe Response pipe to be enqueued.
/// @param notif_pipe Notification pipe to be enqueued.
/// @return void
void enqueue(FIFOBuffer *fifo, const char *req_pipe, const char *resp_pipe, const char *notif_pipe);

/// Dequeues an element from the FIFO buffer.
/// @param fifo FIFO buffer to be modified.
/// @param req_pipe Request pipe to be dequeued.
/// @param resp_pipe Response pipe to be dequeued.
/// @param notif_pipe Notification pipe to be dequeued.
/// @return void
void dequeue(FIFOBuffer *fifo, char *req_pipe, char *resp_pipe, char *notif_pipe);

/// Creates a new stack.
/// @return Newly created stack, NULL on failure
stack* create_stack();

/// checks if the stack is empty.
/// @param s stack to be checked.
/// @return 1 if the stack is empty, 0 otherwise.
int is_empty(stack* s);

/// pushes a new key to the stack.
/// @param s stack to be modified.
/// @param key Key to be pushed.
/// @return void
void push(stack* s, char* key);

/// pops the top key from the stack.
/// @param s stack to be modified.
char* pop(stack* s);

/// Destroys the stack.
/// @param s stack to be deleted.
/// @return void.
void destroy_stack(stack* s);

/// Creates a new event hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

/// Appends a new key value pair to the hash table.
/// @param ht Hash table to be modified.
/// @param key Key of the pair to be written.
/// @param value Value of the pair to be written.
/// @return 0 if the node was appended successfully, 1 otherwise.
int write_pair(HashTable *ht, const char *key, const char *value);

/// Deletes the value of given key.
/// @param ht Hash table to delete from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Appends a new node to the list.
/// @param list Event list to be modified.
/// @param key Key of the pair to read.
/// @return 0 if the node was appended successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Subscribes a client to a key.
/// @param ht Hash table to be modified.
/// @param key Key to be subscribed to.
/// @param client_fd File descriptor of the client to be subscribed.
/// @return 1 if the key was not found, 0 otherwise.
int subscribe_key(HashTable *ht, const char *key, int client_fd);

/// Unsubscribes a client from a key.
/// @param ht Hash table to be modified.
/// @param key Key to be unsubscribed from.
/// @param client_fd File descriptor of the client to be unsubscribed.
/// @return 1 if the key was not found, 0 otherwise.
int unsubscribe_key(HashTable *ht, const char *key, int client_fd);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Hashes a key.
/// @param key Key to be hashed.
/// @return Hashed key.
int hash(const char *key);



#endif  // KVS_H
