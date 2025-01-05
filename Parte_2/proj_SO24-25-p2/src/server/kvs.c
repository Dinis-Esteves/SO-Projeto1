#include "kvs.h"
#include "../common/constants.h"
#include "../common/io.h"
#include "string.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      pthread_rwlock_init(&ht->rwlock[i], NULL);
      ht->table[i] = NULL;
  }
  return ht;
}

void notify_clients(int fds[], const char *key, const char* value) {
    char message[sizeof(key) + sizeof(value) + 4] = {0};
    snprintf(message, MAX_STRING_SIZE, "(%s,%s)", key, value);

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (fds[i] > 0) {
            write_all(fds[i], message, sizeof(message));
        }
    }
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
            notify_clients(keyNode->client_fds, key, value);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return copy of the value if found
        }
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            notify_clients(keyNode->client_fds, key, "DELETED");
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }
    return 1;
}

int subscribe_key(HashTable *ht, const char *key, int client_fd) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // find the key in the hash table
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {

            // find an empty slot in the client_fds array
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (keyNode->client_fds[i] <= 0) {
                    keyNode->client_fds[i] = client_fd;
                    return 0;
                }
            }

            return 0;
        }
        keyNode = keyNode->next;
    }
    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            pthread_rwlock_destroy(&ht->rwlock[i]);
            free(temp);
        }
    }
    free(ht);
}

stack* create_stack() {
    stack* s = malloc(sizeof(stack));
    if (!s) return NULL;
    s->top = -1;
    pthread_mutex_init(&s->mutex, NULL);
    return s;
}

int is_empty(stack* s) {
    pthread_mutex_lock(&s->mutex);
    int result = s->top == -1;
    pthread_mutex_unlock(&s->mutex);
    return result;
}

void push(stack* s, char* key) {
    pthread_mutex_lock(&s->mutex);
    if (s->top == MAX_FILES - 1) {
        pthread_mutex_unlock(&s->mutex);
        return;
    }
    s->arr[++s->top] = strdup(key);
    pthread_mutex_unlock(&s->mutex);
}

char* pop(stack* s) {
    pthread_mutex_lock(&s->mutex);
    if (s->top == -1) {  
        pthread_mutex_unlock(&s->mutex);
        return NULL;
    }
    char* key = s->arr[s->top];
    s->top--;
    pthread_mutex_unlock(&s->mutex);
    return key;
}

void destroy_stack(stack* s) {
    pthread_mutex_destroy(&s->mutex);
    free(s);
}

FIFOBuffer* init_FIFO_buffer() {
    FIFOBuffer *fifo = (FIFOBuffer *)malloc(sizeof(FIFOBuffer)); 
    if (!fifo) {
        perror("Failed to allocate memory for FIFO buffer");
        exit(EXIT_FAILURE);
    }
    fifo->front = 0;
    fifo->rear = 0;

    sem_init(&fifo->empty, 0, MAX_CLIENTS); 
    sem_init(&fifo->full, 0, 0);        
    pthread_mutex_init(&fifo->mutex, NULL);

    // Allocate memory for each string in the buffer
    for (int i = 0; i < MAX_CLIENTS; i++) {
        fifo->buffer[i] = malloc(MAX_PIPE_PATH_LENGTH * 3 + 3); // For 3 pipes and delimiters
    }

    return fifo;
}

void destroy_FIFO_buffer(FIFOBuffer *fifo) {
    sem_destroy(&fifo->empty);
    sem_destroy(&fifo->full);
    pthread_mutex_destroy(&fifo->mutex);
    free(fifo);
}

void enqueue(FIFOBuffer *fifo, const char *req_pipe, const char *resp_pipe, const char *notif_pipe) {
    sem_wait(&fifo->empty);

    pthread_mutex_lock(&fifo->mutex);
    snprintf(fifo->buffer[fifo->rear], MAX_PIPE_PATH_LENGTH * 3 + 3, "%s|%s|%s", req_pipe, resp_pipe, notif_pipe); // Format the message
    fifo->buffer[fifo->rear][MAX_PIPE_PATH_LENGTH * 3 + 2] = '\0'; // Explicitly null-terminate
    fifo->rear = (fifo->rear + 1) % MAX_CLIENTS; // Update the rear pointer
    pthread_mutex_unlock(&fifo->mutex);

    sem_post(&fifo->full); // Signal that a slot is now full
}

void dequeue(FIFOBuffer *fifo, char *req_pipe, char *resp_pipe, char *notif_pipe) {
    sem_wait(&fifo->full); // Wait for a full slot

    pthread_mutex_lock(&fifo->mutex);
    char *message = fifo->buffer[fifo->front];
    fifo->front = (fifo->front + 1) % MAX_CLIENTS; // Update the front pointer
    pthread_mutex_unlock(&fifo->mutex);

    // Parse the message into the three pipes
    sscanf(message, "%39[^|]|%39[^|]|%39[^|]", req_pipe, resp_pipe, notif_pipe);

    sem_post(&fifo->empty); // Signal that a slot is now empty
}