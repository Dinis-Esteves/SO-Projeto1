#include "kvs.h"
#include "string.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>

int compare_index(const void *a, const void *b) {
    return *(int*)a - *(int*)b;
}

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

void lock_table_in_order(HashTable *ht, const char (*keys)[MAX_STRING_SIZE], size_t count, int write) {
    int index[count];

    // Get the indices of the keys
    for (int i = 0; i < (int)count; i++) {
        index[i] = hash(keys[i]);
    }

    // Sort the keys in ascending order
    qsort(index, count, sizeof(char*), compare_index);

    // Lock the table in order
    for (int i = 0; i < (int)count; i++) {
        if (i == 0 || index[i] != index[i - 1]) {       // lock if not already locked
            if (write) {
                pthread_rwlock_wrlock(&ht->table_locks[index[i]]);
            } else {
                pthread_rwlock_rdlock(&ht->table_locks[index[i]]);
            }
        }
    }
}

void unlock_table_in_order(HashTable *ht, const char (*keys)[MAX_STRING_SIZE], size_t count) {
    int index[count];

    // Get the indices of the keys
    for (int i = 0; i < (int)count; i++) {
        index[i] = hash(keys[i]);
    }

    // Sort the keys in ascending order
    qsort(index, count, sizeof(char*), compare_index);

    // Unlock the table in order
    for (int i = (int)count - 1; i >= 0; i--) {
        if (i == (int)count - 1 || index[i] != index[i + 1]) {
            pthread_rwlock_unlock(&ht->table_locks[index[i]]);
        }
    }
}

struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
    	pthread_rwlock_init(&ht->table_locks[i], NULL);
  }
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
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

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];

		// free the read-write locks for this index
		pthread_rwlock_destroy(&ht->table_locks[i]);

        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
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


