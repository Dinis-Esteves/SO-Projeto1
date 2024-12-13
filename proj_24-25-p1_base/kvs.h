#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#include <stddef.h>
#include <pthread.h>
#include "constants.h"

typedef struct KeyNode {
    char *key;
    char *value;
    struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t table_locks[TABLE_SIZE];
} HashTable;

typedef struct stack {
    int top;
    char* arr[MAX_FILES];          
    pthread_mutex_t mutex;
} stack;

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

/// Hashes a key.
/// @param key Key to be hashed.
/// @return Hashed key.
int hash(const char *key);

/// Compares two keys.
/// @param a First key to be compared.
/// @param b Second key to be compared.
/// @return 0 if the keys are equal, 1 otherwise.
int compare_index(const void *a, const void *b);

/// Locks the table in order.
/// @param ht Hash table to be locked.
/// @param keys Keys to be locked.
/// @param count Number of keys to be locked.
/// @param write 1 if the lock is for writing, 0 otherwise.
void lock_table_in_order(HashTable *ht, const char (*keys)[MAX_STRING_SIZE], size_t count, int write);

/// Unlocks the table in order.
/// @param ht Hash table to be unlocked.
/// @param keys Keys to be unlocked.
/// @param count Number of keys to be unlocked.
void unlock_table_in_order(HashTable *ht, const char (*keys)[MAX_STRING_SIZE], size_t count);

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

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);


#endif  // KVS_H
