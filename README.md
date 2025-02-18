# SO-Projeto1
## A Multi-Threaded Redis-Like Server in C

This project was developed as part of a Operating Systems (SO) course, focusing on creating a multi-threaded, Redis-like server in C. The server is designed to process .job files, which contain operations to be performed on a key-value store implemented using a hashtable. The server supports concurrent operations, including backups, and was later extended to allow client connections and real-time notifications via pipes.

## Key Features:
Multi-Threaded Server:

The server is capable of handling multiple threads, each responsible for reading and processing .job files.

These files contain commands to manipulate a shared hashtable (e.g., insert, update, delete, or retrieve key-value pairs).

Thread synchronization is implemented to ensure safe access to the shared data structure.

### Concurrent Backups:

The server can perform backups of the hashtable concurrently with other operations.

This ensures data persistence without blocking the main server operations.

### Client Subscription System:

In the second part of the project, the server was extended to allow clients to connect and subscribe to specific keys.

Clients receive real-time notifications via pipes whenever a subscribed key is updated or deleted.

This feature mimics the publish-subscribe pattern found in systems like Redis.

### Signal Handling:

A custom signal handler was implemented to reset the server.

When triggered, the server disconnects all clients, clears their subscriptions, and resets the hashtable to its initial state.

## Technical Details:
Language: C

### Data Structures:

Hashtable for storing key-value pairs.

Thread-safe mechanisms (e.g., mutexes) to handle concurrent access.

Inter-Process Communication (IPC):

Pipes are used for communication between the server and clients.

### Signal Handling:

Custom signal handlers (e.g., for SIGINT or SIGTERM) to manage server resets gracefully.

## Challenges:
Ensuring thread safety while allowing concurrent operations.

Implementing a reliable notification system for client subscriptions.

Handling edge cases in signal handling to avoid resource leaks or undefined behavior.
