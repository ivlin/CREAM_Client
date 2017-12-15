# CREAM_Server
CREAM Server - a multithreaded concurrent server meant for online storage cache of key-value pairs

##Implementation
This is a server implemented in C that essentially acts as a cache that stores key-value pairs. The server uses multiple threads
for better performance. A queue stores all the jobs for that needs to be performed by the threads and the information is stored 
on a hashmap. The map uses an open addressing system to deal with collisions and a very simple hash algorithm.

##Usage
Currently, running the CREAM server requires 3 arguments:
`bin/cream NUM_THREADS PORT_NUM CAPACITY`
NUM_THREADS - number of threads
PORT_NUM - the port the server will listen on
MAP_CAPACITY - maximum capacity of the hashmap

##Client Commands
`PUT key value - stores a key-value pair`
`GET key - retrieves the value if the key has been stored`
`DELETE key - deletes the entry at the given key`
`CLEAR - empties the map`
