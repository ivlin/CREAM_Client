#include "cream.h"
#include "utils.h"
#include <string.h>
#include <stdio.h>
#include <sys/socket.h>
#include "csapp.h"
#include "debug.h"
#include "queue.h"
//#include "hashmap.h"

queue_t* conn_queue;
hashmap_t* map;

void handle_req(int connfd, request_header_t header, char* key, char* val, hashmap_t* map);

void queue_free_function(void *item) {
    free(item);
}

void *worker_thread(void* args) {
    request_header_t request_header;
    char *key, *val;
    while (1){
        int connfd = *(int*)dequeue(conn_queue);

        rio_t rio;

        Rio_readinitb(&rio, connfd);

        Rio_readnb(&rio, &request_header, sizeof(request_header_t));

        key=malloc(sizeof(char)*(request_header.key_size+1));
        val=malloc(sizeof(char)*(request_header.value_size+1));

        Rio_readlineb(&rio, key, sizeof(char)*(request_header.key_size+1));
        Rio_readlineb(&rio, val, sizeof(char)*(request_header.value_size+1));

        /*
        debug ("Request headers: \n");
        debug("Code: %d - Keysize: %d - Valuesize: %d\n", request_header.request_code,
            request_header.key_size, request_header.value_size);
        debug("Key: %s - Value: %s\n", key, val);
        */
        handle_req(connfd,request_header,key,val,map);

        Close(connfd);
    }
    return NULL;
}

void map_free_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}

int main(int argc, char *argv[]) {
    if (argc<2 || strcmp(argv[1],"-h")==0){
        exit(1);
    }
    else if (strcmp(argv[1],"-h")==0){
        printf("./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n-h                 Displays this help menu and returns EXIT_SUCCESS.\nNUM_WORKERS        The number of worker threads used to service requests.\nPORT_NUMBER        Port number to listen on for incoming connections.\nMAX_ENTRIES        The maximum number of entries that can be stored in creamW's underlying data store.\n");
        exit(0);
    }

    Signal(SIGPIPE, SIG_IGN);

    conn_queue = create_queue();

    pthread_t thread_ids[atoi(argv[1])];
    for (int ind=0; ind<atoi(argv[1]); ind++){
        if (pthread_create(&thread_ids[ind], NULL, worker_thread, NULL)!=0){
            exit(EXIT_FAILURE);
        }
    }
    //bind and listen from socket
    int listenfd, connfd;
    char hostname[MAXLINE], port[MAXLINE];
    socklen_t clientlen;
    struct sockaddr_storage clientaddr;

    listenfd = Open_listenfd(argv[2]);

    map = create_map(atoi(argv[3]), jenkins_one_at_a_time_hash, map_free_function);

    //accept connections
    while (1) {
        clientlen=sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, &clientlen);
        Getnameinfo((SA *) &clientaddr, clientlen, hostname, MAXLINE, port, MAXLINE, 0);
        int* connfd_ptr = malloc(sizeof(int));
        connfd_ptr=memcpy(connfd_ptr,&connfd,sizeof(int));
        enqueue(conn_queue, connfd_ptr);
    }

    invalidate_queue(conn_queue, queue_free_function);
    exit(0);
}

void handle_req(int connfd, request_header_t header, char* key, char* val, hashmap_t* map){
    if (header.request_code==PUT){
        debug("PUT request received\n");
        bool success=put(map, MAP_KEY(key, header.key_size), MAP_VAL(val, header.value_size), true);
        if (success){
            response_header_t rsp = {OK, 0};
            Rio_writen(connfd, &rsp, sizeof(rsp));
        }
        else{
            response_header_t rsp = {BAD_REQUEST,0};
            Rio_writen(connfd, &rsp, sizeof(rsp));
        }
    }
    else if (header.request_code==GET){
        debug("GET request received\n");
        map_val_t val = get(map, MAP_KEY(key,header.key_size));
        if  (val.val_base!=NULL){
            response_header_t rsp = {OK, val.val_len};
            Rio_writen(connfd, &rsp, sizeof(rsp));
            Rio_writen(connfd, val.val_base, val.val_len);
        }
        else{
            response_header_t rsp = {NOT_FOUND, 0};
            Rio_writen(connfd, &rsp, sizeof(rsp));
        }
    }
    else if (header.request_code==EVICT){
        debug("EVICT request received\n");
        map_node_t node = delete(map, MAP_KEY(key,header.key_size));
        response_header_t rsp = {OK, node.val.val_len};
        Rio_writen(connfd, &rsp, sizeof(rsp));
        Rio_writen(connfd, node.val.val_base, node.val.val_len);
    }
    else if (header.request_code==CLEAR){
        debug("CLEAR request received\n");
        clear_map(map);
        response_header_t rsp = {OK, 0};
        Rio_writen(connfd, &rsp, sizeof(rsp));
    }
    else{
        debug("UNSUPPORTED request received\n");
        response_header_t rsp = {UNSUPPORTED, 0};
        Rio_writen(connfd, &rsp, sizeof(rsp));
    }
}