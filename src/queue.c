#include <stdio.h>
#include "queue.h"

queue_t *create_queue(void) {
    queue_t* queue = (queue_t*)calloc(1,sizeof(queue_t));
    queue->invalid = 0;
    queue->front = queue->rear = NULL;
    if (queue==NULL ||
        pthread_mutex_init(&(queue->lock),NULL)==-1 ||
        sem_init(&(queue->items),0,0)==-1) {
        return NULL;
    }//return NULL if calloc or sem_init fails
    return queue;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function) {
    self->invalid = 1;
    pthread_mutex_destroy(&(self->lock));
    sem_destroy(&(self->items));
    destroy_function(self);
    return false;
}

bool enqueue(queue_t *self, void *item) {
    pthread_mutex_lock(&(self->lock));
    queue_node_t* back_of_line = (queue_node_t*)malloc(sizeof(queue_node_t));
    back_of_line->item = item;
    back_of_line->next = NULL;
    if (self->front == NULL){
        self->front = back_of_line;
    }
    if (self->rear != NULL){
        self->rear->next = back_of_line;
    }
    self->rear = back_of_line;
    pthread_mutex_unlock(&(self->lock));
    sem_post(&(self->items));
    return true;
}

void *dequeue(queue_t *self) {
    sem_wait(&(self->items));
    pthread_mutex_lock(&(self->lock));
    queue_node_t* front = self->front;
    void* item = front->item;
    if (front == NULL){
        return NULL;
    }
    self->front = self->front->next;
    if (self->front == NULL){
        self->rear = NULL;
    }
    pthread_mutex_unlock(&(self->lock));
    free(front);
    return item;
}