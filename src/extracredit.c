#include "utils.h"
#include <string.h>
#include <stdio.h>
#define MAP_KEY(base, len) (map_key_t) {.key_base = base, .key_len = len}
#define MAP_VAL(base, len) (map_val_t) {.val_base = base, .val_len = len}
#define MAP_NODE(key_arg, val_arg, tombstone_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg}
#include <time.h>
#include <errno.h>
int errno;

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    if (capacity<0 || hash_function==NULL || destroy_function==NULL){
        errno=EINVAL;
        return NULL;
    }
    hashmap_t* hashmap = (hashmap_t*)calloc(1,sizeof(hashmap_t));
    if (hashmap!=NULL){
        hashmap->hash_function=hash_function;
        hashmap->destroy_function=destroy_function;
        hashmap->capacity=capacity;
        hashmap->nodes=calloc(capacity,sizeof(map_node_t));
        hashmap->size=0;
        hashmap->num_readers=0;
        hashmap->cur_time=1;
        pthread_mutex_init(&(hashmap->write_lock),NULL);
        pthread_mutex_init(&(hashmap->fields_lock),NULL);
        hashmap->invalid=false;
    }
    return hashmap;
}

bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    int index, original_index;
    index = original_index = get_index(self,key);
    map_node_t* node_at_index;
    pthread_mutex_lock(&(self->write_lock));
    time_t now;
    do {
        node_at_index = &(self->nodes[index]);
        time(&now);
        if (difftime(now, node_at_index->birth_time) > TTL){
            node_at_index->tombstone=true;
        }
        index=(index+1)%self->capacity;
    } while (node_at_index->key.key_base!=0 && !(node_at_index->tombstone) && index!=original_index &&
        !(memcmp(node_at_index->key.key_base, key.key_base, key.key_len)==0 && key.key_len==node_at_index->key.key_len));
    if(node_at_index->key.key_base!=0 && !(node_at_index->tombstone) && index!=original_index){
        printf("%d, %d, %d, %d\n",node_at_index->key.key_base!=NULL , !(node_at_index->tombstone) , index!=original_index, 100);
    }
    int unique=1;
    if (node_at_index->key.key_base==0 || node_at_index->tombstone ||
        ((unique=memcmp(node_at_index->key.key_base, key.key_base, key.key_len))==0 && key.key_len==node_at_index->key.key_len)){
        if (unique){
            pthread_mutex_lock(&(self->fields_lock));
            self->size++;
            pthread_mutex_unlock(&(self->fields_lock));
        }
        *node_at_index = MAP_NODE(key, val, false);
        node_at_index->birth_iteration = self->cur_time;
        time(&(node_at_index->birth_time));
    }
    else if (index==original_index && force){
        int oldest_ind=0;
        for (int cur_ind=0; cur_ind<self->capacity; cur_ind++){
            if (self->nodes[cur_ind].key.key_base !=0 && self->nodes[cur_ind].birth_iteration < self->nodes[oldest_ind].birth_iteration){
                oldest_ind=cur_ind;
            }
        }
        self->nodes[oldest_ind] = MAP_NODE(key, val, false);
        node_at_index->birth_iteration = self->cur_time;
        time(&(node_at_index->birth_time));
    }
    else{
        if (index==original_index && !force){
            errno=ENOMEM;
        }
        pthread_mutex_unlock(&(self->write_lock));
        return false;
    }
    self->cur_time++;
    pthread_mutex_unlock(&(self->write_lock));
    return true;
}

map_val_t get(hashmap_t *self, map_key_t key) {
    if (self==NULL || self->invalid || key.key_base==NULL){
        errno=EINVAL;
        return MAP_VAL(NULL, 0);
    }
    int index, original_index;
    index = original_index = get_index(self,key);
    if (self->num_readers==0){
        pthread_mutex_lock(&(self->write_lock));
    }
    pthread_mutex_lock(&(self->fields_lock));
    self->num_readers++;
    pthread_mutex_unlock(&(self->fields_lock));
    map_node_t* node_at_index = &(self->nodes[index]);
    time_t now;
    do{
        node_at_index = &(self->nodes[index]);
        time(&now);
        if (difftime(now, node_at_index->birth_time) > TTL){
            node_at_index->tombstone=true;
        }
        index=(index+1)%self->capacity;
    }
    while (node_at_index->key.key_base!=0 && index!=original_index &&
        !(memcmp(node_at_index->key.key_base, key.key_base, key.key_len)==0 && key.key_len==node_at_index->key.key_len));

    map_val_t get_val = node_at_index->key.key_base!=0 && !node_at_index->tombstone && (memcmp(node_at_index->key.key_base, key.key_base, key.key_len)==0 && key.key_len==node_at_index->key.key_len) ? node_at_index->val : MAP_VAL(NULL, 0);
    pthread_mutex_lock(&(self->fields_lock));
    self->num_readers--;
    pthread_mutex_unlock(&(self->fields_lock));
    if (self->num_readers==0){
        pthread_mutex_unlock(&(self->write_lock));
    }
    return get_val;
}

map_node_t delete(hashmap_t *self, map_key_t key) {
    if (self==NULL || self->invalid || key.key_base==NULL){
        errno=EINVAL;
        return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL,0), false);
    }
    if (self->num_readers==0){
        pthread_mutex_lock(&(self->write_lock));
    }
    int index, original_index;
    index=original_index = get_index(self,key);
    map_node_t* node_at_index = &(self->nodes[index]);
    while (node_at_index->key.key_base!=0 && index!=original_index &&
        !(memcmp(node_at_index->key.key_base, key.key_base, key.key_len)==0 && key.key_len==node_at_index->key.key_len)){
        node_at_index = &(self->nodes[index]);
        index=(index+1)%self->capacity;
    }
    map_node_t removed_node = MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    if (node_at_index->key.key_base==0 || (memcmp(node_at_index->key.key_base, key.key_base, key.key_len)==0 && key.key_len==node_at_index->key.key_len)){
        node_at_index->tombstone = true;
        removed_node = *node_at_index;
    }
    pthread_mutex_lock(&(self->fields_lock));
    self->size--;
    pthread_mutex_unlock(&(self->fields_lock));
    if (self->num_readers==0){
        pthread_mutex_unlock(&(self->write_lock));
    }
    return removed_node;
}

bool clear_map(hashmap_t *self) {
    if (self==NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&(self->write_lock));
    for (int ind=0; ind<self->capacity; ind++){
        self->destroy_function(self->nodes[ind].key, self->nodes[ind].val);
        self->nodes[ind] = MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
    }
    pthread_mutex_unlock(&(self->write_lock));
    return true;
}

bool invalidate_map(hashmap_t *self) {
    if (self==NULL){
        errno = EINVAL;
        return false;
    }
    pthread_mutex_lock(&(self->fields_lock));
    self->invalid=true;
    clear_map(self);
    free(self->nodes);
    pthread_mutex_unlock(&(self->fields_lock));
    return true;
}
