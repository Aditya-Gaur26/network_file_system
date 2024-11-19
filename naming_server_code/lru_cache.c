#include "headers.h"
// Hash function

#define PRIME_BASE 13
static unsigned int hash(const char *key, int size) {
    int sum = 0;
    int count = 1;
    for(int i = 0; i < strlen(key); i ++) {
        sum = (sum + (count * key[i] % size) % size) % size; 
        count = (count * PRIME_BASE) % size;
    }
    return (unsigned int)sum % size;
}

// Create a new node
static CacheNode* create_node(const char *key, TreeNode* value) {
    CacheNode* node = (CacheNode*)malloc(sizeof(CacheNode));
    strcpy(node->key, key);
    node->value = value;
    node->prev = NULL;
    node->next = NULL;
    return node;
}

// Initialize LRU cache
LRUCache* lru_cache_create(int capacity) {
    LRUCache* cache = (LRUCache*)malloc(sizeof(LRUCache));
    // fprintf(stderr, "initialized cache!\n");
    cache->capacity = capacity;
    cache->size = 0;
    cache->hash_size = capacity * 5;  // 5 times size to reduce collisions
    
    // Initialize hash table
    cache->hash_table = (HashEntryCache**)calloc(cache->hash_size, sizeof(HashEntryCache*));
    
    // Create dummy head and tail
    cache->head = create_node("", NULL);
    cache->tail = create_node("", NULL);
    cache->head->next = cache->tail;
    cache->tail->prev = cache->head;
    
    return cache;
}

// Add node right after head
static void add_node(LRUCache* cache, CacheNode* node) {
    node->prev = cache->head;
    node->next = cache->head->next;
    
    cache->head->next->prev = node;
    cache->head->next = node;
}

// Remove a node from the list
static void remove_node(CacheNode* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

// Move node to front (mark as most recently used)
static void move_to_front(LRUCache* cache, CacheNode* node) {
    remove_node(node);
    add_node(cache, node);
}

// Add or update hash table entry
static void hash_put(LRUCache* cache, const char* key, CacheNode* node) {
    unsigned int index = hash(key, cache->hash_size);
    
    // Check if key already exists
    HashEntryCache* entry = cache->hash_table[index];
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            entry->node = node;
            return;
        }
        entry = entry->next;
    }
    
    // Create new entry
    HashEntryCache* new_entry = (HashEntryCache*)malloc(sizeof(HashEntryCache));
    strcpy(new_entry->key, key);
    new_entry->node = node;
    new_entry->next = cache->hash_table[index];
    cache->hash_table[index] = new_entry;
}

// Get node from hash table
static CacheNode* hash_get(LRUCache* cache, const char *key) {
    unsigned int index = hash(key, cache->hash_size);
    HashEntryCache* entry = cache->hash_table[index];
    
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            return entry->node;
        }
        entry = entry->next;
    }
    return NULL;
}

// Remove entry from hash table
static void hash_remove(LRUCache* cache, const char *key) {
    unsigned int index = hash(key, cache->hash_size);
    HashEntryCache* entry = cache->hash_table[index];
    HashEntryCache* prev = NULL;
    
    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            if (prev) {
                prev->next = entry->next;
            } else {
                cache->hash_table[index] = entry->next;
            }
            free(entry);
            return;
        }
        prev = entry;
        entry = entry->next;
    }
}

// Get value from cache
TreeNode* lru_cache_get(LRUCache* cache, const char *key) {
    CacheNode* node = hash_get(cache, key);
    if (node == NULL) {
        return NULL;
    }
    
    move_to_front(cache, node);
    return node->value;
}

// Put value in cache
void lru_cache_put(LRUCache* cache, const char *key, TreeNode* value) {
    CacheNode* node = hash_get(cache, key);
    
    if (node != NULL) {
        // Update existing node
        node->value = value;
        move_to_front(cache, node);
    } else {
        // Create new node
        CacheNode* new_node = create_node(key, value);
        hash_put(cache, key, new_node);
        add_node(cache, new_node);
        cache->size++;
        
        // Remove least recently used if capacity exceeded
        if (cache->size > cache->capacity) {
            CacheNode* lru = cache->tail->prev;
            remove_node(lru);
            hash_remove(cache, lru->key);
            free(lru);
            cache->size--;
        }
    }
}

// Free all memory used by cache
void lru_cache_free(LRUCache* cache) {
    // Free all nodes
    CacheNode* current = cache->head->next;
    while (current != cache->tail) {
        CacheNode* next = current->next;
        free(current);
        current = next;
    }
    
    // Free dummy nodes
    free(cache->head);
    free(cache->tail);
    
    // Free hash table entries
    for (int i = 0; i < cache->hash_size; i++) {
        HashEntryCache* entry = cache->hash_table[i];
        while (entry != NULL) {
            HashEntryCache* next = entry->next;
            free(entry);
            entry = next;
        }
    }
    
    // Free hash table and cache structure
    free(cache->hash_table);
    free(cache);
}