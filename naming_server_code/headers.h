#ifndef __HEADERS_NS__
#define __HEADERS_NS__

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <json-c/json.h>
#include <time.h>
#include <limits.h>
#include <string.h>

#include <ifaddrs.h>
#include <netdb.h>
#include <fcntl.h>

#define MAX_STORAGE_SERVERS 100
#define MAX_PATHS_PER_SERVER 1000
#define MAX_PATH_LENGTH 1040
#define MAX_BUFFER_SIZE 4096
#define MAX_CLIENTS 50
#define MAX_FILENAME_LENGTH 1024
#define MAX_CHILDREN 256

#define MAX_BACKUP_OF 10

typedef struct pair{
    int sid;   //jonsa backup krra h
    int for_backup;
    int sid_of_backup; //jiska backup h
} pair;

typedef struct TreeNode {
    char name[MAX_FILENAME_LENGTH];
    int is_directory;
    struct TreeNode* parent;
    struct TreeNode** children;
    int child_count;
    int max_children;
    pair* storage_servers;  // Array of storage server IDs that have this file/directory (promotes reduncdancy for backup too)

    int server_count;
    int is_written_to;

    bool only_read;
} TreeNode;



typedef struct Client Client;
typedef struct StorageServer StorageServer;

typedef struct Request {
    int reqid;
    Client *client_associated;
    StorageServer *server_associated;
    TreeNode *corr_node;
}Request;

struct Client {
    int id;
    char ip[16];
    int port;

    int sock;
    int pending_count;
    Request pending_requests[256];
};


typedef struct StorageServer{
    int id;
    char ip[16];
    int nm_port;
    int client_port;
    
    char* accessible_paths[MAX_PATHS_PER_SERVER];
    int types[MAX_PATHS_PER_SERVER];
    int types_backup_paths[MAX_PATHS_PER_SERVER];

    int pending_backup_cnt;
    char *pending_backup_paths[MAX_PATHS_PER_SERVER];
    int path_count;

    time_t last_heartbeat;
    int health_status_retries;
    int active;

    int sock;
    int current_load;

    Request processing_requests[256];

    int backup_load;

    struct StorageServer *backup1;
    struct StorageServer *backup2;
    struct StorageServer **backup_of;
} StorageServer;

typedef struct {
    StorageServer* storage_servers[MAX_STORAGE_SERVERS];
    Client* clients_in_queue[MAX_CLIENTS];
    
    int storage_server_count;
    int client_count;
    pthread_mutex_t lock;
    TreeNode* root;
    int server_socket;

    pthread_mutex_t replication_lock;
} NamingServer;

typedef struct CacheNode {
    char key[MAX_PATH_LENGTH];
    TreeNode* value;      // ss_id to redirect to
    struct CacheNode* prev;
    struct CacheNode* next;
} CacheNode;

// Hash table entry
typedef struct HashEntryCache {
    char *key;
    CacheNode* node;
    struct HashEntryCache* next;  // For collision handling
} HashEntryCache;

// LRU Cache structure
typedef struct {
    int capacity;
    int size;
    CacheNode* head;      // Dummy head
    CacheNode* tail;      // Dummy tail
    HashEntryCache** hash_table;
    int hash_size;
} LRUCache;


// Function declarations
void* handle_connection(void* arg);
void initialize_naming_server();
int handle_storage_server_registration(int client_socket, json_object* request);
int handle_client_connection(int client_socket, json_object* request);
int send_message(int socket, json_object* msg);
json_object* receive_request(int socket);
void cleanup_naming_server();


// Tree Directory Structure
TreeNode* create_tree_node(const char* name, int is_directory);
void add_child_node(TreeNode* parent, TreeNode* child);
TreeNode* find_node(TreeNode* root, const char* path);
void add_server_to_node(TreeNode* node, const int server_id, int for_backup, int sid_for_backup);
void free_tree_node(TreeNode* node);
char* get_node_path(TreeNode* node);
json_object* tree_to_json(TreeNode* node);
void parse_path_components(const char* path, char** components, int* count);

int create_socket_to_send_to(const char* ip_address, int port);

static unsigned int hash(const char *key, int size) ;
static CacheNode* create_node(const char *key, TreeNode* value);
LRUCache* lru_cache_create(int capacity);
static void add_node(LRUCache* cache, CacheNode* node) ;
static void remove_node(CacheNode* node) ;
static void move_to_front(LRUCache* cache, CacheNode* node) ;
static void hash_put(LRUCache* cache, const char *key, CacheNode* node);
static CacheNode* hash_get(LRUCache* cache, const char *key);
static void hash_remove(LRUCache* cache, const char *key);
TreeNode* lru_cache_get(LRUCache* cache, const char *key);
void lru_cache_put(LRUCache* cache, const char *key, TreeNode* value);
void lru_cache_free(LRUCache* cache);

int handle_path_replication(int primary_ss_id);
int handle_client_request(Client *cl, int client_socket, json_object *request);

#endif


// note that the tree node has the server nodes indexed from 1 
// while the nm->storage_servers is indexed from 0.


// handle the while loop of the request coming to me.
// check for errors due to the index issues.

// code_temp
