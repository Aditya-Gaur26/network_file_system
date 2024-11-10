#ifndef __HEADERS_NS__
#define __HEADERS_NS__
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <json-c/json.h>
#include <time.h>

#include <ifaddrs.h>
#include <netdb.h>

#define MAX_STORAGE_SERVERS 100
#define MAX_PATHS_PER_SERVER 1000
#define MAX_PATH_LENGTH 256
#define MAX_BUFFER_SIZE 4096
#define MAX_CLIENTS 50
#define MAX_FILENAME_LENGTH 1024
#define MAX_CHILDREN 256

typedef struct TreeNode {
    char name[MAX_FILENAME_LENGTH];
    int is_directory;
    struct TreeNode* parent;
    struct TreeNode** children;
    int child_count;
    int max_children;
    char** storage_servers;  // Array of storage server IDs that have this file/directory (promotes reduncdancy for backup too)
    int server_count;
} TreeNode;

typedef struct {
    char id[32];
    char ip[16];
    int nm_port;
    int client_port;
    char** accessible_paths;
    int path_count;
    time_t last_heartbeat;
    int active;
} StorageServer;

typedef struct {
    StorageServer* storage_servers[MAX_STORAGE_SERVERS];
    int storage_server_count;
    pthread_mutex_t lock;
    TreeNode* root;
    int server_socket;
} NamingServer;

// Function declarations
void* handle_connection(void* arg);
void initialize_naming_server(NamingServer* nm);
void handle_storage_server_registration(NamingServer* nm, int client_socket, json_object* request);
void handle_client_request(NamingServer* nm, int client_socket, json_object* request);
void send_response(int socket, json_object* response);
json_object* receive_request(int socket);
void cleanup_naming_server(NamingServer* nm);


// Tree Directory Structure
TreeNode* create_tree_node(const char* name, int is_directory);
void add_child_node(TreeNode* parent, TreeNode* child);
TreeNode* find_node(TreeNode* root, const char* path);
void add_server_to_node(TreeNode* node, const char* server_id);
void free_tree_node(TreeNode* node);
char* get_node_path(TreeNode* node);
json_object* tree_to_json(TreeNode* node);
void parse_path_components(const char* path, char** components, int* count);

#endif