#include "headers.h"

NamingServer *nm;
LRUCache* cache;

typedef struct {
    char* path;
    int depth;
} PathDepth;

// Helper function to calculate path depth
int calculate_path_depth(const char* path) {
    if (!path) return 0;
    if (strcmp(path, "/") == 0) return 0;
    
    int depth = 0;
    for (const char* p = path; *p; p++) {
        if (*p == '/') depth++;
    }
    return depth;
}

// Comparison function for qsort
int compare_paths(const void* a, const void* b) {
    const PathDepth* path_a = (const PathDepth*)a;
    const PathDepth* path_b = (const PathDepth*)b;
    // Sort by depth in descending order (deeper paths first)
    return path_b->depth - path_a->depth;
}

// --------------------------------------------------------------

TreeNode* create_tree_node(const char* name, int is_directory) {
    TreeNode* node = (TreeNode*)malloc(sizeof(TreeNode));
    strncpy(node->name, name, MAX_FILENAME_LENGTH - 1);
    node->name[MAX_FILENAME_LENGTH - 1] = '\0';
    node->is_directory = is_directory;
    node->parent = NULL;
    node->max_children = MAX_CHILDREN;
    node->children = (TreeNode**)malloc(sizeof(TreeNode*) * node->max_children);
    node->child_count = 0;
    node->storage_servers = (int*)malloc(sizeof(int) * MAX_STORAGE_SERVERS);
    node->server_count = 0;
    return node;
}

// Add a child node to a parent node
void add_child_node(TreeNode* parent, TreeNode* child) {
    if (parent->child_count < parent->max_children) {
        parent->children[parent->child_count++] = child;
        child->parent = parent;
    }
}

// Split path into components
void parse_path_components(const char* path, char** components, int* count) {
    char* path_copy = strdup(path);
    char* token = strtok(path_copy, "/");
    *count = 0;
    
    while (token != NULL && *count < MAX_PATH_LENGTH) {
        components[*count] = strdup(token);
        (*count)++;
        token = strtok(NULL, "/");
    }
    
    free(path_copy);
}

// Find a node in the tree given a path
TreeNode* find_node(TreeNode* root, const char* path) {
    if (strcmp(path, "/") == 0) return root;

    char* components[MAX_PATH_LENGTH];
    int component_count;
    parse_path_components(path, components, &component_count);
    
    TreeNode* current = root;
    for (int i = 0; i < component_count; i++) {
        int found = 0;
        for (int j = 0; j < current->child_count; j++) {
            if (strcmp(current->children[j]->name, components[i]) == 0) {
                current = current->children[j];
                found = 1;
                break;
            }
        }

        if (!found) {
            // Clean up components
            for (int k = 0; k < component_count; k++) {
                free(components[k]);
            }
            return NULL;
        }
    }
    // Clean up components
    for (int i = 0; i < component_count; i++) {
        free(components[i]);
    }
    
    return current;
}

// Add a storage server ID to a node
void add_server_to_node(TreeNode* node, const int server_id) {
    for (int i = 0; i < node->server_count; i++) {
        if (node->storage_servers[i] == server_id) {
            printf("Already registered\n");
            return;  // Server already registered for this node
        }
    }
    if (node->server_count < MAX_STORAGE_SERVERS) {
        node->storage_servers[node->server_count] = server_id;
        node->server_count++;
    }
    
}

void cleanup_empty_subtree(TreeNode* node) {
    if (!node) return;
    
    // First recursively clean up all children
    if (node->is_directory && node->children) {
        for (int i = node->child_count - 1; i >= 0; i--) {
            cleanup_empty_subtree(node->children[i]);
        }
    }
    
    // If this node has no storage servers, remove it from its parent
    if (node->server_count == 0) {
        if (node->parent) {
            // Find and remove this node from parent's children array
            for (int i = 0; i < node->parent->child_count; i++) {
                if (node->parent->children[i] == node) {
                    // Shift remaining children left
                    for (int j = i; j < node->parent->child_count - 1; j++) {
                        node->parent->children[j] = node->parent->children[j + 1];
                    }
                    node->parent->child_count--;
                    break;
                }
            }
        }
        
        // Free memory
        if (node->children) {
            free(node->children);
        }
        if (node->storage_servers) {
            free(node->storage_servers);
        }
        free(node);
    }
}

void remove_server_from_node(TreeNode* node, const int server_id) {
    for (int i = 0; i < node->server_count; i++) {
        if (node->storage_servers[i] == server_id) {
            node->storage_servers[i] = -1;
            for(int j = i + 1; j < node->server_count; j ++) {
                node->storage_servers[j - 1] = node->storage_servers[j];
            }
            node->server_count --;
            return;  
        }
    }
    if (node->server_count < MAX_STORAGE_SERVERS) {
        node->storage_servers[node->server_count] = server_id;
        node->server_count++;
    }
    
}


// Get full path of a node
char* get_node_path(TreeNode* node) {
    char* path = (char*)malloc(MAX_PATH_LENGTH);
    path[0] = '\0';
    
    TreeNode* current = node;
    char** components = (char**)malloc(sizeof(char*) * MAX_PATH_LENGTH);
    int count = 0;
    
    while (current->parent != NULL) {
        components[count++] = current->name;
        current = current->parent;
    }
    
    strcat(path, "/");
    for (int i = count - 1; i >= 0; i--) {
        strcat(path, components[i]);
        if (i > 0) strcat(path, "/");
    }
    
    free(components);
    return path;
}

void build_path_list(TreeNode* node, const char* current_path, json_object* paths_array) {
    if (!node || !current_path || !paths_array) return;
    
    char* full_path = (char*)malloc(MAX_PATH_LENGTH);
    if (!full_path) return;
    
    // Construct the full path
    if (strcmp(current_path, "/") == 0) {
        if (strcmp(node->name, "/") == 0) {
            snprintf(full_path, MAX_PATH_LENGTH, "/");
        } else {
            snprintf(full_path, MAX_PATH_LENGTH, "/%s", node->name);
        }
    } else {
        snprintf(full_path, MAX_PATH_LENGTH, "%s/%s", current_path, node->name);
    }
    
    // Add path to array if it's not the root
    if (strcmp(full_path, "/") != 0) {
        // Add trailing slash for directories
        if (node->is_directory) {
            size_t len = strlen(full_path);
            if (len + 2 <= MAX_PATH_LENGTH) {
                strcat(full_path, "/");
            }
        }
        json_object_array_add(paths_array, json_object_new_string(full_path));
    }
    
    // Recursively process children
    // printf("%s %d %d\n", node->name, node->children, node->is_directory);
    if (node->is_directory && node->children) {
        // printf("%s\n", node->name);
        for (int i = 0; i < node->child_count; i++) {
            build_path_list(node->children[i], full_path, paths_array);
        }
    }
    
    free(full_path);
}


void free_tree_node(TreeNode *node) {
    free(node);
}

// tree directory structure functions end here.


void check_storage_server_status(void *arg) {
    int storage_server_id = *((int *) arg);

    json_object* status_check = json_object_new_object();
    json_object_object_add(status_check, "status_check", json_object_new_string("give_report"));
    
    int res = send_message(nm->storage_servers[storage_server_id]->sock, status_check);
    if(res == -1) {
        nm->storage_servers[storage_server_id]->health_status_retries --;
        if(nm->storage_servers[storage_server_id]->health_status_retries == 0) {
            free(nm->storage_servers[storage_server_id]);
            nm->storage_servers[storage_server_id] = NULL;
        }
    }else {
        nm->storage_servers[storage_server_id]->health_status_retries = 3;
    }
    sleep(30);
}


// Initialize the Naming Server
void initialize_naming_server() {
    nm = (NamingServer *) malloc(sizeof(NamingServer));
    nm->storage_server_count = 0;
    nm->client_count = 0;

    pthread_mutex_init(&nm->lock, NULL);
    nm->root = create_tree_node("/", 1);
    memset(nm->storage_servers, 0, sizeof(nm->storage_servers));
    for(int i = 0; i < MAX_STORAGE_SERVERS; i ++) {
        nm->storage_servers[i] = NULL;
    }

    memset(nm->clients_in_queue, 0, sizeof(nm->clients_in_queue));
    for(int i = 0; i < MAX_CLIENTS; i ++) {
        nm->clients_in_queue[i] = NULL;
    }

    cache = lru_cache_create(128); 
}

char* get_local_ip() {
    struct ifaddrs *ifaddr, *ifa;
    static char ip[INET_ADDRSTRLEN];
    int found = 0;

    // Get list of interfaces
    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        return NULL;
    }

    // Iterate through interfaces
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        // Only interested in IPv4 addresses
        if (ifa->ifa_addr->sa_family == AF_INET) {
            struct sockaddr_in *addr = (struct sockaddr_in *)ifa->ifa_addr;
            
            // Skip loopback interface
            if (strcmp(ifa->ifa_name, "lo") == 0)
                continue;

            // Convert IP to string
            inet_ntop(AF_INET, &(addr->sin_addr), ip, INET_ADDRSTRLEN);
            
            // Skip local and link-local addresses
            if (strncmp(ip, "127.", 4) != 0 && strncmp(ip, "169.254.", 8) != 0) {
                found = 1;
                break;
            }
        }
    }
    freeifaddrs(ifaddr);
    return found ? ip : NULL;
}

// Main thread handler for each connection
void* handle_connection(void* arg) {
    int client_socket = *((int*)arg);
    free(arg);

    json_object* request = receive_request(client_socket);
    if (request == NULL) {
        printf("Unsupported Format! Closing socket!\n");
        close(client_socket);
        return NULL;
    }

    json_object* type_obj;
    if (json_object_object_get_ex(request, "type", &type_obj)) {
        const char* type = json_object_get_string(type_obj);

        fprintf(stderr, "%s\n", type);        
        if (strcmp(type, "storage_server_register") == 0) {
            int sid = -1;
            if((sid = handle_storage_server_registration(client_socket, request)) != -1) { 
                while(nm->storage_servers[sid] != NULL && nm->storage_servers[sid]->active == 1) {
                    json_object* response;
                    if((response = receive_request(nm->storage_servers[sid]->sock)) != NULL) {
                        char* type = json_object_get_string(response);
                        if(strcmp(type, "naming_server_related") == 0) {
                            json_object* status_obj;
                            if (json_object_object_get_ex(response, "status", &status_obj)) {
                                const char* status = json_object_get_string(status_obj);
                                printf("Storage Server response: %s\n", status);

                                json_object* message_to_client = json_object_new_object(); 
                                if(strcmp(status, "success") == 0) {
                                    json_object_object_add(message_to_client, "status", json_object_new_string("success"));
                                }else if(strcmp(status, "error") == 0)  {
                                    json_object_object_add(message_to_client, "status", json_object_new_string("error"));
                                }
                        
                            }
                        }else if(strcmp(type, "client_related") == 0) {
                            json_object *status_obj, *client_id_obj, *client_port_obj, *client_ip_obj, *client_request_id_obj, *error_msg;
                            json_object_object_get_ex(response, "client_id", &client_id_obj);
                            json_object_object_get_ex(response, "client_port", &client_port_obj);
                            json_object_object_get_ex(response, "client_ip", &client_ip_obj);
                            json_object_object_get_ex(response, "client_request_id", &client_request_id_obj);

                            int client_id = json_object_get_int(client_id_obj);
                            int client_port = json_object_get_int(client_port_obj);
                            const char* client_ip = json_object_get_string(client_ip_obj);
                            int client_request_id = json_object_get_int(client_request_id_obj);

                            if (json_object_object_get_ex(response, "status", &status_obj)) {
                                const char* status = json_object_get_string(status_obj);
                                printf("Storage Server response: %s\n", status);

                                json_object* message_to_client = json_object_new_object(); 
                                if(strcmp(status, "success") == 0) {
                                    json_object *object_to_send_to_client = json_object_new_object();
                                    json_object_object_add(object_to_send_to_client, "request_id", json_object_new_int(client_request_id));
                                    json_object_object_add(object_to_send_to_client, "status", json_object_new_string("success"));

                                }else if(strcmp(status, "error") == 0)  {
                                    json_object_object_get_ex(response, "status", &error_msg);
                                    const char* error_msg1 = json_object_get_string(error_msg);
                                    // json_object_object_add(message_to_client, "status", json_object_new_string("error"));

                                    json_object *object_to_send_to_client = json_object_new_object();
                                    json_object_object_add(object_to_send_to_client, "request_id", json_object_new_int(client_request_id));
                                    json_object_object_add(object_to_send_to_client, "status", json_object_new_string("error"));
                                    json_object_object_add(object_to_send_to_client, "message", json_object_new_string(error_msg1));
                                }
                        
                            }
                        }
                    }else {
                        fprintf(stderr, "band!\n");
                        
                        for (int i = 0; i < nm->storage_servers[sid]->path_count; i++) {
                            TreeNode* node = find_node(nm->root, nm->storage_servers[sid]->accessible_paths[i]);
                            if (node) {
                                remove_server_from_node(node, sid);
                            }
                        }
                        
                        // Create array of PathDepth structures
                        PathDepth* path_depths = (PathDepth*)malloc(sizeof(PathDepth) * nm->storage_servers[sid]->path_count);
                        if (!path_depths) return NULL;
                        
                        // Fill the array with paths and their depths
                        for (int i = 0; i < nm->storage_servers[sid]->path_count; i++) {
                            path_depths[i].path = nm->storage_servers[sid]->accessible_paths[i];
                            path_depths[i].depth = calculate_path_depth(nm->storage_servers[sid]->accessible_paths[i]);
                        }
                        
                        // Sort paths by depth in descending order
                        qsort(path_depths, nm->storage_servers[sid]->path_count, sizeof(PathDepth), compare_paths);
                        
                        // Second pass: clean up empty subtrees (now in correct order)
                        for (int i = 0; i < nm->storage_servers[sid]->path_count; i++) {
                            TreeNode* node = find_node(nm->root, path_depths[i].path);
                            if (node) {
                                cleanup_empty_subtree(node);
                            }
                        }
                        
                        // Clean up
                        free(path_depths);

                        close(nm->storage_servers[sid]->sock);
                        free(nm->storage_servers[sid]); 
                        nm->storage_servers[sid] = NULL;
                        break;
                    }
                }
            }
        } else
        if (strcmp(type, "client_request") == 0) {
            int cid = -1;
            cid = handle_client_connection(client_socket, request);
            cid --;
        }
    }
    // close(client_socket);
    return NULL;
}

// Handle Storage Server registration
int handle_storage_server_registration(int client_socket, json_object* request) {
    pthread_mutex_lock(&nm->lock);

    if (nm->storage_server_count >= MAX_STORAGE_SERVERS) {
        json_object* response = json_object_new_object();
        json_object_object_add(response, "status", json_object_new_string("error"));
        json_object_object_add(response, "message", json_object_new_string("Maximum storage servers reached"));
        send_message(client_socket, response);
        json_object_put(response);
        pthread_mutex_unlock(&nm->lock);
        return -1;
    }

    // Create new storage server entry

    // Get IP and ports from request
    json_object* ip_obj, *nm_port_obj, *client_port_obj, *paths_obj;
    json_object_object_get_ex(request, "ip", &ip_obj);
    json_object_object_get_ex(request, "nm_port", &nm_port_obj);
    json_object_object_get_ex(request, "client_port", &client_port_obj);
    json_object_object_get_ex(request, "accessible_paths", &paths_obj);

    StorageServer* ss;
    bool is_found_in_queue = false;
    for(int i = 0; i < nm->storage_server_count; i ++) {
        if(nm->storage_servers[i] == NULL) continue;
        if(strcmp(nm->storage_servers[i]->ip, json_object_get_string(ip_obj)) == 0 && nm->storage_servers[i]->nm_port == json_object_get_int(nm_port_obj) && nm->storage_servers[i]->active == 1) {
            ss = nm->storage_servers[i];
            is_found_in_queue = true;
            break;
        }
    }

    if(!is_found_in_queue) {
        ss = (StorageServer*)malloc(sizeof(StorageServer));
    
        const char* ip_addr = json_object_get_string(ip_obj);
        strncpy(ss->ip, ip_addr, strlen(ip_addr));
        ss->ip[strlen(ip_addr)] = '\0';
        ss->nm_port = json_object_get_int(nm_port_obj);
        ss->client_port = json_object_get_int(client_port_obj);
        ss->sock = client_socket;
        ss->path_count = 0;

        ss->active = 1;
        ss->last_heartbeat = time(NULL);
        ss->health_status_retries = 3;

        bool is_found = false;
        for(int i = 0; i < nm->storage_server_count; i ++) {
            if(nm->storage_servers[i] == NULL) {
                ss->id = i + 1;
                nm->storage_servers[i] = ss;
                is_found = true;
                break;
            }  
        }
        if(!is_found) {
            ss->id = nm->storage_server_count + 1;
            nm->storage_server_count ++;
            nm->storage_servers[nm->storage_server_count - 1] = ss;
        }
    }else {
        pthread_mutex_unlock(&nm->lock);
        return -1;
    }

    // // work 14/11/2024
    // for(int i = 0; i < nm->storage_server_count; i ++) {
    //     if(strcmp(nm->storage_servers[i]->ip, ss->ip) == 0) {
    //         json_object* response = json_object_new_object();
    //         json_object_object_add(response, "status", json_object_new_string("registration refused!"));
    //         json_object_object_add(response, "code", json_object_new_string("Error 101 : Already registered!\n"));

    //         send_message(client_socket, response);
    //         json_object_put(response);

    //         pthread_mutex_unlock(&nm->lock);
    //     }
    // }

    fprintf(stderr, "Connection from: %d:%s. Its client port is %d.\n", ss->nm_port, ss->ip, ss->client_port);
    // Handle paths
    json_object_object_get_ex(request, "paths", &paths_obj);
    int path_count = json_object_array_length(paths_obj);

    for (int i = 0; i < path_count; i++) {
        const char* path = json_object_get_string(json_object_array_get_idx(paths_obj, i));
        
        // Remove trailing slash if present
        char normalized_path[MAX_PATH_LENGTH];
        strncpy(normalized_path, path, MAX_PATH_LENGTH - 1);
        normalized_path[MAX_PATH_LENGTH - 1] = '\0';
        
        size_t path_len = strlen(normalized_path);
        int is_directory = 0;
        
        // Check if path ends with '/'
        if (path_len > 0 && normalized_path[path_len - 1] == '/') {
            is_directory = 1;
            normalized_path[path_len - 1] = '\0';  // Remove trailing slash
            path_len--;
        }

        ss->path_count ++;
        ss->accessible_paths[ss->path_count - 1] = (char *) malloc(sizeof(char) * MAX_PATH_LENGTH);
        strcpy(ss->accessible_paths[ss->path_count - 1], normalized_path);


        // Split path into components
        char* components[MAX_PATH_LENGTH];
        int component_count;
        parse_path_components(normalized_path, components, &component_count);
        
        if (component_count == 0) {
            // Handle empty path error
            continue;
        }
        
        // Traverse/build the tree
        TreeNode* current = nm->root;
        for (int j = 0; j < component_count; j++) {
            int found = 0;
            for (int k = 0; k < current->child_count; k++) {
                if (strcmp(current->children[k]->name, components[j]) == 0) {
                    current = current->children[k];
                    found = 1;
                    break;
                }
            }
            
            if (!found) {
                // Determine if this component should be a directory
                int should_be_directory = is_directory || (j < component_count - 1);
                
                // Create new node
                TreeNode* new_node = create_tree_node(components[j], should_be_directory);
                if (new_node == NULL) {
                    // Handle memory allocation error
                    for (int cleanup = 0; cleanup < component_count; cleanup++) {
                        free(components[cleanup]);
                    }
                    // Add appropriate error handling here
                    continue;
                }
                
                add_child_node(current, new_node);
                // printf("%s\n", get_node_path(new_node));
                current = new_node;
            } else {
                // If this is the last component and the path ends with '/',
                // ensure the existing node is marked as a directory
                if (j == component_count - 1 && is_directory) {
                    current->is_directory = 1;
                }
            }
        }
        
        // Add server ID to the final node
        //   fprintf(stderr, "coming here!\n");
        add_server_to_node(current, ss->id);
        //   fprintf(stderr, "went here!\n");
        // Clean up components
        for (int j = 0; j < component_count; j++) {
            free(components[j]);
        }
    }
    // Send success response
    json_object* response = json_object_new_object();
    json_object_object_add(response, "status", json_object_new_string("success"));
    json_object_object_add(response, "ss_id", json_object_new_int(ss->id)); // tell gaur.

    send_message(client_socket, response);
    json_object_put(response);

    pthread_mutex_unlock(&nm->lock);
    return ss->id - 1;
}

int find_storage_server_with_path(const char *path) {
    int num_ss = nm->storage_server_count;

    TreeNode *tmp = lru_cache_get(cache, path);
    if(tmp == NULL) {
        tmp = find_node(nm->root, path);
        lru_cache_put(cache, path, tmp);
    }
    
    if(tmp != NULL) {
        int mn = INT_MAX;
        int min_id = -1;
        for(int i = 0; i < tmp->server_count; i ++) {
            if(mn > nm->storage_servers[tmp->storage_servers[i] - 1]->current_load) {
                mn = nm->storage_servers[tmp->storage_servers[i] - 1]->current_load;
                min_id = nm->storage_servers[tmp->storage_servers[i] - 1]->id; 
            }
        }
        return min_id;
    }
    
    return -1;
}

int is_parent_of_path(TreeNode* node, const char* path) {
    char* node_path = get_node_path(node);
    size_t node_path_len = strlen(node_path);
    int is_parent = strncmp(node_path, path, node_path_len) == 0 && (path[node_path_len] == '/' || path[node_path_len] == '\0');
    free(node_path);
    return is_parent;
}

// Find the most suitable storage server for a new path
StorageServer* find_best_storage_server_for_creation(const char* path) {
    StorageServer* best_server = NULL;
    int min_load = INT_MAX;
    
    // First, try to find servers that host parent directories
    TreeNode* parent_node = NULL;
    char parent_path[MAX_PATH_LENGTH];
    strncpy(parent_path, path, MAX_PATH_LENGTH - 1);
    
    // Get parent path by removing last component
    char* last_slash = strrchr(parent_path, '/');
    if (last_slash != NULL) {
        if (last_slash == parent_path) {
            // Parent is root
            parent_path[1] = '\0';
        } else {
            *last_slash = '\0';
        }
        parent_node = lru_cache_get(cache, path);
        if(parent_node == NULL) {
            parent_node = find_node(nm->root, path);
            lru_cache_put(cache, path, parent_node);
        }
        // parent_node = find_node(nm->root, parent_path);
    }
    
    if (parent_node != NULL) {
        // First preference: Use servers that host the parent directory
        for (int i = 0; i < parent_node->server_count; i++) {
            int server_id = parent_node->storage_servers[i];
            StorageServer* ss = nm->storage_servers[server_id - 1];
            
            if (ss->active && ss->current_load < min_load) {
                min_load = ss->current_load;
                best_server = ss;
            }
        }
    }
    
    // If no suitable server found hosting parent directory,
    // find server with least load that can access a path closest to requested path
    if (best_server == NULL) {
        for (int i = 0; i < nm->storage_server_count; i++) {
            StorageServer* ss = nm->storage_servers[i];
            if (!ss->active) continue;
            
            // Check if this server can handle the new path
            for (int j = 0; j < ss->path_count; j++) {
                TreeNode *tmp = lru_cache_get(cache, path);
                if(tmp == NULL) {
                    tmp = find_node(nm->root, path);
                    lru_cache_put(cache, path, tmp);
                }
                if (is_parent_of_path(tmp, path)) {
                    if (ss->current_load < min_load) {
                        min_load = ss->current_load;
                        best_server = ss;
                    }
                    break;
                }
            }
        }

        if(best_server == NULL) {
            for(int i = 0; i < nm->storage_server_count; i ++) {
                StorageServer* ss = nm->storage_servers[i];
                if (!ss->active) continue;

                if (ss->current_load < min_load) {
                    min_load = ss->current_load;
                    best_server = ss;
                }
            }
        }
    }
    return best_server;
}

int handle_client_connection(int client_socket, json_object* request) {
    pthread_mutex_lock(&nm->lock);
    
    json_object* operation_obj;
    json_object_object_get_ex(request, "operation", &operation_obj);
    
    json_object *ip_object, *port_object, *req_id;
    json_object_object_get_ex(request, "client_ip", &ip_object);
    json_object_object_get_ex(request, "client_port", &port_object);
    json_object_object_get_ex(request, "request_id", &req_id);

    // client is sending a unique request id on every request of its.
    int req_id_client = json_object_get_int(req_id);

    Client* cl;
    
    bool is_found_in_queue = false;
    for(int i = 0; i < nm->client_count; i ++) {
        if(strcmp(nm->clients_in_queue[i]->ip, json_object_get_string(ip_object)) == 0 && nm->clients_in_queue[i]->port == json_object_get_int(port_object)) {
            cl = nm->clients_in_queue[i];
            is_found_in_queue = true;
            break;
        }
    }

    if(!is_found_in_queue) {
        cl = (Client*)malloc(sizeof(Client));
        cl->id = nm->client_count + 1;
        strcpy(cl->ip, json_object_get_string(ip_object));
        
        cl->port = json_object_get_int(port_object);
        cl->pending_count = 0;

        nm->client_count ++;
        nm->clients_in_queue[nm->client_count - 1] = cl;
    }

    const char* operation = json_object_get_string(operation_obj);
    json_object* response = json_object_new_object();
    if(strcmp(operation, "read") == 0 || strcmp(operation, "write") == 0 || strcmp(operation, "stream") == 0 || strcmp(operation, "get_info") == 0) {
        json_object* path_obj;
        json_object_object_get_ex(request, "path", &path_obj);
        const char* path = json_object_get_string(path_obj);

        // int server_with_path = find_storage_server_with_path(path);
        TreeNode *existing_node = find_node(nm->root, path);
        if (existing_node == NULL) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("no file location found!\n"));
            send_message(client_socket, response);
            json_object_put(response);

            pthread_mutex_unlock(&nm->lock);
            free(cl);
            return -1;
        }
        // fprintf(stderr, "%d\n", existing_node->storage_servers[0]);
        json_object_object_add(response, "ip", json_object_new_string(nm->storage_servers[existing_node->storage_servers[0] - 1]->ip));
        json_object_object_add(response, "client_port", json_object_new_int(nm->storage_servers[existing_node->storage_servers[0] - 1]->client_port));
    }else     
    if (strcmp(operation, "create") == 0) {
        json_object *path_obj, *name_obj, *check_obj;
        json_object_object_get_ex(request, "path", &path_obj);

        const char* path = json_object_get_string(path_obj);

        json_object_object_get_ex(request, "name", &name_obj);
        const char* name = json_object_get_string(name_obj);

        json_object_object_get_ex(request, "is_directory", &check_obj);
        bool is_dir = json_object_get_boolean(path_obj);
        
        char *path1 = (char *) malloc(sizeof(char) * MAX_PATH_LENGTH);
        strcpy(path1, path);
        path1 = strcat(path1, name);

        // Handle path creation
        // TreeNode* existing_node = find_node(nm->root, path1);
        TreeNode *existing_node = find_node(nm->root, path);
        if (existing_node != NULL) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("Path already exists"));
            send_message(client_socket, response);
            json_object_put(response);

            pthread_mutex_unlock(&nm->lock);
            return -1;
        }
        
        StorageServer* best_server = find_best_storage_server_for_creation(path1); // come here  
        if (best_server == NULL) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("No suitable storage server found"));

            send_message(client_socket, response);
            json_object_put(response);
            pthread_mutex_unlock(&nm->lock);
            return -1;
        } else {
            cl->pending_count ++;
            cl->pending_requests[cl->pending_count - 1] = req_id_client;

            int c_load = best_server->current_load ++;
            best_server->processing_requests[c_load] = req_id_client;

            json_object *request = json_object_new_object();
            json_object_object_add(request, "request_code", json_object_new_string("create_empty"));
            json_object_object_add(request, "request_id", json_object_new_int(req_id_client));
            json_object_object_add(request, "path", json_object_new_string(path1));
            json_object_object_add(request, "type", json_object_new_boolean(is_dir));

            // int ss_sock = create_socket_to_send_to(best_server->ip, best_server->nm_port);
            send_message(best_server->sock, request);
            json_object_put(request);

            free(path1);
            // close(ss_sock);
        }
    }
    else if (strcmp(operation, "delete") == 0) {
        json_object *path_obj;
        json_object_object_get_ex(request, "path", &path_obj);

        const char* path = json_object_get_string(path_obj);

        // Handle path creation
        // TreeNode* existing_node = find_node(nm->root, path);
        // fprintf(stderr, "hello\n");
        int server_with_path = find_storage_server_with_path(path);
        // fprintf("%d\n", server_with_path);

        if (server_with_path == -1) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("No server with the path found!"));
            send_message(client_socket, response);
            json_object_put(response);

            pthread_mutex_unlock(&nm->lock);
            return -1;
        }

        cl->pending_count ++;
        cl->pending_requests[cl->pending_count - 1] = req_id_client;

        int c_load = nm->storage_servers[server_with_path - 1]->current_load ++;
        nm->storage_servers[server_with_path - 1]->processing_requests[c_load] = req_id_client;

        json_object *request = json_object_new_object();
        json_object_object_add(request, "request_id", json_object_new_int(req_id_client));
        json_object_object_add(request, "request_code", json_object_new_string("delete"));
        json_object_object_add(request, "path", json_object_new_string(path));

        // int ss_sock = create_socket_to_send_to(nm->storage_servers[server_with_path]->ip, nm->storage_servers[server_with_path]->nm_port);
        // send_message(ss_sock, request);
        // json_object_put(request);
        send_message(nm->storage_servers[server_with_path - 1]->sock, request);
        json_object_put(request);

        // json_object *response1;
        // response1 = receive_request(nm->storage_servers[server_with_path - 1]->sock);

        // json_object* status_obj;
        // if (json_object_object_get_ex(response1, "status", &status_obj)) {
        //     const char* status = json_object_get_string(status_obj);
        //     printf("Storage Server response: %s\n", status);

        //     if(strcmp(status, "success") == 0) {
        //         json_object_object_add(response, "status", json_object_new_string("success"));
        //     }else if(strcmp(status, "failure") == 0) { 
        //         json_object *msg;
        //         json_object_object_get_ex(response1, "error", &msg);
        //         printf("%s\n", json_object_get_string(msg));
        //         json_object_object_add(response, "status", json_object_new_string("error"));
        //     }
        // }

        // json_object_put(response1);
        // close(ss_sock);
        
    }
    else if (strcmp(operation, "copy") == 0) {
        json_object *path_obj1, *path_obj2;
        json_object_object_get_ex(request, "path1", &path_obj1);
        json_object_object_get_ex(request, "path2", &path_obj2);

        const char* path1 = json_object_get_string(path_obj1);
        const char* path2 = json_object_get_string(path_obj2);

        // Handle path creation
        // TreeNode* existing_node = find_node(nm->root, path);
        int source_server = find_storage_server_with_path(path1);
        if (source_server == -1) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("No server with the source path found!"));
            send_message(client_socket, response);
            json_object_put(response);

            pthread_mutex_unlock(&nm->lock);
            free(cl);
            return -1;
        }

        int dest_server = find_storage_server_with_path(path2);
        if (dest_server == -1) {
            json_object_object_add(response, "status", json_object_new_string("error"));
            json_object_object_add(response, "message", json_object_new_string("No server with the dest path found!"));
            send_message(client_socket, response);
            json_object_put(response);

            pthread_mutex_unlock(&nm->lock);
            free(cl);
            return -1;
        }

        cl->pending_count ++;
        cl->pending_requests[cl->pending_count - 1] = req_id_client;

        int c_load = nm->storage_servers[dest_server]->current_load ++;
        nm->storage_servers[dest_server]->processing_requests[c_load] = req_id_client;
        
        json_object *request = json_object_new_object();
        json_object_object_add(request, "request_code", json_object_new_string("copy"));
        json_object_object_add(request, "request_id", json_object_new_int(req_id_client));
        json_object_object_add(request, "destination_path", json_object_new_string(path1));

        json_object_object_add(request, "storage_ip", json_object_new_string(nm->storage_servers[source_server]->ip));
        json_object_object_add(request, "storage_port", json_object_new_int(nm->storage_servers[source_server]->client_port));
        json_object_object_add(request, "storage_path", json_object_new_string(path2));

        // int ss_sock = create_socket_to_send_to(nm->storage_servers[dest_server]->ip, nm->storage_servers[dest_server]->nm_port);
        // send_message(ss_sock, request);
        // json_object_put(request);

        send_message(nm->storage_servers[dest_server]->sock, request);
        json_object_put(request);

        // json_object *response1;
        // response1 = receive_request(nm->storage_servers[dest_server]->sock);

        // json_object* status_obj;
        // if (json_object_object_get_ex(response1, "status", &status_obj)) {
        //     const char* status = json_object_get_string(status_obj);
        //     printf("Storage Server response: %s\n", status);

        //     if(strcmp(status, "success") == 0) {
        //         json_object_object_add(response, "status", json_object_new_string("success"));
        //     }else if(strcmp(status, "error") == 0) {
        //         json_object_object_add(response, "status", json_object_new_string("error"));
        //     }
        // }

        // json_object_put(response1);
        // close(ss_sock);
    }else if(strcmp(operation, "list_all") == 0) {
        json_object* paths_array = json_object_new_array();
    
        // Start recursive path building from root
        build_path_list(nm->root, "", paths_array);
        
        json_object_object_add(response, "status", json_object_new_string("success"));
        json_object_object_add(response, "paths", paths_array);
        
        // send_message(client_socket, response);
        // json_object_put(response);
    }
    
    send_message(client_socket, response);
    // fprintf(stderr, "hello\n");
    json_object_put(response);
    // fprintf(stderr, "hello\n");

    pthread_mutex_unlock(&nm->lock);   

    return cl->id;
}

int create_socket_to_send_to(const char* ip_address, int port) {
    // Create socket
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0) {
        perror("Socket creation failed");
        return -1;
    }

    // Initialize server address structure
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    // Convert IP address from string to binary form
    if (inet_pton(AF_INET, ip_address, &server_addr.sin_addr) <= 0) {
        perror("Invalid address / Address not supported");
        close(client_socket);
        return -1;
    }

    // Connect to server
    if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(client_socket);
        return -1;
    }

    return client_socket;
}

// Helper function to send response
int send_message(int socket, json_object* msg) {
    const char* json_str = json_object_to_json_string(msg);
    uint32_t length = strlen(json_str);
    uint32_t network_length = htonl(length);
    
    if(send(socket, &network_length, sizeof(network_length), 0) == -1) {
        return -1;
    }

    if(send(socket, json_str, length, 0) == -1) {
        return -1;
    }
}

// Helper function to receive request
json_object* receive_request(int socket) {
    uint32_t length;
    if (recv(socket, &length, sizeof(length), 0) <= 0) {
        return NULL;
    }
    length = ntohl(length);

    char* buffer = (char*)malloc(length + 1);
    int total_received = 0;
    while (total_received < length) {
        int received = recv(socket, buffer + total_received, length - total_received, 0);
        if (received <= 0) {
            free(buffer);
            return NULL;
        }
        total_received += received;
    }
    buffer[length] = '\0';
    fprintf(stderr, "%s\n", buffer);

    json_object* request = json_tokener_parse(buffer);
    free(buffer);
    return request;
}

// Cleanup function
void cleanup_naming_server() {
    pthread_mutex_lock(&nm->lock);
    free_tree_node(nm->root);
    for (int i = 0; i < nm->storage_server_count; i++) {
        StorageServer* ss = nm->storage_servers[i];
        for (int j = 0; j < ss->path_count; j++) {
            free(ss->accessible_paths[j]);
        }
        // free(ss->accessible_paths);
        free(ss);
    }

    for (int i = 0; i < nm->client_count; i++) {
        Client* cl = nm->clients_in_queue[i];
        free(cl);
    }
    
    pthread_mutex_unlock(&nm->lock);
    pthread_mutex_destroy(&nm->lock);
    close(nm->server_socket);
}

int main(int argc, char* argv[]) {
    initialize_naming_server();

    // Create server socket
    nm->server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm->server_socket < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(nm->server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("Setsockopt failed");
        return 1;
    }

    // Setup server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(argc > 1 ? atoi(argv[1]) : 0);


    // Bind
    if (bind(nm->server_socket, (struct sockaddr*)&server_addr, 
        sizeof(server_addr)) < 0) {
        perror("Bind failed");
        return 1;
    }

    // Get the assigned port if using port 0
    socklen_t len = sizeof(server_addr);
    if (getsockname(nm->server_socket, (struct sockaddr*)&server_addr, &len) < 0) {
        perror("Getsockname failed");
        return 1;
    }

    printf("%s\n", get_local_ip());
    printf("Naming Server started on port %d\n", ntohs(server_addr.sin_port));

    // Listen
    if (listen(nm->server_socket, 100) < 0) {
        perror("Listen failed");
        return 1;
    }

    // Accept connections
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int* client_socket = malloc(sizeof(int));
        *client_socket = accept(nm->server_socket, (struct sockaddr*)&client_addr, &client_len);
        if (*client_socket < 0) {
            perror("Accept failed");
            free(client_socket);
            continue;
        }

        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_connection, client_socket) != 0) {
            perror("Thread creation failed");
            close(*client_socket);
            free(client_socket);
            continue;
        }
        pthread_detach(thread);
    }

    cleanup_naming_server();
    return 0;
}