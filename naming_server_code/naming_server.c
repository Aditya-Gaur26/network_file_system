#include "headers.h"

TreeNode* create_tree_node(const char* name, int is_directory) {
    TreeNode* node = (TreeNode*)malloc(sizeof(TreeNode));
    strncpy(node->name, name, MAX_FILENAME_LENGTH - 1);
    node->name[MAX_FILENAME_LENGTH - 1] = '\0';
    node->is_directory = is_directory;
    node->parent = NULL;
    node->max_children = MAX_CHILDREN;
    node->children = (TreeNode**)malloc(sizeof(TreeNode*) * node->max_children);
    node->child_count = 0;
    node->storage_servers = (char**)malloc(sizeof(char*) * MAX_STORAGE_SERVERS);
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
void add_server_to_node(TreeNode* node, const char* server_id) {
    for (int i = 0; i < node->server_count; i++) {
        if (strcmp(node->storage_servers[i], server_id) == 0) {
            return;  // Server already registered for this node
        }
    }
    
    if (node->server_count < MAX_STORAGE_SERVERS) {
        node->storage_servers[node->server_count] = strdup(server_id);
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

// Convert tree to JSON for directory listing
json_object* tree_to_json(TreeNode* node) {
    json_object* obj = json_object_new_object();
    json_object_object_add(obj, "name", json_object_new_string(node->name));
    json_object_object_add(obj, "type", 
        json_object_new_string(node->is_directory ? "directory" : "file"));
    
    if (node->is_directory) {
        json_object* children = json_object_new_array();
        for (int i = 0; i < node->child_count; i++) {
            json_object_array_add(children, tree_to_json(node->children[i]));
        }
        json_object_object_add(obj, "children", children);
    }
    
    json_object* servers = json_object_new_array();
    for (int i = 0; i < node->server_count; i++) {
        json_object_array_add(servers, 
            json_object_new_string(node->storage_servers[i]));
    }
    json_object_object_add(obj, "storage_servers", servers);
    
    return obj;
}

void free_tree_node(TreeNode *node) {
    free(node);
}

// tree directory structure functions end here.


// Initialize the Naming Server
void initialize_naming_server(NamingServer* nm) {
    nm->storage_server_count = 0;
    pthread_mutex_init(&nm->lock, NULL);
    nm->root = create_tree_node("/", 1);
    memset(nm->storage_servers, 0, sizeof(nm->storage_servers));
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
    NamingServer* nm = (NamingServer*)pthread_getspecific(*(pthread_key_t*)arg);

    json_object* request = receive_request(client_socket);
    if (request == NULL) {
        close(client_socket);
        return NULL;
    }

    json_object* type_obj;
    if (json_object_object_get_ex(request, "type", &type_obj)) {
        const char* type = json_object_get_string(type_obj);
        
        if (strcmp(type, "storage_server_register") == 0) {
            printf("Storage server has come!\n");
            handle_storage_server_registration(nm, client_socket, request);
        } else if (strcmp(type, "client_request") == 0) {
            handle_client_request(nm, client_socket, request);
        }
    }

    json_object_put(request);
    close(client_socket);
    return NULL;
}

// Handle Storage Server registration
void handle_storage_server_registration(NamingServer* nm, int client_socket, json_object* request) {
    pthread_mutex_lock(&nm->lock);

    if (nm->storage_server_count >= MAX_STORAGE_SERVERS) {
        json_object* response = json_object_new_object();
        json_object_object_add(response, "status", json_object_new_string("error"));
        json_object_object_add(response, "message", json_object_new_string("Maximum storage servers reached"));
        send_response(client_socket, response);
        json_object_put(response);
        pthread_mutex_unlock(&nm->lock);
        return;
    }

    // Create new storage server entry
    StorageServer* ss = (StorageServer*)malloc(sizeof(StorageServer));
    snprintf(ss->id, sizeof(ss->id), "SS_%d", nm->storage_server_count + 1);

    // Get IP and ports from request
    json_object* ip_obj, *nm_port_obj, *client_port_obj, *paths_obj;
    json_object_object_get_ex(request, "ip", &ip_obj);
    json_object_object_get_ex(request, "nm_port", &nm_port_obj);
    json_object_object_get_ex(request, "client_port", &client_port_obj);
    json_object_object_get_ex(request, "accessible_paths", &paths_obj);

    strncpy(ss->ip, json_object_get_string(ip_obj), sizeof(ss->ip) - 1);
    ss->nm_port = json_object_get_int(nm_port_obj);
    ss->client_port = json_object_get_int(client_port_obj);


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
        add_server_to_node(current, ss->id);
        
        // Clean up components
        for (int j = 0; j < component_count; j++) {
            free(components[j]);
        }
    }

    ss->active = 1;
    ss->last_heartbeat = time(NULL);

    // Add to naming server
    nm->storage_servers[nm->storage_server_count++] = ss;

    // Send success response
    json_object* response = json_object_new_object();
    json_object_object_add(response, "status", json_object_new_string("success"));
    json_object_object_add(response, "ss_id", json_object_new_string(ss->id));
    send_response(client_socket, response);
    json_object_put(response);

    pthread_mutex_unlock(&nm->lock);
}

// Handle client requests
void handle_client_request(NamingServer* nm, int client_socket, json_object* request) {
    json_object* operation_obj;
    json_object_object_get_ex(request, "operation", &operation_obj);
    const char* operation = json_object_get_string(operation_obj);

    pthread_mutex_lock(&nm->lock);

    if (strcmp(operation, "get_file_location") == 0) {
        json_object* path_obj;
        json_object_object_get_ex(request, "path", &path_obj);
        const char* requested_path = json_object_get_string(path_obj);

        // Create response array of servers that have the file
        json_object* servers_array = json_object_new_array();

        for (int i = 0; i < nm->storage_server_count; i++) {
            StorageServer* ss = nm->storage_servers[i];
            if (!ss->active) continue;

            for (int j = 0; j < ss->path_count; j++) {
                if (strcmp(ss->accessible_paths[j], requested_path) == 0) {
                    json_object* server_info = json_object_new_object();
                    json_object_object_add(server_info, "id", json_object_new_string(ss->id));
                    json_object_object_add(server_info, "ip", json_object_new_string(ss->ip));
                    json_object_object_add(server_info, "client_port", 
                        json_object_new_int(ss->client_port));
                    json_object_array_add(servers_array, server_info);
                    break;
                }
            }
        }

        json_object* response = json_object_new_object();
        json_object_object_add(response, "status", json_object_new_string("success"));
        json_object_object_add(response, "servers", servers_array);
        send_response(client_socket, response);
        json_object_put(response);

    } else if (strcmp(operation, "list_files") == 0) {
        json_object* files_array = json_object_new_array();
        
        // Collect unique paths from all storage servers
        for (int i = 0; i < nm->storage_server_count; i++) {
            StorageServer* ss = nm->storage_servers[i];
            if (!ss->active) continue;

            for (int j = 0; j < ss->path_count; j++) {
                json_object_array_add(files_array, 
                    json_object_new_string(ss->accessible_paths[j]));
            }
        }

        json_object* response = json_object_new_object();
        json_object_object_add(response, "status", json_object_new_string("success"));
        json_object_object_add(response, "files", files_array);
        send_response(client_socket, response);
        json_object_put(response);
    }

    pthread_mutex_unlock(&nm->lock);
}

// Helper function to send response
void send_response(int socket, json_object* response) {
    const char* json_str = json_object_to_json_string(response);
    uint32_t length = strlen(json_str);
    uint32_t network_length = htonl(length);
    
    send(socket, &network_length, sizeof(network_length), 0);
    send(socket, json_str, length, 0);
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
        int received = recv(socket, buffer + total_received, 
            length - total_received, 0);
        if (received <= 0) {
            free(buffer);
            return NULL;
        }
        total_received += received;
    }
    buffer[length] = '\0';

    json_object* request = json_tokener_parse(buffer);
    free(buffer);
    return request;
}

// Cleanup function
void cleanup_naming_server(NamingServer* nm) {
    pthread_mutex_lock(&nm->lock);
    free_tree_node(nm->root);
    for (int i = 0; i < nm->storage_server_count; i++) {
        StorageServer* ss = nm->storage_servers[i];
        for (int j = 0; j < ss->path_count; j++) {
            free(ss->accessible_paths[j]);
        }
        free(ss->accessible_paths);
        free(ss);
    }
    
    pthread_mutex_unlock(&nm->lock);
    pthread_mutex_destroy(&nm->lock);
    close(nm->server_socket);
}

int main(int argc, char* argv[]) {
    NamingServer nm;
    initialize_naming_server(&nm);

    // Create server socket
    nm.server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm.server_socket < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(nm.server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
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
    if (bind(nm.server_socket, (struct sockaddr*)&server_addr, 
        sizeof(server_addr)) < 0) {
        perror("Bind failed");
        return 1;
    }

    // Get the assigned port if using port 0
    socklen_t len = sizeof(server_addr);
    if (getsockname(nm.server_socket, (struct sockaddr*)&server_addr, &len) < 0) {
        perror("Getsockname failed");
        return 1;
    }

    printf("%s\n", get_local_ip());
    printf("Naming Server started on port %d\n", ntohs(server_addr.sin_port));

    // Listen
    if (listen(nm.server_socket, 10) < 0) {
        perror("Listen failed");
        return 1;
    }

    // Accept connections
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int* client_socket = malloc(sizeof(int));
        *client_socket = accept(nm.server_socket, (struct sockaddr*)&client_addr, 
            &client_len);
        
        if (*client_socket < 0) {
            perror("Accept failed");
            free(client_socket);
            continue;
        }

        printf("Connection has come!\n");
        pthread_t thread;
        if (pthread_create(&thread, NULL, handle_connection, client_socket) != 0) {
            perror("Thread creation failed");
            close(*client_socket);
            free(client_socket);
            continue;
        }
        pthread_detach(thread);
    }

    cleanup_naming_server(&nm);
    return 0;
}