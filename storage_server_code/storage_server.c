#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <netdb.h>
#include <json-c/json.h>
#include <ifaddrs.h>

#define MAX_PATHS 100
#define MAX_PATH_LENGTH 256
#define BUFFER_SIZE 1024
#define MAX_CLIENTS 20

typedef struct {
    int server_id;
    char ip_address[16];
    int nm_port;
    int client_port;
    char accessible_paths[MAX_PATHS][MAX_PATH_LENGTH];
    int path_count;
} StorageServer;

typedef struct {
    int socket;
    struct sockaddr_in address;
    StorageServer* server;  // Added server pointer
} SocketInfo;

typedef struct {
    int client_fd;
    struct sockaddr_in address;
    StorageServer* server;
} ClientInfo;

// Global variables for client management
pthread_t client_threads[MAX_CLIENTS];
ClientInfo* active_clients[MAX_CLIENTS];
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
int concurrent_clients = 0;
StorageServer server;
int client_socket;
// Function prototypes
int initialize_socket(int port);
int connect_to_naming_server(const char* nm_ip, int nm_port);
void register_with_naming_server(StorageServer* server, int nm_socket);
void* handle_client_connections(void* arg);
void* handle_single_client(void* arg);
int validate_path(const char* path);
void print_server_info(StorageServer* server);
void cleanup_client_threads(void);
char* get_local_ip();

json_object* receive_request(int socket) {
    uint32_t length;
    if (recv(socket, &length, sizeof(length), 0) < 0) {
        return NULL;
    }
    length = ntohl(length);

    char* buffer = (char*)malloc(length + 1);
    int total_received = 0;
    while (total_received < length) {
        int received = recv(socket, buffer + total_received, length - total_received, 0);
        if (received < 0) {
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



int main(int argc, char* argv[]) {
    if (argc < 5) {
        printf("Usage: %s <server_id> <naming_server_ip> <naming_server_port> <client_port> [paths...]\n", argv[0]);
        return 1;
    }

  
    server.server_id = atoi(argv[1]);
    server.nm_port = atoi(argv[3]);
    server.client_port = atoi(argv[4]);
    server.path_count = 0;

    // get_local_ip
    strcpy(server.ip_address, get_local_ip());

    // Process accessible paths
    for (int i = 5; i < argc && server.path_count < MAX_PATHS; i++) {
        if (validate_path(argv[i])) {
            strncpy(server.accessible_paths[server.path_count], argv[i], MAX_PATH_LENGTH - 1);
            server.accessible_paths[server.path_count][MAX_PATH_LENGTH - 1] = '\0';
            server.path_count++;
        } else {
            printf("Warning: Invalid path '%s', skipping\n", argv[i]);
        }
    }

    print_server_info(&server);

    // Connect to naming server
    int nm_socket = connect_to_naming_server(argv[2], server.nm_port);
    if (nm_socket < 0) {
        printf("Failed to connect to naming server\n");
        return 1;
    }

    // Register with naming server
    register_with_naming_server(&server, nm_socket);

    // Initialize client socket
    client_socket = initialize_socket(server.client_port);
    if (client_socket < 0) {
        printf("Failed to initialize client socket\n");
        close(nm_socket);
        return 1;
    }

    // Initialize client thread arrays
    memset(client_threads, 0, sizeof(client_threads));
    memset(active_clients, 0, sizeof(active_clients));

    // Create thread to handle client connections
    pthread_t client_thread;
    SocketInfo client_info = {
        .socket = client_socket,
        .server = &server
    };
    if (pthread_create(&client_thread, NULL, handle_client_connections, &client_info) != 0) {
        printf("Failed to create client handling thread\n");
        close(nm_socket);
        close(client_socket);
        return 1;
    }

    printf("Storage Server %d initialized and running\n", server.server_id);

    // Main loop
    char buffer[BUFFER_SIZE];
    while (1) {
        

        json_object* nm_respone = receive_request(nm_socket);
        if (nm_respone == NULL) {
            printf("Unsupported Format! Closing client socket!\n");
            close(client_socket);
            return NULL;
        }
        json_object* status;
        json_object* ss_id;

        if (json_object_object_get_ex(nm_respone, "status", &status)) {
            const char* type = json_object_get_string(status);
            printf("%s\n",type);
            
        }
        if (json_object_object_get_ex(nm_respone, "ss_id", &ss_id)) {
            const char* type = json_object_get_string(ss_id);
            printf("%s\n",type);
        }


    }

    // Cleanup
    cleanup_client_threads();
    close(nm_socket);
    close(client_socket);
    pthread_cancel(client_thread);
    pthread_join(client_thread, NULL);

    return 0;
}

void* handle_single_client(void* arg) {
    ClientInfo* client = (ClientInfo*)arg;
    char buffer[BUFFER_SIZE];
    int bytes_read;

    printf("Started handling client: %s:%d\n",
           inet_ntoa(client->address.sin_addr),
           ntohs(client->address.sin_port));

    while ((bytes_read = recv(client->client_fd, buffer, BUFFER_SIZE - 1, 0)) >= 0) {
        if(bytes_read == 0 ){
            int check_dead = recv(client->client_fd,buffer,1,MSG_PEEK);
            if(check_dead == 0)break;
            continue;
        }

        buffer[bytes_read] = '\0';
        
        // Handle client request here
        printf("Received from client %s:%d: %s\n",
               inet_ntoa(client->address.sin_addr),
               ntohs(client->address.sin_port),
               buffer);
        
        // Echo back to client (replace with actual response handling)
        send(client->client_fd, "Message received\n", 16, 0);
    }

    printf("Client disconnected: %s:%d\n",
           inet_ntoa(client->address.sin_addr),
           ntohs(client->address.sin_port));

    // Cleanup client resources
    close(client->client_fd);
    
    // Remove client from active clients
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (active_clients[i] == client) {
            active_clients[i] = NULL;
            break;
        }
    }
    concurrent_clients--;
    pthread_mutex_unlock(&clients_mutex);
    
    free(client);
    return NULL;
}

void* handle_client_connections(void* arg) {
    SocketInfo* info = (SocketInfo*)arg;
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    while (1) {
        while(concurrent_clients >= MAX_CLIENTS) {
            usleep(10);  // Sleep for 1ms when max clients reached
        }

        int client_fd = accept(info->socket, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        // Create new client info
        ClientInfo* client = (ClientInfo*)malloc(sizeof(ClientInfo));
        client->client_fd = client_fd;
        memcpy(&client->address, &client_addr, sizeof(struct sockaddr_in));
        client->server = info->server;

        // Find available slot
        pthread_mutex_lock(&clients_mutex);
        int slot = -1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (active_clients[i] == NULL) {
                slot = i;
                break;
            }
        }

        if (slot == -1) {
            pthread_mutex_unlock(&clients_mutex);
            printf("No slots available for new client\n");
            close(client_fd);
            free(client);
            continue;
        }

        // Create new thread for client
        if (pthread_create(&client_threads[slot], NULL, handle_single_client, client) != 0) {
            pthread_mutex_unlock(&clients_mutex);
            perror("Failed to create client thread");
            close(client_fd);
            free(client);
            continue;
        }

        active_clients[slot] = client;
        concurrent_clients++;
        pthread_mutex_unlock(&clients_mutex);

        printf("New client connected: %s:%d (slot: %d)\n",
               inet_ntoa(client_addr.sin_addr),
               ntohs(client_addr.sin_port),
               slot);
    }

    return NULL;
}

void cleanup_client_threads() {
    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (active_clients[i] != NULL) {
            pthread_cancel(client_threads[i]);
            close(active_clients[i]->client_fd);
            free(active_clients[i]);
            active_clients[i] = NULL;
        }
    }
    concurrent_clients = 0;
    pthread_mutex_unlock(&clients_mutex);
}

// Your existing functions remain unchanged
int initialize_socket(int port) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("Socket creation failed");
        return -1;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(sock_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        close(sock_fd);
        return -1;
    }

    if (listen(sock_fd, 5) < 0) {
        perror("Listen failed");
        close(sock_fd);
        return -1;
    }

    return sock_fd;
}

int connect_to_naming_server(const char* nm_ip, int nm_port) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("Socket creation failed");
        return -1;
    }

    struct sockaddr_in nm_address;
    nm_address.sin_family = AF_INET;
    nm_address.sin_port = htons(nm_port);
    
    if (inet_pton(AF_INET, nm_ip, &nm_address.sin_addr) <= 0) {
        perror("Invalid naming server address");
        close(sock_fd);
        return -1;
    }

    if (connect(sock_fd, (struct sockaddr*)&nm_address, sizeof(nm_address)) < 0) {
        perror("Connection to naming server failed");
        close(sock_fd);
        return -1;
    }

    return sock_fd;
}

void register_with_naming_server(StorageServer* server, int nm_socket) {
   json_object *request = json_object_new_object();

    // Add the fields to the JSON object
    json_object_object_add(request, "type", json_object_new_string("storage_server_register"));
    json_object_object_add(request, "ip", json_object_new_string(server->ip_address));
    json_object_object_add(request, "nm_port", json_object_new_int(server->nm_port));
    json_object_object_add(request, "client_port", json_object_new_int(server->client_port));

    // Create the "accessible_paths" array and add paths to it
    json_object *paths_array = json_object_new_array();
    for (int i = 0; i < server->path_count; i++) {
        json_object_array_add(paths_array, json_object_new_string(server->accessible_paths[i]));
    }
    json_object_object_add(request, "paths", paths_array);

    // Convert the JSON object to a string for sending
   
    // Print the JSON string for debugging

   
    const char* registration = json_object_to_json_string(request);
    printf("%s\n", registration); 
    uint32_t length = strlen(registration);
    uint32_t network_length = htonl(length);
    
    send(nm_socket, &network_length, sizeof(network_length), 0);
    send(nm_socket, registration, length, 0);
    json_object_put(request);
}

int validate_path(const char* path) {
    return 1;
    // DIR* dir = opendir(path);
    // if (dir) {
    //     closedir(dir);
    //     return 1;
    // }

    // FILE* file = fopen(path, "r");
    // if (file) {
    //     fclose(file);
    //     return 1;
    // }

    // return 0;
}

void print_server_info(StorageServer* server) {
    printf("\nStorage Server Information:\n");
    printf("Server ID: %d\n", server->server_id);
    printf("IP Address: %s\n", server->ip_address);
    printf("Naming Server Port: %d\n", server->nm_port);
    printf("Client Port: %d\n", server->client_port);
    printf("Accessible Paths:\n");
    for (int i = 0; i < server->path_count; i++) {
        printf("  %d: %s\n", i + 1, server->accessible_paths[i]);
    }
    printf("\n");
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