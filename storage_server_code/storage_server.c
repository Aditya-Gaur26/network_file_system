#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dirent.h>
#include <errno.h>
#include <pthread.h>

#define MAX_PATHS 100
#define MAX_PATH_LENGTH 256
#define BUFFER_SIZE 1024

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
} SocketInfo;

// Function prototypes
int initialize_socket(int port);
int connect_to_naming_server(const char* nm_ip, int nm_port);
void register_with_naming_server(StorageServer* server, int nm_socket);
void* handle_client_connections(void* arg);
int validate_path(const char* path);
void print_server_info(StorageServer* server);

int main(int argc, char* argv[]) {
    if (argc < 5) {
        printf("Usage: %s <server_id> <naming_server_ip> <naming_server_port> <client_port> [paths...]\n", argv[0]);
        return 1;
    }

    StorageServer server;
    server.server_id = atoi(argv[1]);
    server.nm_port = atoi(argv[3]);
    server.client_port = atoi(argv[4]);
    server.path_count = 0;

    // Get local IP address
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    struct hostent* host_entry = gethostbyname(hostname);
    strcpy(server.ip_address, inet_ntoa(*(struct in_addr*)host_entry->h_addr_list[0]));

    // Process accessible paths from command line arguments
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
    int client_socket = initialize_socket(server.client_port);
    if (client_socket < 0) {
        printf("Failed to initialize client socket\n");
        close(nm_socket);
        return 1;
    }

    // Create thread to handle client connections
    pthread_t client_thread;
    SocketInfo client_info = {
        .socket = client_socket
    };
    if (pthread_create(&client_thread, NULL, handle_client_connections, &client_info) != 0) {
        printf("Failed to create client handling thread\n");
        close(nm_socket);
        close(client_socket);
        return 1;
    }

    printf("Storage Server %d initialized and running\n", server.server_id);

    // Main loop to keep the server running
    char buffer[BUFFER_SIZE];
    while (1) {
        // Handle naming server communications
        int bytes_read = recv(nm_socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_read <= 0) {
            printf("Connection to naming server lost\n");
            break;
        }
        buffer[bytes_read] = '\0';
        // Handle naming server messages here
    }

    // Cleanup
    close(nm_socket);
    close(client_socket);
    pthread_cancel(client_thread);
    pthread_join(client_thread, NULL);

    return 0;
}

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
    // Create registration message in JSON format
    char registration[BUFFER_SIZE];
    snprintf(registration, BUFFER_SIZE,
        "{"
        "\"server_id\": %d,"
        "\"ip_address\": \"%s\","
        "\"nm_port\": %d,"
        "\"client_port\": %d,"
        "\"path_count\": %d"
        "}",
        server->server_id,
        server->ip_address,
        server->nm_port,
        server->client_port,
        server->path_count
    );

    send(nm_socket, registration, strlen(registration), 0);

    // Send paths in separate messages to avoid buffer size issues
    for (int i = 0; i < server->path_count; i++) {
        char path_msg[MAX_PATH_LENGTH + 50];
        snprintf(path_msg, sizeof(path_msg),
            "{\"path_index\": %d, \"path\": \"%s\"}",
            i, server->accessible_paths[i]
        );
        send(nm_socket, path_msg, strlen(path_msg), 0);
    }
}

void* handle_client_connections(void* arg) {
    SocketInfo* info = (SocketInfo*)arg;
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    while (1) {
        int client_fd = accept(info->socket, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        printf("New client connected: %s:%d\n", 
            inet_ntoa(client_addr.sin_addr), 
            ntohs(client_addr.sin_port));

        // Handle client communication here
        // You would typically create a new thread for each client
        close(client_fd);
    }

    return NULL;
}

int validate_path(const char* path) {
    DIR* dir = opendir(path);
    if (dir) {
        closedir(dir);
        return 1;
    }

    // Check if it's a regular file
    FILE* file = fopen(path, "r");
    if (file) {
        fclose(file);
        return 1;
    }

    return 0;
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