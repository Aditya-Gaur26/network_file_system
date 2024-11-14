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
#include <sys/stat.h>
#include <limits.h> // Add this header for PATH_MAX
#include <stdbool.h> // Add this header for bool type


#define PATH_MAX 4096
#define MAX_PATHS 1000
#define MAX_PATH_LENGTH 256
#define BUFFER_SIZE 1024
#define MAX_CLIENTS 20
#define BASE_DIR "myserver/"
#define INVALID_PATH 10
#define MAX_READ 4096
#define CHUNK_SIZE 2000
#define DATA_SIZE_TO_SHIFT_WRITE_STYLE 10000



// Get file information (size and permissions)
typedef struct {
    off_t size;           // File size in bytes
    mode_t permissions;   // File permissions
    time_t last_modified; // Last modification time
    int is_directory;     // 1 if directory, 0 if file
} file_info_t;


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
    int client_number;
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
int nm_socket;
pthread_t client_thread;

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
int copy_path(const char* source_path, const char* dest_path);
int delete_path(const char* path) ;
int create_empty(const char* path, int is_directory);
void add_base(char **path) ;
ssize_t read_file_range(const char* path, char* buffer, size_t buffer_size,off_t offset, size_t length);

void stream_to_socket(const char* data, size_t size, void* user_data);
int stream_audio(const char* path,void (*callback)(const char* data, size_t size, void* user_data),void* user_data) ;
int get_file_info(const char* path, file_info_t* info);
int write_file_for_client(int client_fd , char*path ,int append,char* error,int write_size);
int write_file_for_client_large(ClientInfo*client , char*path ,int append,char* error,int write_size,int request_id);
ssize_t read_file_for_client(char*error,int client_fd ,char* path);
void send_client_work_ack_to_ns(ClientInfo*client,char*type,char*status,char*error,int stop,int request_id);
int is_within_base_dir(const char *path);
void send_to_naming_server(json_object*data);
void send_naming_work_to_ns(char*type,char*status,char*error,int request_id);
void  terminate_seperate_client_code();
void send_to_client(json_object*data,int client_fd);

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
    printf("|%s|\n",buffer);
    json_object* request = json_tokener_parse(buffer);
    free(buffer);
    return request;
}



int main(int argc, char* argv[]) {
    if (argc < 5) {
        printf("Usage: %s <server_id> <naming_server_ip> <naming_server_port> <client_port>\n", argv[0]);
        return 1;
    }
    printf("hello\n");

    const char availaible_paths_file_name[MAX_PATH_LENGTH] = "available_paths.txt";
    server.server_id = atoi(argv[1]);
    server.nm_port = atoi(argv[3]);
    server.client_port = atoi(argv[4]);
    server.path_count = 0;

    // get_local_ip
    strcpy(server.ip_address, get_local_ip());
    FILE *availaible_paths_file = fopen(availaible_paths_file_name, "r");
    if (!availaible_paths_file) {
        perror("Error opening file");
        return 1;
    }

    char line[MAX_PATH_LENGTH];
    while (fgets(line, sizeof(line), availaible_paths_file) && server.path_count < MAX_PATHS) {
        // Remove newline character if present
        line[strcspn(line, "\n")] = '\0';

        if (validate_path(line)) {
            strncpy(server.accessible_paths[server.path_count], line, MAX_PATH_LENGTH - 1);
            server.accessible_paths[server.path_count][MAX_PATH_LENGTH - 1] = '\0';
            server.path_count++;
        } else {
            printf("Warning: Invalid path '%s', skipping\n", line);
        }
    }
    fclose(availaible_paths_file);

    print_server_info(&server);

    // Connect to naming server
    nm_socket = connect_to_naming_server(argv[2], server.nm_port);
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
    json_object*request_code_object;
    while (1) {
        
        json_object* nm_response = receive_request(nm_socket);
        if(nm_response == NULL){
            
            terminate_seperate_client_code();
            return 0;
        }
        char*error = malloc(sizeof(char)*1000);
        strcpy(error,"No error");
        char*path = malloc(sizeof(char)*256);
        int request_id = -1;
        json_object*request_id_object;
        if(json_object_object_get_ex(nm_response,"request_code",&request_code_object)){
            const char*request_code = json_object_get_string(request_code_object);
            int response_code = 1;
            
            if(strcmp("create_empty",request_code) == 0){
                json_object*path_object;
                json_object*type_obj;
               
                bool is_directory; // 1 for directory 0 for file 
                if(json_object_object_get_ex(nm_response,"path",&path_object)){
                    strcpy(path,json_object_get_string(path_object));
                    add_base(&path);
                }
                else{
                    strcpy(error,"not able to extract path");
                    response_code = 0;
                }

                if(json_object_object_get_ex(nm_response,"type",&type_obj)){
                    is_directory = json_object_get_boolean(type_obj);
                }
                else{
                    strcpy(error,"not able to extract file type : directory or not");
                    response_code = 0;
                }

                if(json_object_object_get_ex(nm_response,"request_id",&request_id_object)){
                    request_id = json_object_get_int(request_id_object);
                }
                else{
                    strcpy(error,"not able to extract request id");
                    response_code = 0;
                }

                int res = create_empty(path,is_directory);
                if(res == -1){
                    response_code = 0;
                    strcpy(error,"path not created : may be server already has the same path");
                }
                if(res == 0){
                    send_naming_work_to_ns("naming_server_related","success",error,request_id);

                }
                else if(res == -1){
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id);

                }
                json_object_put(path_object);
                json_object_put(type_obj);
            }
            else if(strcmp("delete",request_code) == 0){
                json_object*path_object;
                
                
                if(json_object_object_get_ex(nm_response,"path",&path_object)){
                    strcpy(path,json_object_get_string(path_object));
            
                    if(!validate_path(path)){
                        response_code = 0;
                        strcpy(error,"invalid path for deletion");
                    }
                    add_base(&path);
                    
                }
                else{
                    response_code = 0;
                    strcpy(error,"not able to extract path for deletion");
                }

                if(json_object_object_get_ex(nm_response,"request_id",&request_id_object)){
                    request_id = json_object_get_int(request_id_object);
                }
                else{
                    strcpy(error,"not able to extract request id");
                    response_code = 0;
                }

                int res = delete_path(path);
                if(res == -1){
                    response_code = 0;
                    strcpy(error,"Not able to delete file ");
                }
                if(response_code == 1){
                    send_naming_work_to_ns("naming_server_related","success",error,request_id);

                }
                else {
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id);
                }
                json_object_put(path_object);

            }
            else if(strcmp("copy",request_code) == 0){
                json_object*path_object;
                json_object*storage_ip_object;
                json_object*storage_port_object;
                
                char*storage_ip;
                int storage_port;
                
                strcpy(error,"No error");
                if(json_object_object_get_ex(nm_response,"storage_path",&path_object)){
                    strcpy(path,json_object_get_string(path_object));
                }
                else{
                    response_code = 0;
                    strcpy(error,"Not able to extract path for stored file");
                }

                if(json_object_object_get_ex(nm_response,"storage_ip",&storage_ip_object)){
                    strcpy(storage_ip,json_object_get_string(storage_ip_object));
                }
                else{
                    response_code = 0;
                    strcpy(error,"Not able to extract ip for storage server");
                }
                if(json_object_object_get_ex(nm_response,"storage_port",&storage_port_object)){
                    storage_port = json_object_get_int(storage_port_object);
                }
                else{
                    response_code = 0;
                    strcpy(error,"Not able to extract port for storage server");
                }

                   if(json_object_object_get_ex(nm_response,"request_id",&request_id_object)){
                    request_id = json_object_get_int(request_id_object);
                }
                else{
                    strcpy(error,"not able to extract request id");
                    response_code = 0;
                }

                int res = 1; // implent the code with return succes as  0 and fail as -1
                if(res == -1){
                    response_code = 0;
                    strcpy(error,"unable to copy from storage server");
                }
                if(response_code == 1){
                    send_naming_work_to_ns("naming_server_related","success",error,request_id);

                }
                else {
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id);
                }
            
                json_object_put(path_object);
                json_object_put(storage_ip_object);
                json_object_put(storage_port_object);
            }
          
        }
        free(error);
        free(path);
        json_object_put(request_id_object);
        
    }
    json_object_put(request_code_object);

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

    printf("Started handling client: %s:%d\n",inet_ntoa(client->address.sin_addr),ntohs(client->address.sin_port));

    json_object* client_request = receive_request(client->client_fd);

    if(client_request == NULL){
        printf("Client disconnected: %s:%d\n",inet_ntoa(client->address.sin_addr),ntohs(client->address.sin_port));
        close(client->client_fd);
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

    json_object*request_code_object;
    if(json_object_object_get_ex(client_request,"request_code",&request_code_object)){
        const char*request_code = json_object_get_string(request_code_object);
        int response_code = 1;
        char*error = malloc(sizeof(char)*1000);
        strcpy(error,"No Error");
        char*path = malloc(sizeof(char)*256);
        if(strcmp("read",request_code) == 0){
            
            json_object*path_object;
            
            if(json_object_object_get_ex(client_request,"path",&path_object)){
                strcpy(path,json_object_get_string(path_object));
                
                if(!validate_path(path)){
                    response_code = 0;
                    strcpy(error,"Invalid path");
                }
                add_base(&path);
                
            }
            else{
                strcpy(error,"not able to extract path");
                response_code = 0;
            }

            
            int res =-1;
            if(response_code == 1){
                res = read_file_for_client(error,client->client_fd,path);
            }
            else{
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("failure"));
                json_object_object_add(response , "error",json_object_new_string(error));
                json_object_object_add(response , "stop",json_object_new_int(1));
                send_to_client(response,client->client_fd);
                json_object_put(response);
            }
            json_object_put(path_object);
        }
        else if(strcmp("write",request_code) == 0){
            json_object*path_object;
            json_object*data_object;
            json_object*append_flag_object;
            json_object*write_size_object;
            json_object*request_id_object;
            int request_id;
            int write_size = 0;
            bool append_flag=0;
            
            if(json_object_object_get_ex(client_request,"path",&path_object)){
                strcpy(path,json_object_get_string(path_object));
                
                if(!validate_path(path)){
                    response_code = 0;
                    strcpy(error,"Invalid path");
                }
                add_base(&path);
            }
            else{
                strcpy(error,"not able to extract path");
                response_code = 0;
            }
            if(json_object_object_get_ex(client_request,"append_flag",&append_flag_object)){
                append_flag = json_object_get_boolean(append_flag_object);
            }
            else{
                strcpy(error,"not able to extract append_flag");
                response_code = 0;
            }
            if(json_object_object_get_ex(client_request,"write_size",&write_size_object)){
                write_size = json_object_get_int(write_size_object);
            }
            else{
                strcpy(error,"not able to extract write size");
                response_code = 0;
            }
            if(json_object_object_get_ex(client_request,"request_id",&request_id_object)){
                request_id = json_object_get_int(request_id_object);
            }
            else{
                strcpy(error,"not able to extract request id");
                response_code = 0;
            }

            int res = -1;
            if( response_code == 0){
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("failure"));
                json_object_object_add(response , "error",json_object_new_string(error));
                json_object_object_add(response , "stop",json_object_new_int(1));
                send_to_client(response,client->client_fd);
                json_object_put(response);
            }
            else{
               if(write_size > DATA_SIZE_TO_SHIFT_WRITE_STYLE){
                    json_object*response = json_object_new_object();
                    json_object_object_add(response , "status",json_object_new_string("pending"));
                    json_object_object_add(response , "stop",json_object_new_int(1));
                    send_to_client(response,client->client_fd);
                    json_object_put(response);
                    res =  write_file_for_client_large(client,path,append_flag,error,write_size,request_id);
               }
               else res = write_file_for_client(client->client_fd,path,append_flag,error,write_size);
            }
            
            json_object_put(path_object);
            json_object_put(append_flag_object);
            json_object_put(data_object);

        }
        else if(strcmp("get_file_info",request_code) == 0){
            json_object*path_object;
                
            strcpy(error,"No error");
            if(json_object_object_get_ex(client_request,"path",&path_object)){
                strcpy(path,json_object_get_string(path_object));
                if(!validate_path(path)){
                    response_code =0;
                    strcpy(error,"Invalid Path");
                }
                add_base(&path);
            }
            else{
                response_code = 0;
                strcpy(error,"Not able to extract path for stored file");
            }

            file_info_t*info_holder = malloc(sizeof(file_info_t));
            int res = get_file_info(path,info_holder); // implent the code with return succes as  0 and fail as -1
            if(res == -1){
                response_code = 0;
                strcpy(error,"unable to fetch file info");
            }
            if(response_code == 1){
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("success"));
                json_object *file_info = json_object_new_object();
                // Add file size
                json_object_object_add(file_info, "size", json_object_new_int64(info_holder->size));
                // Add file permissions (in octal)
                json_object_object_add(file_info, "permissions", json_object_new_int(info_holder->permissions));
                // Add last modified time
                char last_modified_str[64];
                struct tm *tm_info = localtime(&info_holder->last_modified);
                strftime(last_modified_str, sizeof(last_modified_str), "%Y-%m-%d %H:%M:%S", tm_info);
                json_object_object_add(file_info, "last_modified", json_object_new_string(last_modified_str));
                // Add whether it's a directory or a file
                json_object_object_add(file_info, "is_directory", json_object_new_boolean(info_holder->is_directory));
                // Add the file info to the response
                json_object_object_add(response, "file_info", file_info);
                json_object_object_add(response,"stop",json_object_new_int(1));
                send_to_naming_server(response);
                json_object_put(response);
                json_object_put(file_info);
            }
            else {
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("failure"));
                json_object_object_add(response , "error",json_object_new_string(error));
                send_to_naming_server(response);
                json_object_put(response);
            }
            json_object_put(path_object);
        }
        else if(strcmp("stream_audio_files",request_code) == 0 ){
            
        }
        free(error);

    }
    json_object_put(request_code_object);
    

    printf("Client disconnected: %s:%d\n",inet_ntoa(client->address.sin_addr),ntohs(client->address.sin_port));

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
            usleep(10);  // Sleep for 0.01ms when max clients reached
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
        else{
            client->client_number = slot;
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
    send_to_naming_server(request);
    json_object_put(request);

    json_object* nm_response = receive_request(nm_socket);
    if (nm_response == NULL) {
        return;
    }

    json_object* status;
    if (json_object_object_get_ex(nm_response, "status", &status)) {
        const char* status_str = json_object_get_string(status);
        printf("%s\n",status_str);
        json_object* ss_id;
        if (json_object_object_get_ex(nm_response, "ss_id", &ss_id)) {
            int type = json_object_get_int(ss_id);
            printf("%d\n",type);
        }
        json_object_put(ss_id);
    }
    json_object_put(status);
    
        
}
int validate_path(const char *path) {
    char full_path[PATH_MAX];

    // Construct the full path by prepending "myserver/" to the input path
    snprintf(full_path, sizeof(full_path), "%s%s", BASE_DIR, path);

    // Attempt to open the path as a directory
    DIR *dir = opendir(full_path);
    if (dir) {
        closedir(dir);
        return 1;  // Path is a valid directory within myserver
    }

    // Attempt to open the path as a file
    FILE *file = fopen(full_path, "r");
    if (file) {
        fclose(file);
        return 1;  // Path is a valid file within myserver
    }

    return 0;  // Path is neither a valid directory nor file within myserver
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

// Create an empty file or directory
int create_empty(const char* path, int is_directory) {
    if (is_directory) {
        // Create directory with read/write/execute permissions (0755)
        if (mkdir(path, 0755) == 0) {
            return 0;  // Success
        }
    } else {
        // Create empty file
        FILE* file = fopen(path, "w");
        if (file != NULL) {
            fclose(file);
            return 0;  // Success
        }
    }
    return -1;  // Error
}

// Delete a file or directory recursively
int delete_path(const char* path) {
    struct stat path_stat;
    
    // Check if path exists
    if (stat(path, &path_stat) != 0) {
        return -1;
    }

    if (S_ISDIR(path_stat.st_mode)) {
        DIR* dir = opendir(path);
        if (dir == NULL) {
            return -1;
        }

        struct dirent* entry;
        while ((entry = readdir(dir)) != NULL) {
            // Skip . and ..
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            // Construct full path
            char full_path[PATH_MAX];
            snprintf(full_path, PATH_MAX, "%s/%s", path, entry->d_name);

            // Recursively delete contents
            if (delete_path(full_path) != 0) {
                closedir(dir);
                return -1;
            }
        }
        closedir(dir);
        return rmdir(path);  // Delete the empty directory
    } else {
        return unlink(path);  // Delete file
    }
}


// Copy file or directory from source to destination
int copy_path(const char* source_path, const char* dest_path) {
    struct stat source_stat;
    
    // Check if source exists
    if (stat(source_path, &source_stat) != 0) {
        return -1;
    }

    if (S_ISDIR(source_stat.st_mode)) {
        // Create destination directory
        if (mkdir(dest_path, 0755) != 0) {
            return -1;
        }

        DIR* dir = opendir(source_path);
        if (dir == NULL) {
            return -1;
        }

        struct dirent* entry;
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            char source_full[PATH_MAX];
            char dest_full[PATH_MAX];
            snprintf(source_full, PATH_MAX, "%s/%s", source_path, entry->d_name);
            snprintf(dest_full, PATH_MAX, "%s/%s", dest_path, entry->d_name);

            if (copy_path(source_full, dest_full) != 0) {
                closedir(dir);
                return -1;
            }
        }
        closedir(dir);
        return 0;
    } else {
        // Copy file
        FILE* source = fopen(source_path, "rb");
        if (source == NULL) {
            return -1;
        }

        FILE* dest = fopen(dest_path, "wb");
        if (dest == NULL) {
            fclose(source);
            return -1;
        }

        char buffer[8192];
        size_t bytes_read;
        while ((bytes_read = fread(buffer, 1, sizeof(buffer), source)) > 0) {
            if (fwrite(buffer, 1, bytes_read, dest) != bytes_read) {
                fclose(source);
                fclose(dest);
                return -1;
            }
        }

        fclose(source);
        fclose(dest);
        return 0;
    }
}

// Read entire file content into a buffer
// Returns: -1 on error, file size on success
// buffer is allocated by the function and must be freed by caller
ssize_t read_file_for_client(char*error,int client_fd,char* path) {
    char buffer[MAX_READ];
    FILE* file = fopen(path, "rb");
    if (file == NULL) {
        return -1;
    }

    // Get file size
    fseek(file, 0, SEEK_END);
    ssize_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    if (file_size < 0) {
        fclose(file);
        strcpy(error,"unable to get file size ");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        json_object_put(response);
        return -1;
    }
    
    int bytes_read =0;
    int chunk_size =0;
    while( ( chunk_size = fread(buffer,1,CHUNK_SIZE,file)) > 0){
        bytes_read+=chunk_size;
        buffer[chunk_size] = '\0';
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("success"));
        json_object_object_add(response,"data",json_object_new_string(buffer));
        json_object_object_add(response,"stop",json_object_new_string(0));
        json_object_object_add(response,"chunk_size",json_object_new_int(chunk_size));
        send_to_client(response,client_fd);
        json_object_put(response);
    }
    
    if (ferror(file)) {
        fclose(file);
        strcpy(error,"something wrong occurred while reading file");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        json_object_put(response);
        return -1;
    }

    if (bytes_read != file_size) {
        fclose(file);
        strcpy(error,"unable to read the file completely");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        json_object_put(response);
        return -1;
    }

    json_object*response = json_object_new_object();
    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response,"stop",json_object_new_int(0));
    send_to_client(response,client_fd);
    json_object_put(response);
    
    fclose(file);

    // Null terminate if it's text
    return file_size;
}

// Write data to file
// Returns: -1 on error, 0 on success
int write_file_for_client(int client_fd , char*path ,int append,char* error,int write_size) {
    json_object*response = json_object_new_object();
    FILE* file = fopen(path, append ? "ab" : "wb");
    if (file == NULL) {
        strcpy(error,"unabe to open file");
       
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        json_object_put(response);
        return -1;
    }

    
    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response , "stop",json_object_new_int(0));
    send_to_client(response,client_fd);
    json_object_put(response);

    json_object*stop_object;
    json_object*data_object;
    json_object*chunk_size_object;
    int total_bytes_written =0;
    int response_code =1;
    while(1){
        json_object*client_request = receive_request(client_fd);
        if(client_request == NULL){
            fclose(file);
            return -1;
        }
        char data[4096];
        int chunk_size;
        int stop;
        
        
        if(json_object_object_get_ex(client_request,"stop",&stop_object)){
            stop = json_object_get_int(stop_object);
        }
        else{
            strcpy(error ,"please send stop signal with the data packet");
        
            json_object_object_add(response , "status",json_object_new_string("failure"));
            json_object_object_add(response , "error",json_object_new_string(error));
            json_object_object_add(response , "stop",json_object_new_int(1));
            send_to_client(response,client_fd);
            json_object_put(response);
            response_code= 0;
            break;
        }

        if(stop == 1)break;

        if(json_object_object_get_ex(client_request,"chunk_size",&chunk_size_object)){
            chunk_size =json_object_get_int(chunk_size_object);
        }
        else{
            strcpy(error ,"please send chunnk size with the data packet");
            
            json_object_object_add(response , "status",json_object_new_string("failure"));
            json_object_object_add(response , "error",json_object_new_string(error));
            json_object_object_add(response , "stop",json_object_new_int(1));
            send_to_client(response,client_fd);
            json_object_put(response);
            response_code =0;
            break;
        }

      
        if(json_object_object_get_ex(client_request,"data",&data_object)){
            strcpy(data,json_object_get_string(data_object));
        }
        else{
            strcpy(error ,"please send chunk size with the data packet");
           
            json_object_object_add(response , "status",json_object_new_string("failure"));
            json_object_object_add(response , "error",json_object_new_string(error));
            json_object_object_add(response , "stop",json_object_new_int(1));
            send_to_client(response,client_fd);
            json_object_put(response);
            response_code=0;
            break;
        }

        if(chunk_size > CHUNK_SIZE || strlen(data) != chunk_size){
            if(chunk_size != strlen(data) ){
                strcpy(error,"chunk size does not matches length of the data");
            }
            else strcpy(error ,"chunk size cannot exceed 2000");
           
            json_object_object_add(response , "status",json_object_new_string("failure"));
            json_object_object_add(response , "error",json_object_new_string(error));
            json_object_object_add(response , "stop",json_object_new_int(1));
            send_to_client(response,client_fd);
            json_object_put(response);
            response_code= 0;
            break;
        }


        int bytes_written = fwrite(data,1,chunk_size,file);

        if(bytes_written != chunk_size){
            strcpy(error,"some error occurred while writing to the file");
            
            json_object_object_add(response , "status",json_object_new_string("failure"));
            json_object_object_add(response , "error",json_object_new_string(error));
            json_object_object_add(response , "stop",json_object_new_int(1));
            send_to_client(response,client_fd);
            json_object_put(response);
            response_code=0;
            break;
        }
        total_bytes_written+=bytes_written;
        
    }
    json_object_put(stop_object);
    json_object_put(data_object);
    json_object_put(chunk_size_object);

    if(response_code == 0){
        return -1;
    }

    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response , "stop",json_object_new_int(1));
    json_object_object_add(response, "total_bytes_written",json_object_new_int(total_bytes_written));
    send_to_client(response,client_fd);
    json_object_put(response);
    fclose(file);

    return 1;
}

int write_file_for_client_large(ClientInfo*client , char*path ,int append,char* error,int write_size,int request_id){
    int client_fd = client->client_fd;
    FILE* file = fopen(path, append ? "ab" : "wb");
    if (file == NULL) {
        strcpy(error,"unabe to open file");
        send_client_work_ack_to_ns(client,"client_related","failure",error,1,request_id);
        return -1;
    }

    json_object*stop_object;
    json_object*data_object;
    json_object*chunk_size_object;
    int total_bytes_written =0;
    int actual_total_bytes_written=0;
    char large_data[1000000];
    char data[4096];
    while(1){
        json_object*client_request = receive_request(client_fd);
        if(client_request == NULL){
            fclose(file);
            return -1;
        }
        
        int chunk_size;
        int stop;
        int response_code =1;
        
        if(json_object_object_get_ex(client_request,"stop",&stop_object)){
            stop = json_object_get_int(stop_object);
        }
        else{
            strcpy(error ,"please send stop signal with the data packet");
            send_client_work_ack_to_ns(client,"client_related","failure",error,1,request_id);

            break;
        }

        if(stop == 1)break;

        if(json_object_object_get_ex(client_request,"chunk_size",&chunk_size_object)){
            chunk_size =json_object_get_int(chunk_size_object);
        }
        else{
            strcpy(error ,"please send chunk size with the data packet");
            send_client_work_ack_to_ns(client,"client_related","failure",error,1,request_id);
            break;
        }

      
        if(json_object_object_get_ex(client_request,"data",&data_object)){
            strcpy(data,json_object_get_string(data_object));
        }
        else{
            strcpy(error ,"please send chunk size with the data packet");
            send_client_work_ack_to_ns(client,"client_related","failure",error,1,request_id);
            break;
        }

        if(chunk_size > CHUNK_SIZE || strlen(data) != chunk_size){
            if(chunk_size != strlen(data) ){
                strcpy(error,"chunk size does not matches length of the data");
            }
            else strcpy(error ,"chunk size cannot exceed 2000");
            
            send_client_work_ack_to_ns(client,"client_related","failure",error,1,request_id);
            break;
        }


        strcat(large_data,data);
        total_bytes_written+=strlen(data);
        actual_total_bytes_written+=strlen(data);

        if(total_bytes_written > 1000000 - 10000){
            fwrite(large_data,1,total_bytes_written,file);
            total_bytes_written=0;
            large_data[0]='\0';
        }
       
    }

    if(total_bytes_written > 0 ) fwrite(large_data,1,total_bytes_written,file);


    send_client_work_ack_to_ns(client,"client_related","success",error,1,request_id);
  
    json_object_put(stop_object);
    json_object_put(data_object);
    json_object_put(chunk_size_object);

    
    fclose(file);

    return 1;
}

// Returns: -1 on error, 0 on success
int get_file_info(const char* path, file_info_t* info) {
    struct stat st;
    
    if (stat(path, &st) == -1) {
        return -1;
    }

    info->size = st.st_size;
    info->permissions = st.st_mode & 0777;  // Only permission bits
    info->last_modified = st.st_mtime;
    info->is_directory = S_ISDIR(st.st_mode);

    return 0;
}

// Stream audio file in chunks
// callback is called for each chunk of data read
// Returns: -1 on error, 0 on success
// int stream_audio(const char* path, void (*callback)(const char* data, size_t size, void* user_data),void* user_data) {
//     FILE* file = fopen(path, "rb");
//     if (file == NULL) {
//         return -1;
//     }

//     // Use a reasonable buffer size for streaming (e.g., 64KB)
//     const size_t CHUNK_SIZE = 64 * 1024;
//     char* buffer = (char*)malloc(CHUNK_SIZE);
//     if (buffer == NULL) {
//         fclose(file);
//         return -1;
//     }

//     size_t bytes_read;
//     while ((bytes_read = fread(buffer, 1, CHUNK_SIZE, file)) > 0) {
//         callback(buffer, bytes_read, user_data);
//     }

//     free(buffer);
//     fclose(file);
//     return 0;
// }

// Example callback function for streaming to a socket
void stream_to_socket(const char* data, size_t size, void* user_data) {
    int socket_fd = *(int*)user_data;
    
    // First send the chunk size
    uint32_t chunk_size = htonl(size);
    send(socket_fd, &chunk_size, sizeof(chunk_size), 0);
    
    // Then send the chunk data
    send(socket_fd, data, size, 0);
}

// Read file in range (for partial reads/resume support)
// Returns: -1 on error, bytes read on success
ssize_t read_file_range(const char* path, char* buffer, size_t buffer_size,off_t offset, size_t length) {
    FILE* file = fopen(path, "rb");
    if (file == NULL) {
        return -1;
    }

    // Seek to offset
    if (fseek(file, offset, SEEK_SET) != 0) {
        fclose(file);
        return -1;
    }

    // Read requested length or up to buffer size
    size_t to_read = (length < buffer_size) ? length : buffer_size;
    size_t bytes_read = fread(buffer, 1, to_read, file);
    
    fclose(file);
    return bytes_read;
}
// int is_path_creatable(const char *path) {
//     return strncmp(path, BASE_DIR, strlen(BASE_DIR)) == 0;
// }

void send_to_naming_server(json_object*data){
    const char* str = json_object_to_json_string(data);
    uint32_t length = strlen(str);
    uint32_t network_length = htonl(length);
    
    send(nm_socket, &network_length, sizeof(network_length), 0);
    send(nm_socket, str, length, 0);
}

void send_to_client(json_object*data,int client_fd){
    const char* str = json_object_to_json_string(data);
    uint32_t length = strlen(str);
    uint32_t network_length = htonl(length);
    
    send(client_fd, &network_length, sizeof(network_length), 0);
    send(client_fd, str, length, 0);
}

void add_base(char **path) {
    // Calculate the required length for the new path
    size_t base_length = strlen(BASE_DIR);
    size_t path_length = strlen(*path);
    size_t full_length = base_length + path_length + 1; // +1 for the null terminator

    // Allocate memory for the new full path
    char *full_path = (char *)malloc(full_length);
    if (!full_path) {
        fprintf(stderr, "Error: Memory allocation failed\n");
        return; // If allocation fails, don't modify the pointer
    }

    // Construct the full path by copying BASE_DIR and appending the original path
    strcpy(full_path, BASE_DIR);
    strcat(full_path, *path);

    // Free the original path if it was dynamically allocated (optional)
    free(*path);

    // Update the original pointer to point to the new full path
    *path = full_path;
}
void  terminate_seperate_client_code(){
    close(client_socket);
    pthread_cancel(client_thread);

}

void send_client_work_ack_to_ns(ClientInfo*client,char*type,char*status,char*error,int stop,int request_id){
    json_object*response = json_object_new_object();
    json_object_object_add(response , "type",json_object_new_string("client_related"));
    json_object_object_add(response,"client_ip",json_object_new_string(inet_ntoa(client->address.sin_addr)));
    json_object_object_add(response,"client_port",json_object_new_int((int)ntohs(client->address.sin_port)));
    json_object_object_add(response,"client_request_id",json_object_new_int(request_id));
    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response , "error",json_object_new_string(error));
    json_object_object_add(response , "stop",json_object_new_int(1));
    send_to_naming_server(response);
    json_object_put(response);
}

void send_naming_work_to_ns(char*type,char*status,char*error,int request_id){
    json_object*response = json_object_new_object();
    json_object_object_add(response , "type",json_object_new_string(type));
    json_object_object_add(response , "status",json_object_new_string(status));
    json_object_object_add(response , "error",json_object_new_string(error));
    if(request_id > 0)json_object_object_add(response , "request_id",json_object_new_int(request_id));
    send_to_naming_server(response);
    json_object_put(response);
}