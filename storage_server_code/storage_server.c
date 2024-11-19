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
#include <ctype.h>

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
#define AUDIO_CHUNK_SIZE 4096


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
    int client_id;
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
int SS_ID = -1;
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
void send_naming_work_to_ns(char*type,char*status,char*error,int request_id,int client_id,char*operation,char*path,int ss_id);
void  terminate_seperate_client_code();
void send_to_client(json_object*data,int client_fd);
bool is_audio_file(const char* path);
char* base64_encode(const unsigned char* data, size_t length) ;
int copy_from_another_storage_server(char*storage,int storage_port,char*path,char*destination_path);
int simple_write_to_file(char*buffer,char*path,int append);
void add_paths_recursively(char* base_path, char* current_path, json_object** paths_array_ptr,json_object**types_array_ptr);  
void ensure_path_exists(char* path,int is_directory);
int is_path_in_current_directory(char* given_path);
void print_binary(const unsigned char *data, size_t length);

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
    FILE*config_file = fopen("configuration.txt","r");
    if(config_file == NULL){
        printf("unable to open configuration file\n");
    }
    char ss_id_array[100];
    if(fgets(ss_id_array,sizeof(ss_id_array),config_file)){
        SS_ID = atoi(ss_id_array);
    }
    fclose(config_file);

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
    pthread_detach(client_thread);

    printf("Storage Server %d initialized and running\n", server.server_id);

    // Main loop
    char buffer[BUFFER_SIZE];
    json_object*request_code_object;
    json_object*client_id_object;
    int client_id=-1;
    while (1) {
        
        json_object* nm_response = receive_request(nm_socket);
        if(nm_response == NULL){
            
            return 0;
        }
        
        char*error = malloc(sizeof(char)*1000);
        strcpy(error,"No error");
        char*path = malloc(sizeof(char)*256);
        int request_id = -1;
        json_object*request_id_object;
        int response_code = 1;
       

        if(json_object_object_get_ex(nm_response,"request_code",&request_code_object)){
            const char*request_code = json_object_get_string(request_code_object);
            int response_code = 1;
            
            if(strcmp("create_empty",request_code) == 0){
                json_object*path_object;
                json_object*type_obj;
               
                bool is_directory; // 1 for directory 0 for file 

                if(json_object_object_get_ex(nm_response,"client_id",&client_id_object)){
                    client_id = json_object_get_int(client_id_object);
                }
                else{
                    strcpy(error,"not able to extract client id");
                    response_code=0;
                }

                if(json_object_object_get_ex(nm_response,"path",&path_object)){
                    strcpy(path,json_object_get_string(path_object));
                    // add_base(&path);
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
                if(res == 0){
                    response_code = 0;
                    strcpy(error,"path not created : may be server already has the same path");
                }
                if(res == 1){
                    send_naming_work_to_ns("naming_server_related","success",error,request_id,client_id,"create_empty",path,-1);

                }
                else if(res == -1){
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id,client_id,"create_empty",path,-1);

                }
                // json_object_put(path_object);
                // json_object_put(type_obj);
            }
            else if(strcmp("delete",request_code) == 0){
                json_object*path_object;
                
                if(json_object_object_get_ex(nm_response,"client_id",&client_id_object)){
                    client_id = json_object_get_int(client_id_object);
                }
                else{
                    strcpy(error,"not able to extract client id");
                    response_code=0;
                }
                
                if(json_object_object_get_ex(nm_response,"path",&path_object)){
                    strcpy(path,json_object_get_string(path_object));
            
                    // if(!validate_path(path)){
                    //     response_code = 0;
                    //     strcpy(error,"invalid path for deletion");
                    // }
                    // add_base(&path);
                    if(!is_path_in_current_directory(path)){
                        strcpy(error,"Invalid path");
                        response_code  = 0;
                    }                    
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
                    send_naming_work_to_ns("naming_server_related","success",error,request_id,client_id,"delete",path,-1);

                }
                else {
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id,client_id,"delete",path,-1);
                }
                // // json_object_put(path_object);

            }
            else if(strcmp("copy",request_code) == 0){
                json_object*path_object;
                json_object*storage_ip_object;
                json_object*storage_port_object;
                json_object*destination_path_object;
                json_object*storage_ss_id_object;
                char*storage_ip = malloc(sizeof(char)*40);
                int storage_port;
                int storage_ss_id;
                char*destination_path = malloc(sizeof(char)*256);
                
                if(json_object_object_get_ex(nm_response,"client_id",&client_id_object)){
                    client_id = json_object_get_int(client_id_object);
                }
                else{
                    strcpy(error,"not able to extract client id");
                    response_code=0;
                }

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
                if(json_object_object_get_ex(nm_response,"destination_path",&destination_path_object)){
                    strcpy(destination_path,json_object_get_string(destination_path_object));
                    // if(!validate_path(destination_path)){
                    //     strcpy(error,"destination path not valid");
                    //     response_code =0;
                    // }
                    add_base(&destination_path);
                }

                if(json_object_object_get_ex(nm_response,"request_id",&request_id_object)){
                    request_id = json_object_get_int(request_id_object);
                }
                else{
                    strcpy(error,"not able to extract request id");
                    response_code = 0;
                }

                if(json_object_object_get_ex(nm_response,"ssid",&storage_ss_id_object)){
                    storage_ss_id = json_object_get_int(storage_ss_id_object);
                }
                else{
                    strcpy(error,"not able to extract storage server's ss_id");
                    response_code = 0;
                }

                int res = copy_from_another_storage_server(storage_ip,storage_port,path,destination_path);
                if(res == -1){
                    response_code = 0;
                    strcpy(error,"unable to copy from storage server");
                }
                if(response_code == 1){
                    send_naming_work_to_ns("naming_server_related","success",error,request_id,client_id,"copy",path,storage_ss_id);

                }
                else {
                    send_naming_work_to_ns("naming_server_related","failure",error,request_id,client_id,"copy",path,storage_ss_id);
                }
            
                // // json_object_put(path_object);
                // // json_object_put(storage_ip_object);
                // // json_object_put(storage_port_object);
                // // json_object_put(destination_path_object);

                free(storage_ip);
                free(destination_path);
            }
            else if(strcmp("list_all",request_code) == 0){
                json_object* response = json_object_new_object();
                json_object* paths_array = json_object_new_array();
                json_object* types_array = json_object_new_array();
                json_object*list_path_object;
                char list_path[1024];
                if(json_object_object_get_ex(nm_response,"path",&list_path_object)){
                    strcpy(list_path,json_object_get_string(list_path_object));
                    // if(!directory_exists(list_path)){
                    //     response_code=0;
                    //     strcpy(error,"root directory not valid");
                    // }
                }
                else{
                    strcpy(error,"not able to extract path");
                    response_code =0;
                }
                
                if(response_code = 1){
                    add_paths_recursively(list_path,list_path, &paths_array,&types_array);
                    json_object_object_add(response, "status", json_object_new_string("success"));
                    json_object_object_add(response, "paths", paths_array);
                    json_object_object_add(response, "types", types_array);
                    
                    send_to_naming_server(response);
                    // // json_object_put(response);
                }
                else{
                    json_object_object_add(response, "status", json_object_new_string("failure"));
                    json_object_object_add(response, "error", json_object_new_string(error));
                    
                    send_to_naming_server(response);
                    // // json_object_put(response);
                }
               
            }
            else if(strcmp("replica",request_code) == 0){
                json_object*parent_directory_object;
                json_object*paths_array_object;
                json_object*types_array_object;
                char parent_directory[1024];
                if(json_object_object_get_ex(nm_response,"parent_directory",&parent_directory_object)){
                    strcpy(parent_directory,json_object_get_string(parent_directory_object));
                }
                else{
                    response_code = 0;
                    strcpy(error,"not able to get parent directory");
                }

                if(!json_object_object_get_ex(nm_response,"paths",&paths_array_object) || !json_object_object_get_ex(nm_response,"types",&types_array_object) ){
                    response_code= 0;
                    strcpy(error,"not able to get paths");
                }
            }
          
        }
        free(error);
        free(path);
        // // json_object_put(request_id_object);
        
    }
    // // json_object_put(request_code_object);
    // // json_object_put(client_id_object);

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
    json_object*client_id_object;
    int client_id=-1;
    if(json_object_object_get_ex(client_request,"request_code",&request_code_object)){
        const char*request_code = json_object_get_string(request_code_object);
        int response_code = 1;
        char*error = malloc(sizeof(char)*1000);
        strcpy(error,"No Error");
        char*path = malloc(sizeof(char)*256);
        if(strcmp("read",request_code) == 0){
            
            if(json_object_object_get_ex(client_request,"client_id",&client_id_object)){
                client_id = json_object_get_int(client_id_object);
                 client->client_id =  client_id;
            }
            else{
                strcpy(error,"not able to extract client id");
                response_code=0;
            }

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
                // // json_object_put(response);
            }
            // // json_object_put(path_object);
        }
        else if(strcmp("write",request_code) == 0){
            json_object*path_object;
            json_object*data_object;
            json_object*append_flag_object;
            json_object*write_size_object;
            json_object*request_id_object;
            json_object*sync_object;
            int sync;
            int request_id;
            int write_size = 0;
            bool append_flag=0;

            if(json_object_object_get_ex(client_request,"client_id",&client_id_object)){
                client_id = json_object_get_int(client_id_object);
                client->client_id =  client_id;
            }
            else{
                strcpy(error,"not able to extract client id");
                response_code=0;
            }
            
            if(json_object_object_get_ex(client_request,"path",&path_object)){
                strncpy(path, json_object_get_string(path_object), 255);
                path[255] = '\0'; // Null-terminate explicitly
                
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
                append_flag = json_object_get_int(append_flag_object);
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
            if(json_object_object_get_ex(client_request,"sync",&sync_object)){
                sync = json_object_get_int(sync_object);
            }
            else{
                strcpy(error,"not able to extract sync flag");
                response_code=0;
            }

            int res = -1;
            if( response_code == 0){
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("failure"));
                json_object_object_add(response , "error",json_object_new_string(error));
                json_object_object_add(response , "stop",json_object_new_int(1));
                send_to_client(response,client->client_fd);
                // // json_object_put(response);
            }
            else{
               if(write_size > DATA_SIZE_TO_SHIFT_WRITE_STYLE && !sync ){
                    json_object*response = json_object_new_object();
                    json_object_object_add(response , "status",json_object_new_string("pending"));
                    json_object_object_add(response , "stop",json_object_new_int(1));
                    send_to_client(response,client->client_fd);
                    // // json_object_put(response);
                    res = write_file_for_client_large(client,path,append_flag,error,write_size,request_id);
                }
               else res = write_file_for_client(client->client_fd,path,append_flag,error,write_size);
            }
          
            // if (path_object) // json_object_put(path_object);
            // if (append_flag_object) // json_object_put(append_flag_object);
            // if (data_object) // json_object_put(data_object);
            // if (sync_object) // json_object_put(sync_object);
            // if (request_id_object) // json_object_put(request_id_object);
            // if (write_size_object) // json_object_put(write_size_object);

           

        }
        else if(strcmp("get_file_info",request_code) == 0){
            json_object*path_object;

                
            if(json_object_object_get_ex(client_request,"client_id",&client_id_object)){
                client_id = json_object_get_int(client_id_object);
                client->client_id =  client_id;
            }
            else{
                strcpy(error,"not able to extract client id");
                response_code=0;
            }

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
                json_object_object_add(file_info, "file_name", json_object_new_string(strrchr(path,'/')+ 1));
                json_object_object_add(file_info, "file_path", json_object_new_string(strchr(path,'/')+1) );
                // Add the file info to the response
                json_object_object_add(response, "file_info", file_info);
                json_object_object_add(response,"stop",json_object_new_int(1));
                send_to_client(response,client->client_fd);
                // // json_object_put(response);
                // // json_object_put(file_info);
            }
            else{
                json_object*response = json_object_new_object();
                json_object_object_add(response , "status",json_object_new_string("failure"));
                json_object_object_add(response , "error",json_object_new_string(error));
                send_to_client(response,client->client_fd);
                // // json_object_put(response);
            }
            // // json_object_put(path_object);
        }
        else if(strcmp("stream",request_code) == 0){
            json_object *path_object;

            if(json_object_object_get_ex(client_request,"client_id",&client_id_object)){
                client_id = json_object_get_int(client_id_object);
                client->client_id =  client_id;
            }
            else{
                strcpy(error,"not able to extract client id");
                response_code=0;
            }

            if(json_object_object_get_ex(client_request, "path", &path_object)) {
                strcpy(path, json_object_get_string(path_object));
                
                if(!validate_path(path)) {
                    response_code = 0;
                    strcpy(error, "Invalid path");
                }
                add_base(&path);
                
                // Check if file exists and is an audio file
                if(response_code == 1) {
                    // Get file extension
                    char *ext = strrchr(path, '.');
                    if(ext == NULL) {
                        response_code = 0;
                        strcpy(error, "File has no extension");
                    } else {
                        // Convert extension to lowercase for comparison
                        for(int i = 0; ext[i]; i++) {
                            ext[i] = tolower(ext[i]);
                        }
                        
                        // Check if it's an audio file
                        if(strcmp(ext, ".mp3") != 0 && 
                        strcmp(ext, ".wav") != 0 && 
                        strcmp(ext, ".ogg") != 0 && 
                        strcmp(ext, ".aac") != 0) {
                            response_code = 0;
                            strcpy(error, "Not an audio file");
                        }
                    }
                }
                
                // Stream the audio file if everything is valid
                if(response_code == 1) {
                    FILE *audio_file = fopen(path, "rb");
                    if(audio_file == NULL) {
                        response_code = 0;
                        strcpy(error, "Unable to open audio file");
                    } else {
                        // Send success response first
                        json_object *response = json_object_new_object();
                        json_object_object_add(response, "status", json_object_new_string("success"));
                        json_object_object_add(response, "message", json_object_new_string("Starting audio stream"));
                        json_object_object_add(response, "stop", json_object_new_int(0));
                        send_to_client(response, client->client_fd);
                        // // json_object_put(response);
                        
                        // Stream the file in chunks
                        char temp_buffer[1024];
                        char buffer[17640];
                        int total_bytes_read = 0;
                        size_t bytes_read;
                        

                        while ((bytes_read = fread(buffer, 1, sizeof(buffer), audio_file)) > 0) {

                            char *base64_data = NULL;
                            size_t base64_len = 0;
                            base64_data = base64_encode((unsigned char*)buffer, bytes_read);
                            total_bytes_read+=bytes_read;
                            // Create JSON response
                            json_object *chunk_response = json_object_new_object();
                            json_object_object_add(chunk_response, "status", json_object_new_string("streaming"));
                            json_object_object_add(chunk_response, "data", json_object_new_string(base64_data));
                            json_object_object_add(chunk_response, "bytes", json_object_new_int(bytes_read));
                            json_object_object_add(chunk_response, "stop", json_object_new_int(0));
                            json_object_object_add(chunk_response, "total_bytes_read", json_object_new_int(total_bytes_read));

                            // Send to client
                            send_to_client(chunk_response, client->client_fd);

                            // Clean up
                            free(base64_data);
                         
                            
                        }
                    }

                     
                }
                
                if(response_code == 0) {
                    // Send error response
                    json_object *response = json_object_new_object();
                    json_object_object_add(response, "status", json_object_new_string("failure"));
                    json_object_object_add(response, "error", json_object_new_string(error));
                    json_object_object_add(response, "stop", json_object_new_int(1));
                    send_to_client(response, client->client_fd);
                    // // json_object_put(response);
                }
                else{
                    json_object *response = json_object_new_object();
                    json_object_object_add(response, "status", json_object_new_string("success"));
                    json_object_object_add(response, "error", json_object_new_string(error));
                    json_object_object_add(response, "stop", json_object_new_int(1));
                    send_to_client(response, client->client_fd);

                }

                
                // json_object_put(path_object);
            } else {
                json_object *response = json_object_new_object();
                json_object_object_add(response, "status", json_object_new_string("failure"));
                json_object_object_add(response, "error", json_object_new_string("Unable to extract path"));
                json_object_object_add(response, "stop", json_object_new_int(1));
                send_to_client(response, client->client_fd);
                // // json_object_put(response);
            }
        }
        else if(strcmp("list_all",request_code) == 0){
            json_object* response = json_object_new_object();
            json_object* paths_array = json_object_new_array();
            json_object* types_array = json_object_new_array();
            json_object*list_path_object;
            char list_path[1024];
            if(json_object_object_get_ex(client_request,"path",&list_path_object)){
                strcpy(list_path,json_object_get_string(list_path_object));
                // if(!directory_exists(list_path)){
                //     response_code=0;
                //     strcpy(error,"root directory not valid");
                // }
            }
          
            
            add_paths_recursively(list_path,list_path, &paths_array,&types_array);
            
            json_object_object_add(response, "status", json_object_new_string("success"));
            json_object_object_add(response, "paths", paths_array);
            json_object_object_add(response, "types", types_array);

            send_to_client(response,client->client_fd);
            // // json_object_put(response);
            // // json_object_put(paths_array);
            // // json_object_put(types_array);
        }
        free(error);

    }
    // // json_object_put(request_code_object);

       
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
        pthread_detach(client_threads[slot]);

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
    printf("%d\n",sock_fd);

    return sock_fd;
}

void register_with_naming_server(StorageServer* server, int nm_socket) {
   json_object *request = json_object_new_object();

   
    // Add the fields to the JSON object
    json_object_object_add(request, "type", json_object_new_string("storage_server_register"));
    json_object_object_add(request, "ip", json_object_new_string(server->ip_address));
    json_object_object_add(request, "nm_port", json_object_new_int(server->nm_port));
    json_object_object_add(request, "client_port", json_object_new_int(server->client_port));
    json_object_object_add(request,"ss_id",json_object_new_int(SS_ID));

    // Create the "accessible_paths" array and add paths to it
    json_object *paths_array = json_object_new_array();
    for (int i = 0; i < server->path_count; i++) {
        json_object_array_add(paths_array, json_object_new_string(server->accessible_paths[i]));
    }
    json_object_object_add(request, "paths", paths_array);

    // Convert the JSON object to a string for sending
   
    // Print the JSON string for debugging
    send_to_naming_server(request);
    // json_object_put(request);

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
            SS_ID = json_object_get_int(ss_id);
            printf("%d\n",SS_ID);
            FILE*config_file = fopen("configuration.txt","w");
            fprintf(config_file,"%d",SS_ID);
            fclose(config_file);
        }
        // json_object_put(ss_id);
    }
    // json_object_put(status);
    
        
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
    char tmp[256];
    char* p = NULL;
    size_t len;
    struct stat path_stat;
    int created = 0;
    
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    
    // Remove trailing slash if exists
    if (len > 0 && tmp[len - 1] == '/') {
        tmp[len - 1] = 0;
        len--;
    }
    
    // If it's a file path, we'll create directories only up to the last slash
    if (!is_directory) {
        char* last_slash = strrchr(tmp, '/');
        if (last_slash) {
            *last_slash = 0;  // Temporarily cut off the filename
            
            // Create each directory in the path
            for (p = tmp + 1; p < last_slash; p++) {
                if (*p == '/') {
                    *p = 0;
                    if (stat(tmp, &path_stat) != 0) {
                        if (mkdir(tmp, 0755) == 0) {
                            created = 1;
                        }
                    }
                    *p = '/';
                }
            }
            
            // Create the final directory before the file
            if (stat(tmp, &path_stat) != 0) {
                if (mkdir(tmp, 0755) == 0) {
                    created = 1;
                }
            }
            
            *last_slash = '/';  // Restore the full path
            
            // Check if file already exists
            if (stat(tmp, &path_stat) == 0) {
                return 0;  // File exists
            }
            
            // Create empty file
            FILE* f = fopen(tmp, "w");
            if (f) {
                fclose(f);
                return 1;  // Successfully created
            }
            return 0;  // Failed to create file
        }
    } else {
        // It's a directory path, create all directories
        for (p = tmp + 1; *p; p++) {
            if (*p == '/') {
                *p = 0;
                if (stat(tmp, &path_stat) != 0) {
                    if (mkdir(tmp, 0755) == 0) {
                        created = 1;
                    }
                }
                *p = '/';
            }
        }
        
        // Create/check the final directory
        if (stat(tmp, &path_stat) == 0) {
            return 0;  // Directory already exists
        }
        
        if (mkdir(tmp, 0755) == 0) {
            return 1;  // Successfully created
        }
    }
    
    return created; 
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

    // printf("1\n");
    if (file_size < 0) {
        fclose(file);
        strcpy(error,"unable to get file size ");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        // json_object_put(response);
        return -1;
    }
    else if(file_size == 0){
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("success"));
        json_object_object_add(response,"data",json_object_new_string(""));
        json_object_object_add(response,"stop",json_object_new_int(0));
        json_object_object_add(response,"chunk_size",json_object_new_int(0));
        send_to_client(response,client_fd);
    }
    // printf("2\n");
    int bytes_read =0;
    int chunk_size =0;
    while( ( chunk_size = fread(buffer,1,CHUNK_SIZE,file)) > 0){
        bytes_read+=chunk_size;
        buffer[chunk_size] = '\0';
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("success"));
        json_object_object_add(response,"data",json_object_new_string(buffer));
        json_object_object_add(response,"stop",json_object_new_int(0));
        json_object_object_add(response,"chunk_size",json_object_new_int(chunk_size));
        send_to_client(response,client_fd);
        // json_object_put(response);
    }
    // printf("3\n");
    
    if (ferror(file)) {
        fclose(file);
        strcpy(error,"something wrong occurred while reading file");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        // json_object_put(response);
        return -1;
    }

    // printf("4\n");
    if (bytes_read != file_size) {
        fclose(file);
        strcpy(error,"unable to read the file completely");
        json_object*response = json_object_new_object();
        json_object_object_add(response , "status",json_object_new_string("failure"));
        json_object_object_add(response , "error",json_object_new_string(error));
        json_object_object_add(response , "stop",json_object_new_int(1));
        send_to_client(response,client_fd);
        // json_object_put(response);
        return -1;
    }
    // printf("5\n");

    json_object*response = json_object_new_object();
    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response,"stop",json_object_new_int(1));
    send_to_client(response,client_fd);
    // json_object_put(response);
    
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
        // json_object_put(response);
        return -1;
    }

    
    json_object_object_add(response , "status",json_object_new_string("success"));
    json_object_object_add(response , "stop",json_object_new_int(0));
    send_to_client(response,client_fd);
    // json_object_put(response);

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
            // json_object_put(response);
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
            // json_object_put(response);
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
            // json_object_put(response);
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
            // json_object_put(response);
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
            // json_object_put(response);
            response_code=0;
            break;
        }
        total_bytes_written+=bytes_written;
        
    }

  
    // json_object_put(stop_object);
    // json_object_put(data_object);
    // json_object_put(chunk_size_object);

    if(response_code == 0){
        fclose(file);
        return -1;
    }
    else{
        json_object_object_add(response , "status",json_object_new_string("success"));
        json_object_object_add(response , "stop",json_object_new_int(1));
        json_object_object_add(response, "total_bytes_written",json_object_new_int(total_bytes_written));
        send_to_client(response,client_fd);
        // json_object_put(response);
    }

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
  
    // json_object_put(stop_object);
    // json_object_put(data_object);
    // json_object_put(chunk_size_object);

    
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
    printf("sending to naming server |%s|\n",str);
    send(nm_socket, &network_length, sizeof(network_length), 0);
    send(nm_socket, str, length, 0);
}

void send_to_client(json_object*data,int client_fd){
    const char* str = json_object_to_json_string(data);
    uint32_t length = strlen(str);
    uint32_t network_length = htonl(length);
    printf("sending to client |%s|\n",str);
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
    json_object_object_add(response,"client_id",json_object_new_int(client->client_id));
    json_object_object_add(response , "status",json_object_new_string(status));
    json_object_object_add(response , "error",json_object_new_string(error));
    json_object_object_add(response , "stop",json_object_new_int(1));
    send_to_naming_server(response);
    // json_object_put(response);
}

void send_naming_work_to_ns(char*type,char*status,char*error,int request_id,int client_id , char*operation,char*path,int ss_id){
    json_object*response = json_object_new_object();
    json_object_object_add(response , "client_id",json_object_new_int(client_id));
    json_object_object_add(response , "type",json_object_new_string(type));
    json_object_object_add(response , "status",json_object_new_string(status));
    json_object_object_add(response , "error",json_object_new_string(error));
    json_object_object_add(response , "operation",json_object_new_string(operation));
    json_object_object_add(response , "path",json_object_new_string(path));
    if(request_id > 0)json_object_object_add(response , "request_id",json_object_new_int(request_id));
    if(ss_id > 0) json_object_object_add(response,"ssid",json_object_new_int(ss_id));
    send_to_naming_server(response);
    // json_object_put(response);
}


bool is_audio_file(const char* path) {
    const char* ext = strrchr(path, '.');
    if(ext == NULL) return false;
    
    // Convert to lowercase for comparison
    char ext_lower[10] = {0};
    int i;
    for(i = 0; ext[i] && i < 9; i++) {
        ext_lower[i] = tolower(ext[i]);
    }
    
    return (strcmp(ext_lower, ".mp3") == 0 ||
            strcmp(ext_lower, ".wav") == 0 ||
            strcmp(ext_lower, ".ogg") == 0 ||
            strcmp(ext_lower, ".aac") == 0);
}


// Base64 Encoding Table
static const char base64_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// Function to encode binary data to Base64
char* base64_encode(const unsigned char* data, size_t length) {
    size_t encoded_length = 4 * ((length + 2) / 3);
    char* encoded_data = (char*)malloc(encoded_length + 1);
    if (encoded_data == NULL) {
        return NULL; // Memory allocation failed
    }

    size_t i, j;
    for (i = 0, j = 0; i < length;) {
        uint32_t octet_a = i < length ? data[i++] : 0;
        uint32_t octet_b = i < length ? data[i++] : 0;
        uint32_t octet_c = i < length ? data[i++] : 0;

        uint32_t triple = (octet_a << 16) + (octet_b << 8) + octet_c;

        encoded_data[j++] = base64_table[(triple >> 18) & 0x3F];
        encoded_data[j++] = base64_table[(triple >> 12) & 0x3F];
        encoded_data[j++] = base64_table[(triple >> 6) & 0x3F];
        encoded_data[j++] = base64_table[triple & 0x3F];
    }

    // Handle padding
    for (size_t i = 0; i < (encoded_length - j); ++i) {
        encoded_data[encoded_length - 1 - i] = '=';
    }

    encoded_data[encoded_length] = '\0'; // Null-terminate the Base64 string
    return encoded_data;
}
int copy_from_another_storage_server(char*storage_ip,int storage_port,char*path,char*destination_path){

    int sock_fd, bytes_received, total_bytes = 0;
    struct sockaddr_in server_addr;
    FILE* dest_file = NULL;
    
    // Create socket
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        return -1;
    }
    
    // Set up server address structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(storage_port);
    if (inet_pton(AF_INET, storage_ip, &server_addr.sin_addr) <= 0) {
        close(sock_fd);
        return -1;
    }
    
    // Connect to server
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock_fd);
        return -1;
    } 

    // First, send list request to get all paths
    json_object* list_request = json_object_new_object();
    json_object_object_add(list_request, "request_code", json_object_new_string("list_all"));
    char new_path[1024]; // Temporary buffer for the new path
    snprintf(new_path, sizeof(new_path), "myserver/%s", path);
    json_object_object_add(list_request, "path", json_object_new_string(new_path));
    
    // Send list request
    send_to_client(list_request, sock_fd);
    
    // Get response with file list
    json_object* list_response = receive_request(sock_fd);
    if (list_response == NULL) {
        close(sock_fd);
        return -1;
    }
    // Extract paths array from response
    json_object* paths_array;
    json_object* types_array;  // Array indicating if each path is file or directory
    if (!json_object_object_get_ex(list_response, "paths", &paths_array) ||
        !json_object_object_get_ex(list_response, "types", &types_array)) {
        close(sock_fd);
        return -1;
    }
    
    ensure_path_exists(destination_path,0);
    
    // Iterate through each path and copy it
    int success = 1;
    int array_length = json_object_array_length(paths_array);
    
    for (int i = 0; i < array_length; i++) {
        json_object* path_obj = json_object_array_get_idx(paths_array, i);
        json_object* type_obj = json_object_array_get_idx(types_array, i);
        
        const char* relative_path = json_object_get_string(path_obj);
        const char* type = json_object_get_string(type_obj);
        
        // Construct source and destination paths
        char full_source_path[512];
        char full_dest_path[512];
        snprintf(full_source_path, sizeof(full_source_path), "%s/%s", path, relative_path);
        snprintf(full_dest_path, sizeof(full_dest_path), "%s/%s", destination_path, relative_path);
        printf("%s\n",full_dest_path);
        if (strcmp(type, "directory") == 0) {
            // If it's a directory, just ensure it exists
            ensure_path_exists(full_dest_path,1);
            continue;
        }

        // For files, create a new connection and copy content
        int new_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (new_sock_fd < 0) continue;

        if (connect(new_sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            close(new_sock_fd);
            continue;
        }

        // Ensure directory exists for this file
        ensure_path_exists(full_dest_path,0);

        
        // Send read request for this file
        json_object* read_request = json_object_new_object();
        json_object_object_add(read_request, "request_code", json_object_new_string("read"));
        json_object_object_add(read_request, "path", json_object_new_string(full_source_path));
        json_object_object_add(read_request, "client_id", json_object_new_int(-1));
        
        send_to_client(read_request, new_sock_fd);

        // Copy file content
        char buffer[100000];
        strcpy(buffer, "");
        int append_it = 0;
        int res = 1;

        // File copying logic...
        while (1) {
            json_object* response = receive_request(new_sock_fd);
            if (response == NULL) {
                res = 0;
                break;
            }

            json_object* stop_object;
            json_object* status_object;
            json_object* error_object;
            json_object* data_object;

            int stop;
            char status[100];
            
            if (!json_object_object_get_ex(response, "stop", &stop_object) ||
                !json_object_object_get_ex(response, "status", &status_object)) {
                res = 0;
                break;
            }

            stop = json_object_get_int(stop_object);
            strcpy(status, json_object_get_string(status_object));

            if (strcmp(status, "failure") == 0) {
                res = 0;
                break;
            }
            else if (strcmp(status, "success") == 0) {
                if (stop == 1) break;
                
                char temp_buffer[2*CHUNK_SIZE];
                if (json_object_object_get_ex(response, "data", &data_object)) {
                    strcpy(temp_buffer, json_object_get_string(data_object));
                    strcat(buffer, temp_buffer);
                    if (strlen(buffer) > 100000 - 10000) {
                        simple_write_to_file(buffer, full_dest_path, append_it);
                        append_it = 1;
                        strcpy(buffer,"");
                    }
                }
                else {
                    res = 0;
                    break;
                }
            }
            else {
                res = 0;
                break;
            }
        }

        if (res == 1) {
            simple_write_to_file(buffer, full_dest_path, append_it);
        }
        else {
            success = 0;
        }

        // json_object_put(read_request);
        close(new_sock_fd);
    }

    printf("copying of data is complete\n");
    // json_object_put(list_request);
    // json_object_put(list_response);
    close(sock_fd);
    return success;
    
}

int simple_write_to_file(char *buffer, char *path, int append) {
    // Determine the mode based on the append flag
    const char *mode = append ? "a" : "w";

    // Open the file in the specified mode
    FILE *file = fopen(path, mode);
    if (file == NULL) {
        // Return an error code if the file cannot be opened
        perror("Error opening file");
        return -1;
    }

    // Write the buffer to the file
    if (fputs(buffer, file) == EOF) {
        // Return an error code if writing fails
        perror("Error writing to file");
        fclose(file);
        return -1;
    }

    // Close the file
    if (fclose(file) != 0) {
        // Return an error code if the file cannot be closed
        perror("Error closing file");
        return -1;
    }

    // Return success code
    return 1;
}
void add_paths_recursively(char* base_path, char* current_path, json_object** paths_array_ptr, json_object** types_array_ptr) {
    struct stat path_stat;
    json_object*paths_array = *paths_array_ptr;
    json_object*types_array = *types_array_ptr;
    printf("hello base path |%s| current path |%s| \n",base_path,current_path);
    // First check if current_path is a file
    if (stat(current_path, &path_stat) == 0 && S_ISREG(path_stat.st_mode)) {
        // It's a file, just add it to the array
        char relative_path[1024];
        // +1 to skip the trailing slash after base_path
        snprintf(relative_path, sizeof(relative_path), "%s", current_path + strlen(base_path) + 1);
        printf("%s\n",relative_path);
        json_object_array_add(paths_array, json_object_new_string(relative_path));
        json_object_array_add(types_array, json_object_new_string("file"));
        return;
    }

    printf("%s\n",current_path);
    
    // If it's a directory, proceed with directory traversal
    char path[1024];
    struct dirent* dp;
    DIR* dir = opendir(current_path);
    
    if (!dir) return;
    
    while ((dp = readdir(dir)) != NULL) {
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
            continue;
            
        // Construct the full path
        snprintf(path, sizeof(path), "%s/%s", current_path, dp->d_name);
        
        // Get relative path by removing base_path and the trailing slash
        char relative_path[1024];
        // +1 to also skip the trailing slash after base_path
        snprintf(relative_path, sizeof(relative_path), "%s", path + strlen(base_path) + 1);
        printf("%s\n",relative_path);
        if (stat(path, &path_stat) == 0) {
            if (S_ISREG(path_stat.st_mode)) {
                // It's a file
                json_object_array_add(paths_array, json_object_new_string(relative_path));
                json_object_array_add(types_array, json_object_new_string("file"));
            } else if (S_ISDIR(path_stat.st_mode)) {
                // It's a directory
                json_object_array_add(paths_array, json_object_new_string(relative_path));
                json_object_array_add(types_array, json_object_new_string("directory"));
                add_paths_recursively(base_path, path, &paths_array, &types_array);
            }
        }
    }
    
    closedir(dir);
}
int directory_exists(const char* path) {
    DIR *dir;
    struct dirent *entry;
    int found = 0;
    
    // Open current directory
    dir = opendir(".");
    if (dir == NULL) {
        return 0;
    }
    
    closedir(dir);
    return found;
}
void ensure_path_exists(char* path, int is_directory) {
    char tmp[256];
    char* p = NULL;
    size_t len;
    struct stat path_stat;
    char* last_slash;
    
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    
    // Remove trailing slash if exists
    if (tmp[len - 1] == '/') {
        tmp[len - 1] = 0;
    }
    
    if (!is_directory) {
        // For files, create directories only up to the last slash
        last_slash = strrchr(tmp, '/');
        if (last_slash) {
            *last_slash = 0;  // Temporarily cut off the filename
            
            // Create each directory in the path
            for (p = tmp + 1; p < last_slash; p++) {
                if (*p == '/') {
                    *p = 0;
                    if (stat(tmp, &path_stat) != 0) {
                        mkdir(tmp, 0755);
                    }
                    *p = '/';
                }
            }
            
            // Create the final directory before the file
            if (stat(tmp, &path_stat) != 0) {
                mkdir(tmp, 0755);
            }
            
            *last_slash = '/';  // Restore the full path
        }
    } else {
        // For directories, create all directories in the path
        for (p = tmp + 1; *p; p++) {
            if (*p == '/') {
                *p = 0;
                if (stat(tmp, &path_stat) != 0) {
                    mkdir(tmp, 0755);
                }
                *p = '/';
            }
        }
        // Create the final directory
        if (stat(tmp, &path_stat) != 0) {
            mkdir(tmp, 0755);
        }
    }
}

int is_path_in_current_directory(char* given_path) {
    char current_dir[1024];
    char resolved_path[1024];

    // Get the current working directory
    if (getcwd(current_dir, sizeof(current_dir)) == NULL) {
        perror("getcwd");
        return 0;
    }

    // Resolve the given path to an absolute path
    if (realpath(given_path, resolved_path) == NULL) {
        perror("realpath");
        return 0;
    }

    // Check if the resolved path starts with the current directory path
    size_t current_dir_len = strlen(current_dir);
    if (strncmp(current_dir, resolved_path, current_dir_len) == 0 &&
        (resolved_path[current_dir_len] == '/' || resolved_path[current_dir_len] == '\0')) {
        return 1; // The path is inside the current directory
    } else {
        return 0; // The path is outside the current directory
    }
}

int backup_files_from_storage_server(char *storage_ip, int storage_port, json_object *paths_array, json_object *types_array,char*parent_directory) {
    
    int sock_fd;
    struct sockaddr_in server_addr;
    
    // Create initial socket for testing connection
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        return -1;
    }
    
    // Set up server address structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(storage_port);
    if (inet_pton(AF_INET, storage_ip, &server_addr.sin_addr) <= 0) {
        close(sock_fd);
        return -1;
    }
    
    // Test connection to server
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(sock_fd);
        return -1;
    }
    close(sock_fd); // Close test connection
    
   
    
    // Iterate through each path and copy it
    int success = 1;
    int array_length = json_object_array_length(paths_array);
    
    for (int i = 0; i < array_length; i++) {
        json_object* path_obj = json_object_array_get_idx(paths_array, i);
        json_object* type_obj = json_object_array_get_idx(types_array, i);
        
        const char* relative_path = json_object_get_string(path_obj);
        const char* type = json_object_get_string(type_obj);
        
        // Construct destination path
        char full_dest_path[512];
        snprintf(full_dest_path, sizeof(full_dest_path), "%s/%s", parent_directory, relative_path);

        if (strcmp(type, "directory") == 0) {
            // If it's a directory, just ensure it exists
            ensure_path_exists(full_dest_path,1);
            continue;
        }

        // For files, create a new connection and copy content
        int new_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (new_sock_fd < 0) continue;

        if (connect(new_sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            close(new_sock_fd);
            continue;
        }

        // Ensure directory exists for this file
        ensure_path_exists(full_dest_path,0);

        // Send read request for this file
        json_object* read_request = json_object_new_object();
        json_object_object_add(read_request, "request_code", json_object_new_string("read"));
        json_object_object_add(read_request, "path", json_object_new_string(relative_path));
        
        send_to_client(read_request, new_sock_fd);

        // Copy file content
        char buffer[100000];
        strcpy(buffer, "");
        int append_it = 0;
        int res = 1;

        // File copying logic
        while (1) {
            json_object* response = receive_request(new_sock_fd);
            if (response == NULL) {
                res = 0;
                break;
            }

            json_object* stop_object;
            json_object* status_object;
            json_object* data_object;

            int stop;
            char status[100];
            
            if (!json_object_object_get_ex(response, "stop", &stop_object) ||
                !json_object_object_get_ex(response, "status", &status_object)) {
                res = 0;
                break;
            }

            stop = json_object_get_int(stop_object);
            strcpy(status, json_object_get_string(status_object));

            if (strcmp(status, "failure") == 0) {
                res = 0;
                break;
            }
            else if (strcmp(status, "success") == 0) {
                if (stop == 1) break;
                
                char temp_buffer[2*CHUNK_SIZE];
                if (json_object_object_get_ex(response, "data", &data_object)) {
                    strcpy(temp_buffer, json_object_get_string(data_object));
                    strcat(buffer, temp_buffer);
                    if (strlen(buffer) > 100000 - 10000) {
                        simple_write_to_file(buffer, full_dest_path, append_it);
                        append_it = 1;
                        buffer[0] = '\0';
                    }
                }
                else {
                    res = 0;
                    break;
                }
            }
            else {
                res = 0;
                break;
            }
            
            // json_object_put(response);
        }

        if (res == 1) {
            simple_write_to_file(buffer, full_dest_path, append_it);
        }
        else {
            success = 0;
        }

        // json_object_put(read_request);
        close(new_sock_fd);
    }

    return success;
}

void print_binary(const unsigned char *data, size_t length) {
    for (size_t i = 0; i < length; i++) {
        for (int bit = 7; bit >= 0; bit--) {
            printf("%d", (data[i] >> bit) & 1);
        }
        printf(" ");
    }
    printf("\n");
}