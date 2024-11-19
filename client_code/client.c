#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <json-c/json.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/types.h>
#include <netinet/in.h>

#define BUFFER_SIZE 2048
#define MAX_SIZE 1000000
#define TIMEOUT_SEC 5  // Timeout in seconds
#define TIMEOUT_USEC 0 // Timeout in microseconds

#define NM_IP "192.168.223.211"
#define NM_PORT 43016
#define TIMEOUT_SEC 5

int reqid = 0;
int nm_sock;
char client_ip[INET_ADDRSTRLEN];
int client_port;
int pending;
int client_id;
char decoded_str[BUFFER_SIZE];
char ss_ip[INET_ADDRSTRLEN];
int ss_port;

json_object *receive_response_partial(int socket)
{
    uint32_t length;
    if (recv(socket, &length, sizeof(length), 0) <= 0)
    {
        return NULL;
    }
    length = ntohl(length);

    char *buffer = (char *)malloc(length + 1);
    int total_received = 0;
    while (total_received < length)
    {
        int received = recv(socket, buffer + total_received,
                            length - total_received, 0);
        if (received <= 0)
        {
            free(buffer);
            return NULL;
        }
        total_received += received;
    }
    buffer[length] = '\0';
    printf("%s\n", buffer);
    json_object *response = json_tokener_parse(buffer);
    free(buffer);
    return response;
}

json_object *receive_timed_object(int socket)
{
    fd_set read_fds;
    struct timeval timeout;

    // Initialize file descriptor set
    FD_ZERO(&read_fds);
    FD_SET(socket, &read_fds);

    // Set timeout
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = TIMEOUT_USEC;

    // Wait for data to be available on the socket
    int ret = select(socket + 1, &read_fds, NULL, NULL, &timeout);
    if (ret < 0)
    {
        printf("select() failed");
        return NULL;
    }
    else if (ret == 0)
    {
        printf("Timeout occurred, no data received\n");
        return NULL;
    }

    // Data is available, proceed with recv
    uint32_t length;
    if (recv(socket, &length, sizeof(length), 0) <= 0)
    {
        return NULL;
    }
    length = ntohl(length);

    char *buffer = (char *)malloc(length + 1);
    if (!buffer)
    {
        // perror("Failed to allocate memory");
        return NULL;
    }

    int total_received = 0;
    while (total_received < length)
    {
        int received = recv(socket, buffer + total_received,
                            length - total_received, 0);
        if (received <= 0)
        {
            free(buffer);
            return NULL;
        }
        total_received += received;
    }
    buffer[length] = '\0';

    // printf("Received response: %s\n", buffer);
    json_object *response = json_tokener_parse(buffer);
    free(buffer);
    return response;
}
static const unsigned char base64_decoding_table[256] = {
    [0 ... 255] = 255, // Initialize all entries to invalid
    ['A'] = 0,
    ['B'] = 1,
    ['C'] = 2,
    ['D'] = 3,
    ['E'] = 4,
    ['F'] = 5,
    ['G'] = 6,
    ['H'] = 7,
    ['I'] = 8,
    ['J'] = 9,
    ['K'] = 10,
    ['L'] = 11,
    ['M'] = 12,
    ['N'] = 13,
    ['O'] = 14,
    ['P'] = 15,
    ['Q'] = 16,
    ['R'] = 17,
    ['S'] = 18,
    ['T'] = 19,
    ['U'] = 20,
    ['V'] = 21,
    ['W'] = 22,
    ['X'] = 23,
    ['Y'] = 24,
    ['Z'] = 25,
    ['a'] = 26,
    ['b'] = 27,
    ['c'] = 28,
    ['d'] = 29,
    ['e'] = 30,
    ['f'] = 31,
    ['g'] = 32,
    ['h'] = 33,
    ['i'] = 34,
    ['j'] = 35,
    ['k'] = 36,
    ['l'] = 37,
    ['m'] = 38,
    ['n'] = 39,
    ['o'] = 40,
    ['p'] = 41,
    ['q'] = 42,
    ['r'] = 43,
    ['s'] = 44,
    ['t'] = 45,
    ['u'] = 46,
    ['v'] = 47,
    ['w'] = 48,
    ['x'] = 49,
    ['y'] = 50,
    ['z'] = 51,
    ['0'] = 52,
    ['1'] = 53,
    ['2'] = 54,
    ['3'] = 55,
    ['4'] = 56,
    ['5'] = 57,
    ['6'] = 58,
    ['7'] = 59,
    ['8'] = 60,
    ['9'] = 61,
    ['+'] = 62,
    ['/'] = 63};

void base64_decode(const char *input, size_t input_len, unsigned char *output, size_t *output_len)
{
    size_t i, j;
    unsigned char four_chars[4];
    size_t output_pos = 0;

    for (i = 0; i < input_len; i += 4)
    {
        size_t valid_chars = 0;

        // Decode each group of four Base64 characters
        for (j = 0; j < 4; j++)
        {
            if (i + j < input_len && input[i + j] != '=')
            {
                four_chars[j] = base64_decoding_table[(unsigned char)input[i + j]];
                if (four_chars[j] != 255)
                {
                    valid_chars++;
                }
            }
            else
            {
                four_chars[j] = 0;
            }
        }

        // Convert the 4 characters into 3 bytes
        if (valid_chars >= 2)
        {
            output[output_pos++] = (four_chars[0] << 2) | (four_chars[1] >> 4);
        }
        if (valid_chars >= 3)
        {
            output[output_pos++] = (four_chars[1] << 4) | (four_chars[2] >> 2);
        }
        if (valid_chars == 4)
        {
            output[output_pos++] = (four_chars[2] << 6) | four_chars[3];
        }
    }

    // Set the output length
    if (output_len != NULL)
    {
        *output_len = output_pos;
    }
}

int receive_from_replica =0;
char replica_dir[1000];
void send_final_ack_to_ns(int nm_sock, char *operation)
{
    json_object *response_to_ns = json_object_new_object();
    json_object_object_add(response_to_ns, "type", json_object_new_string("client_ack"));
    json_object_object_add(response_to_ns, "final_ack", json_object_new_int(reqid));
    if (strcmp(operation, "write") == 0)
        json_object_object_add(response_to_ns, "is_write", json_object_new_int(1));
    else
        json_object_object_add(response_to_ns, "is_write", json_object_new_int(0));
    const char *request_str1 = json_object_to_json_string(response_to_ns);

    uint32_t length1 = strlen(request_str1);
    uint32_t network_length1 = htonl(length1);

    send(nm_sock, &network_length1, sizeof(network_length1), 0);
    send(nm_sock, request_str1, length1, 0);
    // close(nm_sock);
}

int initial_receive_ack()
{
    json_object *ack = NULL;

    return 1;
}

void print_binary(const unsigned char *data, size_t length)
{
    for (size_t i = 0; i < length; i++)
    {
        for (int bit = 7; bit >= 0; bit--)
        {
            printf("%d", (data[i] >> bit) & 1);
        }
        printf(" ");
    }
    printf("\n");
}

#define BASE64_ALPHABET_SIZE 64
#define BASE64_PADDING '='

// Base64 decoding table
static const unsigned char base64_decode_table[256] = {
    // Initialization of the decoding table (0 to 63 for valid characters, and -1 for others)
    ['A'] = 0,
    ['B'] = 1,
    ['C'] = 2,
    ['D'] = 3,
    ['E'] = 4,
    ['F'] = 5,
    ['G'] = 6,
    ['H'] = 7,
    ['I'] = 8,
    ['J'] = 9,
    ['K'] = 10,
    ['L'] = 11,
    ['M'] = 12,
    ['N'] = 13,
    ['O'] = 14,
    ['P'] = 15,
    ['Q'] = 16,
    ['R'] = 17,
    ['S'] = 18,
    ['T'] = 19,
    ['U'] = 20,
    ['V'] = 21,
    ['W'] = 22,
    ['X'] = 23,
    ['Y'] = 24,
    ['Z'] = 25,
    ['a'] = 26,
    ['b'] = 27,
    ['c'] = 28,
    ['d'] = 29,
    ['e'] = 30,
    ['f'] = 31,
    ['g'] = 32,
    ['h'] = 33,
    ['i'] = 34,
    ['j'] = 35,
    ['k'] = 36,
    ['l'] = 37,
    ['m'] = 38,
    ['n'] = 39,
    ['o'] = 40,
    ['p'] = 41,
    ['q'] = 42,
    ['r'] = 43,
    ['s'] = 44,
    ['t'] = 45,
    ['u'] = 46,
    ['v'] = 47,
    ['w'] = 48,
    ['x'] = 49,
    ['y'] = 50,
    ['z'] = 51,
    ['0'] = 52,
    ['1'] = 53,
    ['2'] = 54,
    ['3'] = 55,
    ['4'] = 56,
    ['5'] = 57,
    ['6'] = 58,
    ['7'] = 59,
    ['8'] = 60,
    ['9'] = 61,
    ['+'] = 62,
    ['/'] = 63};

int base64_decode2(const char *input, unsigned char **output)
{
    int input_len = strlen(input);
    if (input_len % 4 != 0)
    {
        return -1; // Invalid Base64 length
    }

    int padding = 0;
    if (input[input_len - 1] == BASE64_PADDING)
        padding++;
    if (input[input_len - 2] == BASE64_PADDING)
        padding++;

    int output_len = (input_len / 4) * 3 - padding;
    *output = (unsigned char *)malloc(output_len);
    if (*output == NULL)
        return -1; // Memory allocation failed

    int i = 0, j = 0;
    unsigned char temp[4];
    for (i = 0; i < input_len; i += 4)
    {
        // Decode each block of 4 Base64 characters
        for (int k = 0; k < 4; k++)
        {
            if (input[i + k] == BASE64_PADDING)
            {
                temp[k] = 0;
            }
            else
            {
                temp[k] = base64_decode_table[(unsigned char)input[i + k]];
            }
        }

        // Combine the 4 decoded characters into 3 bytes
        (*output)[j++] = (temp[0] << 2) | (temp[1] >> 4);
        if (j < output_len)
            (*output)[j++] = (temp[1] << 4) | (temp[2] >> 2);
        if (j < output_len)
            (*output)[j++] = (temp[2] << 6) | temp[3];
    }

    return output_len;
}

json_object *receive_response(int nm_sock)
{
    json_object *response = receive_response_partial(nm_sock);
    if (response == NULL)
    {
        printf("Failed to receive response from Naming Server\n");

        return NULL;
    }

    json_object *status_string;
    if (json_object_object_get_ex(response, "status", &status_string))
    {
        if (strcmp("previous_command", json_object_get_string(status_string)) == 0)
        {
            json_object *message_object;
            if (json_object_object_get_ex(response, "message", &message_object))
            {
                printf("%s\n", json_object_get_string(message_object));
            }

            response = receive_response_partial(nm_sock);
        }
    }

    return response;
}

int stream_audio_pipe(int sock)
{

    FILE *ffplay = popen("/usr/bin/ffplay -nodisp -autoexit -", "w");
    if (!ffplay)
    {
        perror("Failed to open pipe to ffplay");
        return -1;
    }

    while (1)
    {
        json_object *resp;

        if ((resp = receive_response(sock)) != NULL)
        {
            struct json_object *statinteger;
            if (json_object_object_get_ex(resp, "stop", &statinteger))
            {
                int stop = json_object_get_int(statinteger);
                if (stop == 1)
                {
                    printf("Received STOP packet..\n");
                    break;
                }
            }
            const char *str = json_object_to_json_string(resp);
            json_object *data_object;
            char data[MAX_SIZE];
            if (json_object_object_get_ex(resp, "data", &data_object))
            {
                char str1[BUFFER_SIZE];
                strcpy(str1, json_object_get_string(data_object));
                size_t sizeofstr1 = strlen(str1);
                size_t output_len = sizeofstr1;

                unsigned char *decoded_string = malloc(sizeofstr1);
                // base64_decode(str1, sizeofstr1, (unsigned char *)decoded_string, &output_len);
                int d_size = base64_decode2(str1, &decoded_string);

                // size_t binaudiolen = strlen(decoded_string);

                size_t chunk_size = fwrite(decoded_string, 1, d_size, ffplay);
                if (chunk_size == 0)
                {
                    perror("Failed to write audio data to ffplay");
                }
            }
        }
        else
        {
            fprintf(stderr, "connection with the server closed\n");
            break;
        }
    }

    int ret = pclose(ffplay);
    if (ret == -1)
    {
        perror("Error closing ffplay pipe");
        return -1;
    }
    else if (ret != 0)
    {
        fprintf(stderr, "ffplay exited with code %d\n", ret);
        return -1;
    }
    return 0;
}
int get_file_size(char *write_file_path)
{
    FILE *file1 = fopen(write_file_path, "rb");

    if (fseek(file1, 0, SEEK_END) != 0)
    {
        perror("fseek failed");
        fclose(file1);
        return -1;
    }

    int fileSize = ftell(file1);
    fclose(file1);
    return fileSize;
}

int connect_to_naming_server(char *nm_ip, int nm_port)
{
    int sock;
    struct sockaddr_in nm_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket creation error");
        return -1;
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(nm_port);
    if (inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr) <= 0)
    {
        printf("Invalid address or address not supported\n");
        close(sock);
        return -1;
    }

    // Connect to the Naming Server
    printf("hello moto\n");
    if (connect(sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0)
    {
        printf("Connection to Naming Server failed");
        close(sock);
        return -1;
    }
    printf("hi\n");
    return sock;
}

int connect_to_storage_server(const char *ss_ip, int *ss_port)
{
    int ss_sock;
    struct sockaddr_in ss_addr;
    ssize_t bytes_received_ss;
    char buffer_ss[BUFFER_SIZE];
    // Create a TCP socket
    if ((ss_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("Socket creation error");
        return -1;
    }

    // Configure the Storage Server address
    ss_addr.sin_family = AF_INET;
    ss_addr.sin_port = htons(*ss_port);
    if (inet_pton(AF_INET, ss_ip, &ss_addr.sin_addr) <= 0)
    {
        printf("Invalid address or address not supported\n");
        close(ss_sock);
        return -1;
    }

    // Connect to the Storage Server
    if (connect(ss_sock, (struct sockaddr *)&ss_addr, sizeof(ss_addr)) < 0)
    {
        printf("Connection to Storage Server failed\n");
        close(ss_sock);
        return -1;
    }
    printf("hi\n");
    return ss_sock;
}

void get_clientip(int nm_sock)
{
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    if (getsockname(nm_sock, (struct sockaddr *)&client_addr, &client_addr_len) == 0)
    {
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        client_port = ntohs(client_addr.sin_port);

        printf("Client IP: %s\n", client_ip);
        printf("Client Port: %d\n", client_port);
    }
}

int get_ss_ip_port(int nm_sock, char *ss_ip, int *ss_port)
{
    json_object *response = receive_response_partial(nm_sock);
    json_object *ip_obj, *port_obj, *client_id_object;
    if (json_object_object_get_ex(response, "ip", &ip_obj) &&
        json_object_object_get_ex(response, "client_port", &port_obj))
    {
        strncpy(ss_ip, json_object_get_string(ip_obj), INET_ADDRSTRLEN);
        *ss_port = json_object_get_int(port_obj);
        // client_id = json_object_get_int(client_id_object);
        return 0;
    }
    else
    {
        printf("Not able to get storage ip adnd port\n");
        return -1;
    }
}

int read_opn(int nm_sock, const char *path, char *ss_ip, int *ss_port)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    printf("helo1\n");
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("read"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    const char *request_str = json_object_to_json_string(request);
    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);
    printf("hello\n");

    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;
        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            // printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            // printf("hello\n");
        }
        else
            return -1;

       

        json_object *response = receive_response_partial(nm_sock);
        json_object *ip_obj, *port_obj,*replica_dir_object;
       
        if (json_object_object_get_ex(response, "ip", &ip_obj) &&
            json_object_object_get_ex(response, "client_port", &port_obj))
        {
            strncpy(ss_ip, json_object_get_string(ip_obj), INET_ADDRSTRLEN);
            *ss_port = json_object_get_int(port_obj);
            
            if(json_object_object_get_ex(response,"receive_from_replica",&replica_dir_object)){
                receive_from_replica =1;
                strcpy(replica_dir,json_object_get_string(replica_dir_object));
            }
            return 0;
        }
        else
        {
            printf("Not able to get storage ip adnd port\n");
            return -1;
        }
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }
}

int write_opn(int nm_sock, const char *path, char *ss_ip, int *ss_port, int syncflag, int append_flag)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();

    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("write"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    if (syncflag == 0)
        json_object_object_add(request, "sync", json_object_new_boolean(0));
    else
        json_object_object_add(request, "sync", json_object_new_boolean(1));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    if (append_flag == 0)
        json_object_object_add(request, "append_flag", json_object_new_boolean(0));
    else
        json_object_object_add(request, "append_flag", json_object_new_boolean(1));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);

    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;

        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {

            client_id = json_object_get_int(client_id_object);
            printf("%d\n", client_id);
        }
        else
            return -1;

        if (get_ss_ip_port(nm_sock, ss_ip, ss_port) == 0)
        {
            return 0;
        }

        else
            return -1;
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }

    return 0;
}

int create_opn(int nm_sock, const char *path, const char *name, int flag)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("create"));
    json_object_object_add(request, "path", json_object_new_string(path));
    if (flag == 1)
        json_object_object_add(request, "is_directory", json_object_new_boolean(1));
    else
        json_object_object_add(request, "is_directory", json_object_new_boolean(0));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    json_object_object_add(request, "name", json_object_new_string(name));

    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);

    // json_object_put(request); // Free JSON object after sending
    json_object *initial_ack = receive_timed_object(nm_sock);
    // if (initial_ack != NULL)
    // {
    //     printf("Initial ACK received from Naming Server..\n");
    // }
    if (initial_ack == NULL)
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }
    else
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;
        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            // printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            // printf("hello\n");
        }

        json_object *response = receive_response(nm_sock);

        if (response == NULL)
        {
            printf("Failed to receive response from NM\n");

            return -1;
        }

        json_object *status_string;
        if (json_object_object_get_ex(response, "status", &status_string))
        {
            const char *status = json_object_get_string(status_string); // Retrieve the string value

            if (strcmp(status, "success") == 0)
            {
                printf("SUCCESS! Request Completed Successfully..\n");
                return 0;
            }
            else if (strcmp(status, "error") == 0)
            {
                printf("FAILURE! Request could not be completed..\n");
                json_object *error_string;
                if (json_object_object_get_ex(response, "message", &error_string))
                {
                    const char *errstr = json_object_get_string(error_string);
                    printf("%s\n", errstr);
                    return -1;
                }
            }
        }
        else
        {
            // printf("Invalid response from Naming Server\n");
            // json_object_put(response);
            return -1;
        }
    }
}

int copy_opn(int nm_sock, const char *src, const char *dest)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("copy"));
    json_object_object_add(request, "path1", json_object_new_string(src));
    json_object_object_add(request, "path2", json_object_new_string(dest));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));

    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);
    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack == NULL)
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }
    else
    {
        printf("Initial ACK received..\n");
        json_object *client_id_object;
        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            // printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            // printf("hello\n");
        }
        else
            return -1;

        json_object *response = receive_response(nm_sock);
        if (response == NULL)
        {
            perror("Failed to receive response from Naming Server");
            return -1;
        }

        json_object *status_string;
        if (json_object_object_get_ex(response, "status", &status_string))
        {
            const char *status = json_object_get_string(status_string);

            if (strcmp(status, "success") == 0)
            {
                printf("SUCCESS! Request Completed Successfully..\n");
                return 0;
            }
            else if (strcmp(status, "error") == 0)
            {
                printf("FAILURE! Request could not be completed..\n");
                json_object *error_string;
                if (json_object_object_get_ex(response, "message", &error_string))
                {
                    const char *errstr = json_object_get_string(error_string);
                    printf("%s\n", errstr);
                }
                return -1;
            }
        }
    }
}
int delete_opn(int nm_sock, char *path)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("delete"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));

    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);

    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;
        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            printf("hello\n");
        }
        else
            return -1;
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }

    json_object *response = receive_response(nm_sock);

    if (response == NULL)
    {
        printf("Failed to receive response from Naming Server\n");
        return -1;
    }
    json_object *status_string;
    if (json_object_object_get_ex(response, "status", &status_string))
    {
        const char *status = json_object_get_string(status_string); // Retrieve the string value

        if (strcmp(status, "success") == 0)
        {
            // Handle success case
            printf("SUCCESS! Request Completed Successfully\n");
            return 0;
        }
        else if (strcmp(status, "error") == 0)
        {
            printf("FAILURE! Request could not be completed\n");
            json_object *error_string;
            if (json_object_object_get_ex(response, "message", &error_string))
            {
                const char *errstr = json_object_get_string(error_string);
                printf("%s\n", errstr);
            }

            return -1;
        }
    }
    else
    {
        printf("Invalid response from Naming Server\n");

        return -1;
    }
}

int list_all(int nm_sock)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("list_all"));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);
    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }

    json_object *response = receive_response(nm_sock);
    if (response == NULL)
    {
        printf("Failed to receive response from Naming Server\n");
        return -1;
    }
    else
    {
        json_object *stat_string;
        if (json_object_object_get_ex(response, "status", &stat_string))
        {
            const char *statuscode = json_object_get_string(stat_string);
            if (strcmp(statuscode, "success") == 0)
            {
                printf("SUCCESS!!\n");
                json_object *paths_array;

                if (json_object_object_get_ex(response, "paths", &paths_array))
                {
                    int array_len = json_object_array_length(paths_array);
                    printf("Number of accessible paths: %d\n", array_len);
                    for (int i = 0; i < array_len; i++)
                    {
                        json_object *path_obj = json_object_array_get_idx(paths_array, i);
                        const char *path = json_object_get_string(path_obj);
                        printf("%s\n", path);
                    }
                }
                return 0;
            }
            else
            {
                printf("FAIURE! Request could not be completed\n");
                json_object *error_obj;
                if (json_object_object_get_ex(response, "message", &error_obj))
                {
                    const char *errstr = json_object_get_string(error_obj);
                    printf("%s\n", errstr);
                }
                return -1;
            }
        }
    }
}

int file_info(int nm_sock, char *path, char *ss_ip, int *ss_port)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("get_info"));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    json_object_object_add(request, "path", json_object_new_string(path));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);
    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;

        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            printf("hello\n");
        }
        else
            return -1;

        if (get_ss_ip_port(nm_sock, ss_ip, ss_port) == 0)
        {
            return 0;
        }

        else
            return -1;
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }
}

int stream_opn(int nm_sock, char *path, char *ss_ip, int *ss_port)
{
    get_clientip(nm_sock);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "type", json_object_new_string("client_request"));
    json_object_object_add(request, "operation", json_object_new_string("stream"));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_ip", json_object_new_string(client_ip));
    json_object_object_add(request, "client_port", json_object_new_int(client_port));
    json_object_object_add(request, "path", json_object_new_string(path));

    const char *request_str = json_object_to_json_string(request);
    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(nm_sock, &network_length, sizeof(network_length), 0);
    send(nm_sock, request_str, length, 0);

    json_object *initial_ack = receive_timed_object(nm_sock);
    if (initial_ack != NULL)
    {
        printf("Initial ACK received from Naming Server..\n");
        json_object *client_id_object;
        // printf("hello1\n");
        if (json_object_object_get_ex(initial_ack, "client_id", &client_id_object))
        {
            printf("hello2\n");
            client_id = json_object_get_int(client_id_object);
            printf("hello\n");
        }
        else
        {
            return -1;
        }

        if (get_ss_ip_port(nm_sock, ss_ip, ss_port) == 0)
        {
            return 0;
        }

        else
            return -1;
    }
    else
    {
        printf("Timeout!! Initial ACK not received..\n");
        return -1;
    }
}

void send_chunks_to_ss(int sock, char *path, char *data)
{
    int count = 0;
    int chunk_size = 2000;
    int datalen = strlen(data);
    int bytes_sent = 0;
    while (bytes_sent < datalen)
    {
        int current_chunk_size;
        if ((datalen - bytes_sent) < chunk_size)
        {
            current_chunk_size = datalen - bytes_sent;
        }
        else
        {
            current_chunk_size = chunk_size;
        }
        char chunk[current_chunk_size + 1];
        strncpy(chunk, &data[bytes_sent], current_chunk_size);
        chunk[current_chunk_size] = '\0';
        int final_chunk = (bytes_sent + current_chunk_size >= datalen) ? 1 : 0;
        json_object *jobj = json_object_new_object();
        json_object_object_add(jobj, "data", json_object_new_string(chunk));
        json_object_object_add(jobj, "chunk_size", json_object_new_int(current_chunk_size));
        json_object_object_add(jobj, "stop", json_object_new_int(0));
        const char *json_str = json_object_to_json_string(jobj);
        uint32_t length = strlen(json_str);
        uint32_t network_length = htonl(length);

        send(sock, &network_length, sizeof(network_length), 0);
        send(sock, json_str, length, 0);
        printf("%s", json_str);
        bytes_sent += current_chunk_size;
        count += current_chunk_size;
    }
}
void sendstop(int sock)
{
    json_object *jobj = json_object_new_object();
    json_object_object_add(jobj, "stop", json_object_new_int(1));
    const char *json_str = json_object_to_json_string(jobj);
    uint32_t length = strlen(json_str);
    uint32_t network_length = htonl(length);

    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, json_str, length, 0);
}

void receive_print_chunk(int sock, char *path)
{
    char respstring[MAX_SIZE];
    int characters_received = 0;
    strcpy(respstring, "");
    while (1)
    {
        int stop = 0;
        json_object *resp = receive_response(sock);

        struct json_object *statinteger;
        if (json_object_object_get_ex(resp, "stop", &statinteger))
        {
            stop = json_object_get_int(statinteger);
            if (stop == 1)
            {
                printf("Received STOP packet, closing connection.\n");
            }
        }
        const char *str = json_object_to_json_string(resp);
        json_object *status_object;
        char data[5 * BUFFER_SIZE];
        if (json_object_object_get_ex(resp, "status", &status_object))
        {
            if (strcmp("success", json_object_get_string(status_object)) == 0)
            {
                if (stop)
                    break;

                json_object *data_object;

                if (json_object_object_get_ex(resp, "data", &data_object))
                {
                    char str[5 * BUFFER_SIZE];
                    strcpy(str, json_object_get_string(data_object));
                    strcat(respstring, str);
                    characters_received += strlen(data);
                    if (strlen(respstring) >= MAX_SIZE - MAX_SIZE / 10)
                    {
                        printf("%s\n", respstring);
                        strcpy(respstring, "");
                    }
                }
            }
            else
            {
                json_object *error_object;
                if (json_object_object_get_ex(resp, "error", &error_object))
                {
                    printf("ERROR IN STORAGE SERVER : %s\n", json_object_get_string(error_object));
                }

                break;
            }
        }
    }
    printf("%s\n", respstring);
}

void client_ss_read_from_replica(int sock, char *path, int nm_sock, char *operation)
{
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("read2"));
    char new_path[1024];
    snprintf(new_path, sizeof(new_path), "%s/%s", replica_dir, path);
    json_object_object_add(request, "path", json_object_new_string(new_path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));

    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);

    receive_print_chunk(sock, path);
    close(sock);
    send_final_ack_to_ns(nm_sock, operation);
    return;
}


void client_ss_read(int sock, char *path, int nm_sock, char *operation)
{
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("read"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));

    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);

    receive_print_chunk(sock, path);
    close(sock);
    send_final_ack_to_ns(nm_sock, operation);
    return;
}

int send_chunks_from_file(int sock, char *filepath)
{

    FILE *file = fopen(filepath, "r");
    if (file == NULL)
    {
        return -1;
    }

    char buffer[2000];
    int bytes_read = 0;

    while ((bytes_read = fread(buffer, 1, 2000, file)) > 0)
    {
        json_object *jobj = json_object_new_object();
        json_object_object_add(jobj, "data", json_object_new_string(buffer));
        json_object_object_add(jobj, "chunk_size", json_object_new_int(bytes_read));
        json_object_object_add(jobj, "stop", json_object_new_int(0));
        const char *json_str = json_object_to_json_string(jobj);
        uint32_t length = strlen(json_str);
        uint32_t network_length = htonl(length);

        send(sock, &network_length, sizeof(network_length), 0);
        send(sock, json_str, length, 0);
    }

    fclose(file);
}

void client_ss_write_file(int sock, char *path, int appendflag, int syncflag, char *write_file_path, int nm_sock, char *operation)
{
    int write_size = get_file_size(write_file_path);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("write"));
    json_object_object_add(request, "path", json_object_new_string(path));
    if (appendflag == 1)
        json_object_object_add(request, "append_flag", json_object_new_int(1));
    else
        json_object_object_add(request, "append_flag", json_object_new_int(0));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "sync", json_object_new_int(syncflag));
    json_object_object_add(request, "write_size", json_object_new_int(write_size));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);
    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);
    json_object *resp = receive_response_partial(sock);
    if (resp == NULL)
    {
        printf("Failed to receive response from Storage Server\n");
        close(sock);
    }
    else
    {
        json_object *status_object;
        char init_status[100];
        if (json_object_object_get_ex(resp, "status", &status_object))
        {
            strcpy(init_status, json_object_get_string(status_object));
        }
        else
        {
            // error handling
            printf("unable to extract status \n");
            close(sock);
            return;
        }

        if (strcmp(init_status, "pending") == 0)
        {
            pending = 1;

            send_chunks_from_file(sock, write_file_path);
            sendstop(sock);
        }
        else if (strcmp(init_status, "failure") == 0)
        {
            json_object *error_obj;
            if (json_object_object_get_ex(resp, "error", &error_obj))
            {
                const char *error_str = json_object_get_string(error_obj);
                printf("%s\n", error_str);
            }
            send_final_ack_to_ns(nm_sock, operation);
        }
        else
        {
            pending = 0;
            send_final_ack_to_ns(nm_sock, operation);
            send_chunks_from_file(sock, write_file_path);
            sendstop(sock);
            json_object *response = receive_response(sock);
            struct json_object *statinteger;
            if (json_object_object_get_ex(response, "stop", &statinteger))
            {
                int status = json_object_get_int(statinteger);
                if (status == 1)
                {
                    printf("Received STOP packet..\n");
                }
            }
            if (json_object_object_get_ex(response, "status", &status_object))
            {
                char status[100];
                json_object *error_object;
                strcpy(status, json_object_get_string(status_object));
                printf("%s\n", status);
                if (strcmp(status, "failure") && json_object_object_get_ex(response, "error", &error_object))
                {
                    printf("%s\n", json_object_get_string(error_object));
                }
            }
        }
    }
    close(sock);
    return;
}

void client_ss_write(int sock, char *path, int appendflag, int syncflag, char *data, int nm_sock, char *operation)
{
    int write_size = strlen(data);
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("write"));
    json_object_object_add(request, "path", json_object_new_string(path));
    if (appendflag == 1)
        json_object_object_add(request, "append_flag", json_object_new_int(1));
    else
        json_object_object_add(request, "append_flag", json_object_new_int(0));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "sync", json_object_new_int(syncflag));
    json_object_object_add(request, "write_size", json_object_new_int(write_size));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);
    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);

    json_object *resp = receive_response_partial(sock);
    if (resp == NULL)
    {
        printf("Failed to receive response from Storage Server\n");
        close(sock);
    }
    else
    {
        json_object *status_object;
        char init_status[100];
        if (json_object_object_get_ex(resp, "status", &status_object))
        {
            strcpy(init_status, json_object_get_string(status_object));
        }
        else
        {
            // error handling
            printf("unable to extract status \n");
            close(sock);
            return;
        }

        if (strcmp(init_status, "pending") == 0)
        {
            pending = 1;
            send_chunks_to_ss(sock, path, data);
            sendstop(sock);
        }
        else if (strcmp(init_status, "failure") == 0)
        {
            json_object *error_obj;
            if (json_object_object_get_ex(resp, "error", &error_obj))
            {
                const char *error_str = json_object_get_string(error_obj);
                printf("%s\n", error_str);
            }
            send_final_ack_to_ns(nm_sock, operation);
        }
        else
        {
            pending = 0;
            send_final_ack_to_ns(nm_sock, operation);
            send_chunks_to_ss(sock, path, data);
            sendstop(sock);
            json_object *response = receive_response(sock);
            struct json_object *statinteger;
            if (json_object_object_get_ex(response, "stop", &statinteger))
            {
                int status = json_object_get_int(statinteger);
                if (status == 1)
                {
                    printf("Received STOP packet..\n");
                }
            }
        }
    }
    close(sock);
    return;
}

void client_ss_fileinfo(int sock, char *path, int, char *operation)
{
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("get_file_info"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);

    // json_object_put(request);
    int fg = 0;
    json_object *response = receive_response(sock);
    struct json_object *statobj;
    if (json_object_object_get_ex(response, "status", &statobj))
    {
        const char *statusstr = json_object_get_string(statobj);
        if (strcmp("failure", statusstr) == 0)
        {
            fg = 1;
        }
    }
    if (fg)
    {
        printf("Failure!! Could not retreive file info..");
        json_object *error_obj;
        if (json_object_object_get_ex(response, "error", &error_obj))
        {
            const char *error_str = json_object_get_string(error_obj);
            printf("%s\n", error_str);
        }
        send_final_ack_to_ns(nm_sock, operation);
    }
    else
    {
        printf("Success!! Following are the file details: \n");
        json_object *file_info_obj;
        if (json_object_object_get_ex(response, "file_info", &file_info_obj))
        {

            json_object *fname_obj, *fpath_obj, *size_obj, *permissions_obj, *last_modified_obj, *is_directory_obj;
            if (json_object_object_get_ex(file_info_obj, "file_name", &fname_obj))
            {
                const char *file_name_ = json_object_get_string(fname_obj);
                printf("File name: %s\n", file_name_);
            }
            else
            {
                printf("Failed to get 'file_name'\n");
            }

            if (json_object_object_get_ex(file_info_obj, "file_path", &fpath_obj))
            {
                const char *file_path_ = json_object_get_string(fpath_obj);
                printf("File path: %s\n", file_path_);
            }
            else
            {
                printf("Failed to get 'file_path'\n");
            }

            if (json_object_object_get_ex(file_info_obj, "size", &size_obj))
            {
                off_t size = json_object_get_int64(size_obj);
                printf("File size: %ld bytes\n", size);
            }
            else
            {
                printf("Failed to get 'size'\n");
            }

            // Retrieve the file permissions
            if (json_object_object_get_ex(file_info_obj, "permissions", &permissions_obj))
            {
                mode_t permissions = json_object_get_int(permissions_obj);
                printf("File permissions (octal): %o\n", permissions);
            }
            else
            {
                printf("Failed to get 'permissions'\n");
            }

            // Retrieve the last modified time
            if (json_object_object_get_ex(file_info_obj, "last_modified", &last_modified_obj))
            {
                const char *last_modified = json_object_get_string(last_modified_obj);
                printf("Last modified: %s\n", last_modified);
            }
            else
            {
                printf("Failed to get 'last_modified'\n");
            }

            if (json_object_object_get_ex(file_info_obj, "is_directory", &is_directory_obj))
            {
                int is_directory = json_object_get_boolean(is_directory_obj);
                printf("Is directory: %s\n", is_directory ? "Yes" : "No");
            }
            else
            {
                printf("Failed to get 'is_directory'\n");
            }
        }
        else
        {
            printf("Failed to get 'file_info' from response\n");
        }
        send_final_ack_to_ns(nm_sock, operation);
    }
    close(sock);
    return;
}

void client_ss_stream(int sock, char *path, int nm_sock, char *operation)
{
    json_object *request = json_object_new_object();
    json_object_object_add(request, "request_code", json_object_new_string("stream"));
    json_object_object_add(request, "path", json_object_new_string(path));
    json_object_object_add(request, "request_id", json_object_new_int(reqid));
    json_object_object_add(request, "client_id", json_object_new_int(client_id));
    const char *request_str = json_object_to_json_string(request);

    uint32_t length = strlen(request_str);
    uint32_t network_length = htonl(length);

    send(sock, &network_length, sizeof(network_length), 0);
    send(sock, request_str, length, 0);

    json_object *init_response = receive_response_partial(sock);
    json_object *status_object;
    json_object_object_get_ex(init_response, "status", &status_object);
    if (strcmp("failure", json_object_get_string(status_object)) == 0)
    {
        json_object *error_object;
        if (json_object_object_get_ex(init_response, "error", &error_object))
        {
            printf("%s\n", json_object_get_string(error_object));
        }

        return;
    }
    else
    {
        printf("started streaming audio\n");
    }

    int res = stream_audio_pipe(sock);
    send_final_ack_to_ns(nm_sock, operation);
    return;
}

int main(int argc,char* argv[])
{
    char operation[BUFFER_SIZE];
    reqid = 1;
    FILE *write_file;
    if (argc != 3) {
        printf("Usage: %s ", argv[0]);
        return 1;
    }
    char* nm_ip=argv[1];
    int nm_port=atoi(argv[2]);
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, nm_ip, &(sa.sin_addr));
    if (result == 1)
    {
        printf("The IP address is valid: %s\n", nm_ip);
        nm_sock = connect_to_naming_server(nm_ip, nm_port);
        if (nm_sock <= 0)
        {
            printf("\nCouldn't connect to Naming Server properly!!\n");
        }
        else
        {
            while (1)
            {
                pending = 0;
                receive_from_replica =0;
                int result = 1;
                int filefg = 0;
                char path[BUFFER_SIZE];
                int syncflag;
                char src[BUFFER_SIZE];
                char dest[BUFFER_SIZE];
                char name[BUFFER_SIZE];
                int appendfg;
                char choice[BUFFER_SIZE];
                char data[MAX_SIZE];
                char file_path[BUFFER_SIZE];
                int r1 = 1;
                int connect_only_once_flag = 0;
                // FILE* write_file;
                printf("Enter the operation (read,write,delete,create,copy,stream,getfileinfo or list).. Enter STOP to exit : \n");
                scanf("%s", operation);
                if (strcmp(operation, "read") == 0)
                {

                    printf("Give path: ");
                    scanf("%s", path);
                    result = read_opn(nm_sock, path, ss_ip, &ss_port);
                }
                else if (strcmp(operation, "write") == 0)
                {

                    printf("Give path: ");
                    scanf("%s", path);
                    printf("Priority write or not? type 0 or 1\n");
                    scanf("%d", &syncflag);
                    printf("Want to append or overwrite? type 0 or 1\n");
                    scanf("%d", &appendfg);
                    printf("What's your choice? Read from file or give input (String or File): ");
                    scanf("%s", choice);
                    if (strcmp(choice, "String") == 0 || strcmp(choice, "string") == 0)
                    {
                        printf("Enter data\n");
                        scanf("%s", data);
                    }
                    else if (strcmp(choice, "File") == 0 || strcmp(choice, "file") == 0)
                    {
                        filefg = 1;
                        printf("Please provide path of file to be read: ");
                        scanf("%s", file_path);

                        write_file = fopen(file_path, "rb");
                        if (write_file == NULL)
                        {
                            printf("please enter a valid file path\n");
                            continue;
                        }
                        // printf("%d\n",filefg);
                    }
                    result = write_opn(nm_sock, path, ss_ip, &ss_port, syncflag, appendfg);
                }
                else if (strcmp(operation, "delete") == 0)
                {

                    printf("Give path: ");
                    scanf("%s", path);
                    r1 = delete_opn(nm_sock, path);
                    // close(nm_sock);
                }
                else if (strcmp(operation, "create") == 0)
                {

                    printf("Give path:\n");
                    scanf("%s", path);
                    strcat(path, "/");
                    printf("Give name:\n");
                    scanf("%s", name);
                    int flag;
                    printf("Enter 1 for creating directory or 0 for creating file\n");
                    scanf("%d", &flag);
                    r1 = create_opn(nm_sock, path, name, flag);
                    // close(nm_sock);
                }
                else if (strcmp(operation, "copy") == 0)
                {

                    printf("Give src path: ");
                    scanf("%s", src);
                    printf("Give dest: ");
                    scanf("%s", dest);
                    r1 = copy_opn(nm_sock, src, dest);
                    // close(nm_sock);
                }
                else if (strcmp(operation, "stream") == 0)
                {

                    printf("Give audio file path: \n");
                    scanf("%s", path);
                    result = stream_opn(nm_sock, path, ss_ip, &ss_port);
                }
                else if (strcmp(operation, "getfileinfo") == 0)
                {
                    printf("Give path\n");
                    scanf("%s", path);
                    result = file_info(nm_sock, path, ss_ip, &ss_port);
                }
                else if (strcmp(operation, "list") == 0)
                {

                    r1 = list_all(nm_sock);
                    // close(nm_sock);
                }
                else if (strcmp(operation, "STOP") == 0)
                {
                    break;
                }
                else
                    printf("Invalid operation!! Please enter valid operation.\n");
                if (result == 0)
                {
                    int ss_sock = connect_to_storage_server(ss_ip, &ss_port);
                    if (ss_sock <= 0)
                    {
                        printf("Couldn't connect to Storage Sever properly!!\n");
                        continue;
                    }
                    if (strcmp(operation, "read") == 0)
                    {
                        if(receive_from_replica == 0)client_ss_read(ss_sock, path, nm_sock, operation);
                        else client_ss_read_from_replica(ss_sock,path,nm_sock,operation);
                    }
                    else if (strcmp(operation, "write") == 0)
                    {
                        if (!filefg)
                        {
                            client_ss_write(ss_sock, path, appendfg, syncflag, data, nm_sock, operation);
                        }
                        else
                        {
                            client_ss_write_file(ss_sock, path, appendfg, syncflag, file_path, nm_sock, operation);
                        }
                    }
                    else if (strcmp(operation, "getfileinfo") == 0)
                    {
                        client_ss_fileinfo(ss_sock, path, nm_sock, operation);
                    }
                    else if (operation, "stream")
                    {
                        client_ss_stream(ss_sock, path, nm_sock, operation);
                    }
                }
                else if (result == -1)
                {
                    printf("Sorry..Request could not be completed\n");
                    // close(nm_sock);
                }
                else if (r1 == -1)
                {
                    printf("Sorry.. Please try again\n");
                }
                reqid++;
            }
        }
    }
    else
    {
        printf("Invalid IP address format!\n");
    }
    return 0;
}