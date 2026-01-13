#pragma once

#include <iostream>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <nlohmann/json.hpp>
#include "common.h" 

using json = nlohmann::json;

class PoolUpdateServer {
    int server_fd;
    int port;
    struct sockaddr_in address;

public:
    PoolUpdateServer(int port_num) : port(port_num) {
        // 1. Create Socket
        if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
            perror("socket failed");
            exit(EXIT_FAILURE);
        }

        // 2. Set Reuse Address (Critical for restarts)
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
            perror("setsockopt");
            exit(EXIT_FAILURE);
        }

        // 3. Set Non-Blocking
        int flags = fcntl(server_fd, F_GETFL, 0);
        fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY; // Bind to 0.0.0.0
        address.sin_port = htons(port);

        // 4. Bind & Listen
        if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
            perror("bind failed");
            exit(EXIT_FAILURE);
        }
        if (listen(server_fd, 20) < 0) { // Backlog of 20 pending updates
            perror("listen");
            exit(EXIT_FAILURE);
        }
        
        std::cout << "✅ TCP Update Server listening on Port " << port << std::endl;
    }

    ~PoolUpdateServer() {
        if (server_fd >= 0) close(server_fd);
    }

    // Call this at the start of your loop
    void poll_updates(HotAddressLookups& hot_addrs) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);

        // Timeout 0.0s = Instant Return (No Waiting)
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 0;

        // Check if there are pending connections
        int activity = select(server_fd + 1, &readfds, NULL, NULL, &timeout);

        if (activity > 0 && FD_ISSET(server_fd, &readfds)) {
            // Loop to accept ALL pending connections (drain the queue)
            while (true) {
                int new_socket;
                int addrlen = sizeof(address);
                
                // Accept new connection
                if ((new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
                    // If accept returns < 0, queue is empty (EWOULDBLOCK/EAGAIN)
                    break; 
                }

                // Read Data
                char buffer[2048] = {0};
                ssize_t valread = read(new_socket, buffer, 2048);
                
                if (valread > 0) {
                    try {
                        auto j = json::parse(buffer);
                        std::string base = j["base_vault"];
                        std::string quote = j["quote_vault"];
                        
                        std::cout << "\n[TCP] ⚡ RECEIVED: " << base << " / " << quote << std::endl;
                        hot_addrs.add_new_vaults(base, quote);
                        
                    } catch (...) { /* Ignore bad JSON */ }
                }
                close(new_socket); // Done
            }
        }
    }
};