/*
 * UDP Echo 服务器 - 配合sockperf测试 (阻塞IO版本)
 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

int main() {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(9000);
    
    bind(fd, (sockaddr*)&addr, sizeof(addr));
    
    printf("UDP Echo server listening on port 9000\n");
    fflush(stdout);
    
    char buf[4096];
    while (true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        
        ssize_t n = recvfrom(fd, buf, sizeof(buf), 0, 
                             (sockaddr*)&client_addr, &len);
        if (n > 0) {
            sendto(fd, buf, n, 0, (sockaddr*)&client_addr, len);
        }
    }
    
    close(fd);
    return 0;
}
