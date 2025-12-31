#include <iostream>
#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <boost/filesystem.hpp>

int main () {
    std::string path = "/tmp/mmap_test/index.tmp";

    if (!boost::filesystem::remove(path)) {
        perror("remove");
    }

    int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open");
    }

    size_t file_size = size_t(8)*size_t(1024)*size_t(1024)*size_t(1024);
    if (ftruncate(fd, file_size) == -1) {
        perror("ftruncate");
        close(fd);
    }

    struct stat file_stat{};

    if (fstat(fd, &file_stat) == -1) {
        perror("stat");
        return 1;
    }

    printf("File size: %ld bytes\n", file_stat.st_size);
    printf("File permissions: %o\n", file_stat.st_mode);
    printf("File owner UID: %d\n", file_stat.st_uid);

    char* addr = reinterpret_cast<char*>(mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    if (addr == MAP_FAILED) {
        perror("mmap");
        close(fd);
    }

    char* data = addr;
    for (size_t i = 0; i < file_size; i++) {
        data[i] = 'a' + (i%26);
    }

    if (msync(addr, file_size, MS_SYNC) == -1) {
        perror("msync");
    }

    std::vector<char> ch;
    ch.reserve(file_size);
    for (size_t i = 0; i < file_size; i++) {
        std::cout<<addr[i];
        ch.emplace_back(addr[i]);
    }
    std::cout<<std::endl;

    if (munmap(addr, file_size) == -1) {
        perror("munmap");
    }
    close(fd);

    int nfd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        perror("open");
    }

    char* new_addr = reinterpret_cast<char*>(mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, nfd, 0));
    if (new_addr ==  MAP_FAILED) {
        perror("new mmap");
        close(nfd);
    }

    std::vector<char> nch;
    nch.reserve(file_size);
    for (size_t i = 0; i < file_size; i++) {
        std::cout<<new_addr[i];
        nch.emplace_back(new_addr[i]);
    }
    std::cout<<std::endl;

    if (munmap(new_addr, file_size) == -1) {
        perror("new munmap");
    }

    for (size_t i = 0; i < file_size; i++) {
        assert(ch[i] == nch[i]);
    }

    close(nfd);
}
