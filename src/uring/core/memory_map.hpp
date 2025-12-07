#include <sys/mman.h>  
#include <charconv> 

struct memory_map_t {
    char* ptr{};
    std::size_t length{};

    memory_map_t() noexcept;
    memory_map_t(memory_map_t&& other) noexcept;
    memory_map_t& operator=(memory_map_t&& other) noexcept;

    memory_map_t(const memory_map_t&) = delete;
    memory_map_t& operator=(const memory_map_t&) = delete;

    bool reserve(
        std::size_t length, 
        int flags = MAP_ANONYMOUS | MAP_PRIVATE
    ) noexcept;
    ~memory_map_t() noexcept;
};