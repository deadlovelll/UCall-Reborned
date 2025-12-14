#include "memory_map.hpp"

memory_map_t::memory_map_t() noexcept = default;

memory_map_t::memory_map_t(memory_map_t&& other) noexcept {
    std::swap(ptr, other.ptr);
    std::swap(length, other.length);
}

memory_map_t& memory_map_t::operator=(memory_map_t&& other) noexcept {
    std::swap(ptr, other.ptr);
    std::swap(length, other.length);
    return *this;
}

bool memory_map_t::reserve(std::size_t length, int flags) noexcept {
    auto new_ptr = reinterpret_cast<char*>(mmap(ptr, length, PROT_WRITE | PROT_READ, flags, -1, 0));
    if (new_ptr == MAP_FAILED) {
        errno;
        return false;
    }
    std::memset(new_ptr, 0, length);
    ptr = new_ptr;
    this->length = length;
    return true;
}

memory_map_t::~memory_map_t() noexcept {
    if (ptr)
        munmap(ptr, length);
    ptr = nullptr;
    length = 0;
}