#ifndef CUSTOM_CONTAINERS_HPP
#define CUSTOM_CONTAINERS_HPP

#include <vector>
#include <string>

namespace Custom {

template <typename T>
struct Hasher {
    size_t operator()(const T& key) const {
        size_t hash = 2166136261U;
        for (auto c : key) {
            hash ^= static_cast<size_t>(c);
            hash *= 16777619U;
        }
        return hash;
    }
};

template <typename T>
class HashSet {
private:
    std::vector<std::vector<T>> buckets;
    size_t num_elements;
    size_t capacity;
    const double MAX_LOAD_FACTOR = 1.0;

    void rehash() {
        size_t new_capacity = capacity * 2 + 1;
        std::vector<std::vector<T>> new_buckets(new_capacity);
        Hasher<T> hasher;
        for (auto& bucket : buckets) {
            for (auto& item : bucket) {
                new_buckets[hasher(item) % new_capacity].push_back(std::move(item));
            }
        }
        buckets = std::move(new_buckets);
        capacity = new_capacity;
    }

public:
    HashSet(size_t cap = 101) : capacity(cap), num_elements(0) {
        buckets.resize(capacity);
    }

    HashSet(const HashSet& other) {
        buckets = other.buckets;
        num_elements = other.num_elements;
        capacity = other.capacity;
    }

    HashSet& operator=(const HashSet& other) {
        if (this != &other) {
            buckets = other.buckets;
            num_elements = other.num_elements;
            capacity = other.capacity;
        }
        return *this;
    }

    void insert(const T& key) {
        if (contains(key)) return;
        if ((double)num_elements / capacity > MAX_LOAD_FACTOR) rehash();
        Hasher<T> hasher;
        buckets[hasher(key) % capacity].push_back(key);
        num_elements++;
    }

    bool contains(const T& key) const {
        Hasher<T> hasher;
        const auto& bucket = buckets[hasher(key) % capacity];
        for (const auto& item : bucket) {
            if (item == key) return true;
        }
        return false;
    }

    void erase(const T& key) {
        Hasher<T> hasher;
        auto& bucket = buckets[hasher(key) % capacity];
        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (*it == key) {
                bucket.erase(it);
                num_elements--;
                return;
            }
        }
    }

    size_t size() const { return num_elements; }

    struct Iterator {
        const HashSet& set;
        size_t b_idx;
        size_t i_idx;
        void advance() {
            while (b_idx < set.capacity && i_idx >= set.buckets[b_idx].size()) {
                b_idx++; i_idx = 0;
            }
        }
        bool operator!=(const Iterator& other) const { return b_idx != other.b_idx || i_idx != other.i_idx; }
        const T& operator*() const { return set.buckets[b_idx][i_idx]; }
        Iterator& operator++() { i_idx++; advance(); return *this; }
    };

    Iterator begin() const { Iterator it{*this, 0, 0}; it.advance(); return it; }
    Iterator end() const { return {*this, capacity, 0}; }
};

template <typename K, typename V>
class HashMap {
private:
    struct Node { K key; V value; };
    std::vector<std::vector<Node>> buckets;
    size_t capacity;
    size_t num_elements;
    const double MAX_LOAD_FACTOR = 1.0;

    void rehash() {
        size_t new_capacity = capacity * 2 + 1;
        std::vector<std::vector<Node>> new_buckets(new_capacity);
        Hasher<K> hasher;
        for (auto& bucket : buckets) {
            for (auto& node : bucket) {
                new_buckets[hasher(node.key) % new_capacity].push_back(std::move(node));
            }
        }
        buckets = std::move(new_buckets);
        capacity = new_capacity;
    }

public:
    HashMap(size_t cap = 1009) : capacity(cap), num_elements(0) {
        buckets.resize(capacity);
    }

    HashMap(const HashMap& other) : buckets(other.buckets), capacity(other.capacity), num_elements(other.num_elements) {}
    
    HashMap& operator=(const HashMap& other) {
        if (this != &other) {
            buckets = other.buckets;
            capacity = other.capacity;
            num_elements = other.num_elements;
        }
        return *this;
    }

    V& operator[](const K& key) {
        Hasher<K> hasher;
        size_t idx = hasher(key) % capacity;
        for (auto& node : buckets[idx]) {
            if (node.key == key) return node.value;
        }
        if ((double)num_elements / capacity > MAX_LOAD_FACTOR) {
            rehash();
            idx = hasher(key) % capacity; 
        }
        buckets[idx].push_back({key, V()});
        num_elements++;
        return buckets[idx].back().value;
    }

    size_t size() const { return num_elements; }

    struct Iterator {
        const HashMap& map;
        size_t b_idx;
        size_t i_idx;
        void advance() {
            while (b_idx < map.capacity && i_idx >= map.buckets[b_idx].size()) {
                b_idx++; i_idx = 0;
            }
        }
        bool operator!=(const Iterator& other) const { return b_idx != other.b_idx || i_idx != other.i_idx; }
        const Node& operator*() const { return map.buckets[b_idx][i_idx]; }
        Iterator& operator++() { i_idx++; advance(); return *this; }
    };

    Iterator begin() const { Iterator it{*this, 0, 0}; it.advance(); return it; }
    Iterator end() const { return {*this, capacity, 0}; }
};

}

#endif