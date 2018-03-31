#include <map>
#include <string>
#include <list>
#include <functional> 
#include <algorithm>

using namespace::std;

#ifndef __CONSISTENT_HASH_H__
#define __CONSISTENT_HASH_H__



template <typename T,
        typename Hash,
        typename Alloc = std::allocator<std::pair<const typename Hash::result_type,T > > >
class consistent_hash_map
{
public:
 
    typedef typename Hash::result_type size_type;
    typedef std::map<size_type,T,std::less<size_type>,Alloc> map_type;
    typedef typename map_type::value_type value_type;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef typename map_type::iterator iterator;
    typedef typename map_type::reverse_iterator reverse_iterator;
    typedef Alloc allocator_type;

public:
    
    consistent_hash_map() {

    }

    ~consistent_hash_map() {

    }

public:
    std::size_t size() const {
        return nodes_.size();
    }

    bool empty() const {
        return nodes_.empty();
    }

    std::pair<iterator,bool> insert(const T& node) {
        size_type hash = hasher_(node);
        return nodes_.insert(value_type(hash,node));
    }


    void erase(iterator it) {
        nodes_.erase(it);
    }


    std::size_t erase(const T& node) {
        size_type hash = hasher_(node);
        return nodes_.erase(hash);
    }

    iterator find(size_type hash) {
        if(nodes_.empty()) {
            return nodes_.end();
        }

        iterator it = nodes_.lower_bound(hash);

        if (it == nodes_.end()) {
            it = nodes_.begin();
        }

        return it;
    }

    iterator find(string key) {
        return find(hasher_(key));
    }

    iterator begin() { return nodes_.begin(); }
    iterator end() { return nodes_.end(); }
    reverse_iterator rbegin() { return nodes_.rbegin(); }
    reverse_iterator rend() { return nodes_.rend(); }


private:

    Hash hasher_;
    map_type nodes_;
};


#endif
