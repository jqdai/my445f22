//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if (curr_size_ == 0 || history.empty()) {
        return false;
    }
    for (auto it = history.begin(); it != history.end(); it ++) {
        if (evictable[it->first]) {
            evictable.erase(it->first);
            curr_size_ --;
            *frame_id = it->first;
            history.erase(it);
            return true;
        }
    }
    return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "abort");
    if (evictable.find(frame_id) != evictable.end()) {
        for(auto it=history.begin(); it!=history.end(); it++) {
            if(it->first == frame_id) {
                for(auto it2=history.rbegin(); it2!=history.rend(); it2++) {
                    //如果遇到==k的,就跳过
                    if(it2->second == k_ && it->second+1 < k_) {
                        continue;
                    } else {
                        history.insert(it2.base(), std::make_pair(frame_id, it->second + 1));
                        history.erase(it);
                        break;
                    }
                }
                break;
            }
        }
    } else {
        if (curr_size_ == replacer_size_) {
            Evict(&frame_id);
        }
        SetEvictable(frame_id, true);
        if(history.size() != 0) {
            for(auto it = history.rbegin(); it != history.rend(); it ++) {
                if(it->second != k_) {
                    history.insert(it.base(), std::make_pair(frame_id, 1));
                    break;
                }
            }
        } else {
            history.push_back(std::make_pair(frame_id, 1));
        }
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "abort");
    if (evictable.find(frame_id) == evictable.end()) {
        return;
    }
    if (set_evictable && !evictable.at(frame_id)) {
        curr_size_ ++;
        evictable.at(frame_id) = true;
    } else if (!set_evictable && evictable.at(frame_id)) {
        curr_size_ --;
        evictable.at(frame_id) = false;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    BUSTUB_ASSERT(frame_id <= (int)replacer_size_, "abort");
    if (evictable.find(frame_id) != evictable.end()) {
        return ;
    }
    if (history.size() == 0) {
        return ;
    }
    if (!evictable.at(frame_id)) {
        abort();
    }
    evictable.erase(frame_id);
    for (auto it = history.begin(); it != history.end(); it ++) {
        if (it->first == frame_id) {
            history.erase(it);
            break;
        }
    }
    curr_size_ --;
}

auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_;
}

}  // namespace bustub
