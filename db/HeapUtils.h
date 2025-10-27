#pragma once

struct HeapNode {
  double prio;
  std::string id;
};

class MinHeap {
public:
    void push(const std::string &id,double prio) {
        if (posMap.find(id) != posMap.end()) {
            update(id, prio);
            return;
        }
        heap.push_back({prio, id});
        int index = heap.size() - 1;
        posMap[id] = index;
        siftUp(index);
    }

    HeapNode pop() {
        if (heap.empty()) {
            throw std::runtime_error("Heap is empty");
        }
        HeapNode minNode = heap[0];
        swapNodes(0, heap.size() - 1);
        posMap.erase(minNode.id);
        heap.pop_back();
        if (!heap.empty()) {
            siftDown(0);
        }
        return minNode;
    }

    const HeapNode &top() const {
        if (heap.empty()) {
            throw std::runtime_error("Heap is empty");
        }
        return heap[0];
    }

    void update(const std::string &id, double newPrio) {
        auto it = posMap.find(id);
        if (it == posMap.end()) {
            push(id,newPrio);
            return;
        }
        int index = it->second;
        double oldPrio = heap[index].prio;
        heap[index].prio = newPrio;
        if (newPrio < oldPrio) {
            siftUp(index);
        } else {
            siftDown(index);
        }
    }

    bool empty() const {
        return heap.empty();
    }

private:
    std::vector<HeapNode> heap;
    std::unordered_map<std::string, int> posMap;

    bool lessThan(const HeapNode &a, const HeapNode &b) {
        if (a.prio != b.prio) {
            return a.prio < b.prio;
        }
        return a.id < b.id;
    }

    void siftUp(int i) {
        while (i > 0) {
            int p = (i - 1) / 2;
            if (lessThan(heap[i], heap[p])) {
                swapNodes(i, p);
                i = p;
            } else {
                break;
            }
        }
    }

    void siftDown(int i) {
        int n = heap.size();
        while (true) {
            int left = i * 2 + 1;
            int right = i * 2 + 2;
            int smallest = i;
            if (left < n && lessThan(heap[left], heap[smallest])) {
                smallest = left;
            }
            if (right < n && lessThan(heap[right], heap[smallest])) {
                smallest = right;
            }
            if (smallest != i) {
                swapNodes(i, smallest);
                i = smallest;
            } else {
                break;
            }
        }
    }

    void swapNodes(int i, int j) {
        std::swap(heap[i], heap[j]);
        posMap[heap[i].id] = i;
        posMap[heap[j].id] = j;
    }
};

class MaxHeap {
public:
    void push(const std::string &id,double prio) {
        if (posMap.find(id) != posMap.end()) {
            update(id, prio);
            return;
        }
        heap.push_back({prio, id});
        int index = heap.size() - 1;
        posMap[id] = index;
        siftUp(index);
    }

    HeapNode pop() {
        if (heap.empty()) {
            throw std::runtime_error("Heap is empty");
        }
        HeapNode maxNode = heap[0];
        swapNodes(0, heap.size() - 1);
        posMap.erase(maxNode.id);
        heap.pop_back();
        if (!heap.empty()) {
            siftDown(0);
        }
        return maxNode;
    }

    const HeapNode &top() const {
        if (heap.empty()) {
            throw std::runtime_error("Heap is empty");
        }
        return heap[0];
    }

    void update(const std::string &id, double newPrio) {
        auto it = posMap.find(id);
        if (it == posMap.end()) {
            push(id, newPrio);
            return;
        }
        int index = it->second;
        double oldPrio = heap[index].prio;
        heap[index].prio = newPrio;
        if (newPrio > oldPrio) {
            siftUp(index);
        } else {
            siftDown(index);
        }
    }

    bool empty() const {
        return heap.empty();
    }

private:
    std::vector<HeapNode> heap;
    std::unordered_map<std::string, int> posMap;

    bool greaterThan(const HeapNode &a, const HeapNode &b) {
        if (a.prio != b.prio) {
            return a.prio > b.prio;
        }
        return a.id < b.id;
    }

    void siftUp(int i) {
        while (i > 0) {
            int p = (i - 1) / 2;
            if (greaterThan(heap[i], heap[p])) {
                swapNodes(i, p);
                i = p;
            } else {
                break;
            }
        }
    }

    void siftDown(int i) {
        int n = heap.size();
        while (true) {
            int left = i * 2 + 1;
            int right = i * 2 + 2;
            int largest = i;
            if (left < n && greaterThan(heap[left], heap[largest])) {
                largest = left;
            }
            if (right < n && greaterThan(heap[right], heap[largest])) {
                largest = right;
            }
            if (largest != i) {
                swapNodes(i, largest);
                i = largest;
            } else {
                break;
            }
        }
    }

    void swapNodes(int i, int j) {
        std::swap(heap[i], heap[j]);
        posMap[heap[i].id] = i;
        posMap[heap[j].id] = j;
    }
};