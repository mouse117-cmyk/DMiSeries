#include "third_party/thmap/thmap.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>
#include "utils.h"

int l_key = 0;
int r_key = 100;

int main() {
  thmap_t *kvmap;

  kvmap = thmap_create(0, nullptr, 0);
  assert(kvmap != nullptr);

  std::vector<int *> v;
  for (int i = l_key; i < r_key; i++) {
    auto obj = new int(i);
    v.emplace_back((int *)obj);
    thmap_put(kvmap, &i, sizeof(i), obj);
  }
  for (int i = l_key; i < r_key; i++) {
    auto obj = new int(i * 2);
    // dont put the already existing key
    auto old = thmap_put(kvmap, &i, sizeof(i), obj);
    assert(std::find(v.begin(), v.end(), old) != v.end());
  }
  v.clear();
  for (int i = l_key; i < r_key; i++) {
    auto obj = thmap_get(kvmap, &i, sizeof(i));
    v.emplace_back((int *)obj);
    // delete
    auto tmp = thmap_del(kvmap, &i, sizeof(i));
    assert(tmp == obj);
  }
  for (auto item : v) {
    std::cout << *item << std::endl;
  }

  thmap_destroy(kvmap);

  return 0;
}