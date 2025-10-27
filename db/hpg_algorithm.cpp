#include "hpg_algorithm.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>

#include "HeapUtils.h"

Algorithm::Algorithm(int num_subgraphs,int bucket_num,std::vector<std::string> IPs,std::vector<int> ports){
  for (int i = 0; i < num_subgraphs; ++i) {
    subgraphs_.emplace_back(i,IPs[i],ports[i]);
  }
  this->bucket_num=bucket_num;
}

void Algorithm::addMetricNode(const std::string& id, double weight) {
  if (metrics_.count(id)>0) {
    metrics_.at(id).weight+=weight;
  }else {
    metrics_.emplace(id, MetricNode(id, weight));
  }
}

std::string Algorithm::addMetricNode(const tsdb::label::Labels& lset, double weight) {
  std::string result=selectSeriesGroup(lset);
  // std::cout<<"result: "<<result<<std::endl;
  addMetricNode(result,weight);
  for (auto& q : queries_) {
    bool flag=true;
    for (auto& q_label : q.second.q_labels) {
      // std::cout<<"q_label: "<<q_label.first<<" "<<q_label.second<<std::endl;
      bool found=false;
      for (auto& label : lset) {
        // std::cout<<"l_label: "<<label.label<<" "<<label.value<<std::endl;
        if (label.label==q_label.first&&label.value==q_label.second) {
          found=true;
          break;
        }
      }
      if (!found) {
        flag=false;
        break;
      }
    }
    if (flag) {
      addEdge(result,q.first);
    }
  }
  return result;
}

void Algorithm::addQueryNode(const std::string& id, double weight,std::unordered_map<std::string,std::string>& q_labels) {
  queries_.emplace(id, QueryNode(id, weight,q_labels));
  for (auto& sub : subgraphs_) {
    httplib::Client cli(sub.IP,sub.port);
    prometheus::ReadRequest readRequest;
    prometheus::Query* query = readRequest.add_queries();

    for (auto q : q_labels) {
      prometheus::LabelMatcher* matcher = query->add_matchers();
      matcher->set_name(q.first);
      matcher->set_value(q.second);
    }

    std::string request_data, compressed_request_data;
    readRequest.SerializeToString(&request_data);
    snappy::Compress(request_data.data(), request_data.size(),
                     &compressed_request_data);
    httplib::Result result=cli.Post("/query", compressed_request_data, "text/plain");

    std::string response_data;
    prometheus::ReadResponse readResponse;
    snappy::Uncompress(result->body.data(), result->body.size(), &response_data);
    if (readResponse.results_size() > 0 && readResponse.results(0).timeseries().size()>0) {
      for (auto m_id : sub.metrics) {
        addEdge(m_id,id);
      }
    }
  }
}

void Algorithm::addQueryNodeSD(const std::string& id, double weight,std::unordered_map<std::string,std::string>& q_labels) {
  queries_.emplace(id, QueryNode(id, weight,q_labels));
}

void Algorithm::addEdge(const std::string& metric_id,
                        const std::string& query_id) {
  if (metrics_.find(metric_id) != metrics_.end() &&
      queries_.find(query_id) != queries_.end()) {
    metrics_.at(metric_id).query_neighbors.insert(query_id);
    queries_.at(query_id).metric_neighbors.insert(metric_id);
  }
}

int Algorithm::calculateCorrelation(const std::string& m1,
                                    const std::string& m2) {
  const auto& neighbors1 = metrics_.at(m1).query_neighbors;
  const auto& neighbors2 = metrics_.at(m2).query_neighbors;

  int common = 0;
  for (const auto& q : neighbors1) {
    if (neighbors2.count(q)) {
      common++;
    }
  }
  return common;
}

int Algorithm::calculateSubgraphCorrelation(const std::string& metric_id,
                                            int subgraph_id) {
  const auto& subgraph = subgraphs_[subgraph_id];
  if (subgraph.metrics.empty()) {
    return 0;
  }

  int total_corr = 0;
  for (const auto& m : subgraph.metrics) {
    total_corr += calculateCorrelation(metric_id, m);
  }

  return total_corr;
}

std::string Algorithm::selectSeriesGroup(const tsdb::label::Labels& lset) {
  std::set<size_t> hash_set;
  std::string name_value;
  for (const auto& label : lset) {
    if (label.label=="__name__") {
      name_value=label.value;
      continue;
    }
    std::string label_value = label.label + label.value;
    hash_set.insert(std::hash<std::string>{}(label_value));
  }
  size_t hash_val = 0;
  for (const auto& hash : hash_set) {
    hash_val ^= hash;
  }
  hash_val = hash_val % bucket_num;
  std::string metric_id="SG"+std::to_string(hash_val)+":"+name_value;
  return metric_id;
}

// std::string Algorithm::selectSeriesGroup(const tsdb::label::Labels& lset) {
//   std::string name_value;
//   std::vector<std::string> kv_pairs;
//   for (const auto& label : lset) {
//     if (label.label == "__name__") name_value = label.value;
//     else kv_pairs.emplace_back(label.label + "=" + label.value);
//   }
//
//   std::sort(kv_pairs.begin(), kv_pairs.end());
//   std::string concat;
//   for (const auto& kv : kv_pairs) concat += kv + ";";
//
//   std::hash<std::string> hasher;
//   size_t hash_val = hasher(concat) % bucket_num;
//
//   return "SG" + std::to_string(hash_val) + ":" + name_value;
// }

void Algorithm::HPG() {
  MaxHeap max_heap;
  std::unordered_map<std::string, double> priority;
  for (const auto& [id, metric] : metrics_) {
    priority[id] = 0.0;
  }

  for (const auto& [id, _] : metrics_) {
    max_heap.push(id, priority[id]);
  }

  std::unordered_set<std::string> assigned_metrics;

  while (!max_heap.empty()) {
    std::string current_metric = max_heap.pop().id;

    if (assigned_metrics.count(current_metric)) {
      continue;
    }

    std::vector<double> current_wloads;
    for (auto& sg : subgraphs_) {
      current_wloads.push_back(sg.w_load);
    }
    double min_wload =
        *std::min_element(current_wloads.begin(), current_wloads.end());

    double metric_weight = metrics_.at(current_metric).weight;
    double max_candidate_wload = min_wload + 1*metric_weight;

    std::vector<int> candidate_subgraphs;
    for (size_t i = 0; i < subgraphs_.size(); ++i) {
      if (subgraphs_[i].w_load <= max_candidate_wload) {
        candidate_subgraphs.push_back(i);
      }
    }

    int best_subgraph = -1;
    double best_correlation = -1.0;

    for (int sg_id : candidate_subgraphs) {
      double corr = calculateSubgraphCorrelation(current_metric, sg_id);

      if (corr > best_correlation ||
          (corr == best_correlation &&
           subgraphs_[sg_id].w_load < subgraphs_[best_subgraph].w_load)) {
        best_correlation = corr;
        best_subgraph = sg_id;
      }
    }
    if (best_subgraph == -1) {
      best_subgraph = std::distance(
          current_wloads.begin(),
          std::min_element(current_wloads.begin(), current_wloads.end()));
    }

    subgraphs_[best_subgraph].metrics.insert(current_metric);
    subgraphs_[best_subgraph].w_load += metric_weight;
    metric_assignments_[current_metric] = best_subgraph;
    assigned_metrics.insert(current_metric);

    for (const auto& q_id : metrics_.at(current_metric).query_neighbors) {
      for (const auto& m_id : queries_.at(q_id).metric_neighbors) {
        if (!assigned_metrics.count(m_id)) {
          priority[m_id] -= metric_weight;
          max_heap.update(m_id, priority[m_id]);
        }
      }
    }
  }


  for (const auto& [q_id, query] : queries_) {
    std::map<int, int> subgraph_counts;
    for (const auto& m_id : query.metric_neighbors) {
      int sg_id = metric_assignments_[m_id];
      subgraph_counts[sg_id]++;
    }

    int best_subgraph = -1;
    int max_count = -1;
    double min_load = std::numeric_limits<double>::max();

    for (const auto& [sg_id, count] : subgraph_counts) {
      if (count > max_count ||
          (count == max_count && subgraphs_[sg_id].q_load < min_load)) {
        max_count = count;
        min_load = subgraphs_[sg_id].q_load;
        best_subgraph = sg_id;
      }
    }

    // std::cout<<"best_subgraph: "<<best_subgraph<<std::endl;
    // if (query.metric_neighbors.empty()) {
    //   std::cerr << "Warning: Query " << q_id << " has no metric neighbors" << std::endl;
    // }
    if (best_subgraph==-1) {
      std::cout << "No metrics found, allocated to sub0" << std::endl;
      best_subgraph=0;
    }
    subgraphs_[best_subgraph].queries.insert(q_id);
    query_assignments_[q_id] = best_subgraph;

    double metric_weight_sum = 0.0;
    for (const auto& m_id : query.metric_neighbors) {
      metric_weight_sum += metrics_.at(m_id).weight;
    }
    subgraphs_[best_subgraph].q_load += query.weight;

  }
}

const std::vector<Subgraph>& Algorithm::getSubgraphs() const {
  return subgraphs_;
}

const std::unordered_map<std::string, int>& Algorithm::getMetricAssignments()
    const {
  return metric_assignments_;
}

const std::unordered_map<std::string, int>& Algorithm::getQueryAssignments()
    const {
  return query_assignments_;
}

void Algorithm::printResults() const {
  std::cout << "HPG Algorithm HPG Results:\n" << std::endl;

  for (const auto& sg : subgraphs_) {
    std::cout << "Subgraph " << sg.id << ":" << std::endl;
    std::cout << "  WLoad: " << std::fixed << std::setprecision(2) << sg.w_load
              << std::endl;
    std::cout << "  QLoad: " << std::fixed << std::setprecision(2) << sg.q_load
              << std::endl;

    std::cout << "  Metrics: ";
    for (const auto& m : sg.metrics) {
      std::cout << m << " ";
    }
    std::cout << std::endl;

    std::cout << "  Queries: ";
    for (const auto& q : sg.queries) {
      std::cout << q << " ";
    }
    std::cout << "\n" << std::endl;
  }

  int cross_node_queries = 0;
  for (const auto& [q_id, query] : queries_) {
    int subgraph_id = query_assignments_.at(q_id);
    bool has_cross = false;

    for (const auto& m_id : query.metric_neighbors) {
      if (metric_assignments_.at(m_id) != subgraph_id) {
        has_cross = true;
        break;
      }
    }

    if (has_cross) {
      cross_node_queries++;
    }
  }
}

double calculateTotalMetricWeight(
    const std::unordered_map<std::string, MetricNode>& metrics) {
  double total = 0.0;
  for (const auto& [_, metric] : metrics) {
    total += metric.weight;
  }
  return total;
}

void Algorithm::GRP4U() {
  int N = subgraphs_.size();
  if (N == 0 || metrics_.empty()) {
    std::cerr << "Error: No subgraphs or metrics exist for reHPGing."
              << std::endl;
    return;
  }

  double total_metric_weight = calculateTotalMetricWeight(metrics_);
  double mu_W = total_metric_weight / N;
  std::cout << "Ideal average write load (μW): " << std::fixed
            << std::setprecision(2) << mu_W << std::endl;

  std::unordered_set<std::string> G_prime_metrics;
  std::unordered_map<std::string, MetricNode> G_prime_metric_map;

  for (auto& sg : subgraphs_) {
    if (sg.w_load <= mu_W) {
      continue;
    }

    double target_remove = sg.w_load - mu_W;
    double sum_removed = 0.0;
    std::cout << "\nSubgraph " << sg.id
              << " is overloaded (WLoad: " << sg.w_load
              << "). Need to remove total weight: " << target_remove
              << std::endl;

    using MetricCorrPair = std::pair<int, std::string>;

    MaxHeap sg_max_heap;

    for (const auto& m_id : sg.metrics) {
      int corr = calculateSubgraphCorrelation(m_id, sg.id);
      sg_max_heap.push(m_id, corr);
    }

    while (!sg_max_heap.empty() && sum_removed < target_remove) {
      auto [curr_corr, curr_m_id] = sg_max_heap.top();
      sg_max_heap.pop();

      const auto& curr_metric = metrics_.at(curr_m_id);
      double m_weight = curr_metric.weight;

      if (sum_removed + m_weight > target_remove &&
          sum_removed < target_remove) {
        continue;
      }

      sg.metrics.erase(curr_m_id);
      sg.w_load -= m_weight;
      sum_removed += m_weight;

      G_prime_metrics.insert(curr_m_id);
      G_prime_metric_map.emplace(curr_m_id, curr_metric);

      metric_assignments_.erase(curr_m_id);

      std::cout << "Removed Metric " << curr_m_id << " (weight: " << m_weight
                << ", corr: " << curr_corr << ") from Subgraph " << sg.id
                << ". Removed sum: " << sum_removed
                << ", Remaining WLoad: " << sg.w_load << std::endl;
    }
  }

  if (G_prime_metrics.empty()) {
    std::cout << "\nNo metrics need reallocation. ReHPG for update completed."
              << std::endl;
    return;
  }
  std::cout << "\nTotal metrics to reallocate in G': " << G_prime_metrics.size()
            << std::endl;

  std::unordered_map<std::string, double> g_prime_priority;
  for (const auto& m_id : G_prime_metrics) {
    g_prime_priority[m_id] = 0.0;
  }

  MaxHeap g_prime_heap;
  for (const auto& m_id : G_prime_metrics) {
    g_prime_heap.push(m_id, g_prime_priority[m_id]);
  }

  std::unordered_set<std::string> reallocated_metrics;
  while (!g_prime_heap.empty()) {
    std::string current_m_id = g_prime_heap.top().id;
    g_prime_heap.pop();

    if (reallocated_metrics.count(current_m_id)) {
      continue;
    }

    std::vector<double> sg_wloads;
    for (const auto& sg : subgraphs_) {
      sg_wloads.push_back(sg.w_load);
    }
    double min_wload = *std::min_element(sg_wloads.begin(), sg_wloads.end());
    double current_m_weight = G_prime_metric_map.at(current_m_id).weight;
    double max_candidate_wload = min_wload + current_m_weight;

    std::vector<int> candidate_sg_ids;
    for (size_t i = 0; i < subgraphs_.size(); ++i) {
      if (subgraphs_[i].w_load <= max_candidate_wload) {
        candidate_sg_ids.push_back(i);
      }
    }

    int best_sg_id = -1;
    int best_corr = -1;
    for (int sg_id : candidate_sg_ids) {
      int curr_corr = calculateSubgraphCorrelation(current_m_id, sg_id);
      if (curr_corr > best_corr ||
          (curr_corr == best_corr &&
           subgraphs_[sg_id].w_load < subgraphs_[best_sg_id].w_load)) {
        best_corr = curr_corr;
        best_sg_id = sg_id;
      }
    }

    if (best_sg_id == -1) {
      best_sg_id =
          std::distance(sg_wloads.begin(),
                        std::min_element(sg_wloads.begin(), sg_wloads.end()));
    }

    auto& target_sg = subgraphs_[best_sg_id];
    target_sg.metrics.insert(current_m_id);
    target_sg.w_load += current_m_weight;
    metric_assignments_[current_m_id] = best_sg_id;
    reallocated_metrics.insert(current_m_id);

    std::cout << "Reallocated Metric " << current_m_id
              << " (weight: " << current_m_weight << ") to Subgraph "
              << best_sg_id << " (corr: " << best_corr
              << ", new WLoad: " << target_sg.w_load << ")" << std::endl;

    const auto& current_metric = G_prime_metric_map.at(current_m_id);
    for (const auto& q_id : current_metric.query_neighbors) {
      if (queries_.find(q_id) == queries_.end()) {
        continue;
      }
      for (const auto& related_m_id : queries_.at(q_id).metric_neighbors) {
        if (G_prime_metrics.count(related_m_id) &&
            !reallocated_metrics.count(related_m_id)) {
          g_prime_priority[related_m_id] -= current_m_weight;
          g_prime_heap.push(related_m_id, g_prime_priority[related_m_id]);
        }
      }
    }
  }

  std::cout << "\nReallocating QueryNodes after Metric reallocation..."
            << std::endl;
  for (auto& [q_id, query] : queries_) {
    std::map<int, int> sg_metric_count;
    for (const auto& m_id : query.metric_neighbors) {
      if (metric_assignments_.find(m_id) == metric_assignments_.end()) {
        continue;
      }
      int sg_id = metric_assignments_.at(m_id);
      sg_metric_count[sg_id]++;
    }

    int best_sg_id = -1;
    int max_metric_count = -1;
    double min_sg_qload = std::numeric_limits<double>::max();
    for (const auto& [sg_id, count] : sg_metric_count) {
      const auto& sg = subgraphs_[sg_id];
      if (count > max_metric_count ||
          (count == max_metric_count && sg.q_load < min_sg_qload)) {
        max_metric_count = count;
        min_sg_qload = sg.q_load;
        best_sg_id = sg_id;
      }
    }

    if (best_sg_id == -1) {
      auto min_qload_it =
          std::min_element(subgraphs_.begin(), subgraphs_.end(),
                           [](const Subgraph& a, const Subgraph& b) {
                             return a.q_load < b.q_load;
                           });
      best_sg_id = min_qload_it->id;
    }

    auto& target_sg = subgraphs_[best_sg_id];

    if (query_assignments_.count(q_id)) {
      int old_sg_id = query_assignments_[q_id];
      subgraphs_[old_sg_id].queries.erase(q_id);
      subgraphs_[old_sg_id].q_load -= query.weight;
    }

    target_sg.queries.insert(q_id);
    target_sg.q_load += query.weight;
    query_assignments_[q_id] = best_sg_id;

    std::cout << "Reallocated Query " << q_id << " (weight: " << query.weight
              << ") to Subgraph " << best_sg_id
              << " (related metrics count: " << max_metric_count
              << ", new QLoad: " << target_sg.q_load << ")" << std::endl;
  }

  std::cout << "\nGRP4U (Graph ReHPGing for Update) completed successfully."
            << std::endl;
}

int calculateSubgraphOverlap(const Subgraph& sg1, const Subgraph& sg2) {
  int overlap_count = 0;
  const auto& smaller_metrics =
      (sg1.metrics.size() <= sg2.metrics.size()) ? sg1.metrics : sg2.metrics;
  const auto& larger_metrics =
      (sg1.metrics.size() > sg2.metrics.size()) ? sg1.metrics : sg2.metrics;

  for (const auto& m_id : smaller_metrics) {
    if (larger_metrics.count(m_id)) {
      overlap_count++;
    }
  }
  return overlap_count;
}

void Algorithm::RPG4I(int new_num_subgraphs,std::vector<std::string> IPs,std::vector<int> ports) {
  int original_num = subgraphs_.size();
  if (new_num_subgraphs <= original_num || new_num_subgraphs <= 0) {
    std::cerr << "Error: New number of subgraphs (" << new_num_subgraphs
              << ") must be greater than original (" << original_num << ")."
              << std::endl;
    return;
  }
  std::cout << "Starting RPG4I: Scale from " << original_num << " to "
            << new_num_subgraphs << " subgraphs." << std::endl;

  std::vector<Subgraph> original_subgraphs = subgraphs_;
  std::unordered_map<std::string, int> original_metric_assignments =
      metric_assignments_;
  std::unordered_map<std::string, int> original_query_assignments =
      query_assignments_;

  std::vector<Subgraph> temp_subgraphs = std::move(subgraphs_);
  std::unordered_map<std::string, int> temp_metric_assign =
      std::move(metric_assignments_);
  std::unordered_map<std::string, int> temp_query_assign =
      std::move(query_assignments_);

  subgraphs_.clear();
  for (int i = 0; i < original_num; ++i) {
    subgraphs_.emplace_back(i,original_subgraphs[i].IP,original_subgraphs[i].port);
  }
  int tmp_count = 0;
  for (int i = original_num; i < new_num_subgraphs; ++i) {
    subgraphs_.emplace_back(i,IPs[tmp_count],ports[tmp_count]);
    tmp_count++;
  }
  tmp_count=0;
  metric_assignments_.clear();
  query_assignments_.clear();

  this->HPG();
  std::vector<Subgraph> lset_prime = subgraphs_;
  std::cout << "Successfully generated LSet' with " << lset_prime.size()
            << " subgraphs via HPG." << std::endl;

  subgraphs_ = std::move(original_subgraphs);
  metric_assignments_ = std::move(original_metric_assignments);
  query_assignments_ = std::move(original_query_assignments);

  std::unordered_set<int> used_lset_prime_ids;
  for (auto& original_sg : subgraphs_) {
    int max_overlap = -1;
    int best_lset_prime_id = -1;

    for (const auto& prime_sg : lset_prime) {
      if (used_lset_prime_ids.count(prime_sg.id)) {
        continue;
      }

      int current_overlap = calculateSubgraphOverlap(original_sg, prime_sg);
      if (current_overlap > max_overlap ||
          (current_overlap == max_overlap &&
           prime_sg.w_load < lset_prime[best_lset_prime_id].w_load)) {
        max_overlap = current_overlap;
        best_lset_prime_id = prime_sg.id;
      }
    }

    if (best_lset_prime_id != -1) {
      const auto& matched_prime_sg = lset_prime[best_lset_prime_id];
      original_sg.metrics = matched_prime_sg.metrics;
      original_sg.queries = matched_prime_sg.queries;
      original_sg.w_load = matched_prime_sg.w_load;
      original_sg.q_load = matched_prime_sg.q_load;

      for (const auto& m_id : original_sg.metrics) {
        metric_assignments_[m_id] = original_sg.id;
      }
      for (const auto& q_id : original_sg.queries) {
        query_assignments_[q_id] = original_sg.id;
      }

      used_lset_prime_ids.insert(best_lset_prime_id);
      std::cout << "Original Subgraph " << original_sg.id
                << " matched LSet' Subgraph " << best_lset_prime_id
                << " (overlap: " << max_overlap << " metrics)." << std::endl;
    }
  }

  int new_sg_counter = original_num;
  for (const auto& prime_sg : lset_prime) {
    if (used_lset_prime_ids.count(prime_sg.id)) {
      continue;
    }

    Subgraph new_sg(new_sg_counter,IPs[tmp_count],ports[tmp_count]);
    tmp_count++;
    new_sg.metrics = prime_sg.metrics;
    new_sg.queries = prime_sg.queries;
    new_sg.w_load = prime_sg.w_load;
    new_sg.q_load = prime_sg.q_load;

    for (const auto& m_id : new_sg.metrics) {
      metric_assignments_[m_id] = new_sg.id;
    }
    for (const auto& q_id : new_sg.queries) {
      query_assignments_[q_id] = new_sg.id;
    }

    subgraphs_.push_back(new_sg);
    std::cout << "Added new Subgraph " << new_sg.id << " (from LSet' Subgraph "
              << prime_sg.id << ", metrics: " << new_sg.metrics.size()
              << ", queries: " << new_sg.queries.size() << ")." << std::endl;

    new_sg_counter++;
    if (subgraphs_.size() >= new_num_subgraphs) {
      break;
    }
  }

  if (subgraphs_.size() != new_num_subgraphs) {
    std::cerr << "Warning: Final subgraph count (" << subgraphs_.size()
              << ") does not match expected (" << new_num_subgraphs << ")."
              << std::endl;
  } else {
    std::cout << "RPG4I completed successfully. Final subgraph count: "
              << subgraphs_.size() << std::endl;
  }
}

void Algorithm::RPG4D(const std::unordered_set<int>& failed_subgraph_ids,
                      int target_num_subgraphs) {
  int original_num = subgraphs_.size();
  if (target_num_subgraphs !=
          original_num - static_cast<int>(failed_subgraph_ids.size()) ||
      target_num_subgraphs < 1) {
    std::cerr << "Error: Invalid target subgraph count. Original: "
              << original_num << ", Failed: " << failed_subgraph_ids.size()
              << ", Target required: "
              << original_num - failed_subgraph_ids.size()
              << ", Got: " << target_num_subgraphs << std::endl;
    return;
  }
  std::cout << "Starting RPG4D: Scale in from " << original_num << " to "
            << target_num_subgraphs << " subgraphs. Failed subgraphs: ";
  for (int id : failed_subgraph_ids) std::cout << id << " ";
  std::cout << std::endl;

  std::vector<Subgraph> valid_subgraphs;
  std::unordered_set<std::string> G_prime_metrics;
  std::unordered_set<std::string> G_prime_queries;
  std::unordered_map<std::string, MetricNode> G_prime_metric_map;

  for (const auto& sg : subgraphs_) {
    if (failed_subgraph_ids.count(sg.id)) {
      for (const auto& m_id : sg.metrics) {
        G_prime_metrics.insert(m_id);
        G_prime_metric_map.emplace(m_id, metrics_.at(m_id));
      }
      for (const auto& q_id : sg.queries) {
        G_prime_queries.insert(q_id);
      }
      std::cout << "Failed Subgraph " << sg.id
                << " processed: " << sg.metrics.size() << " metrics, "
                << sg.queries.size() << " queries added to G'." << std::endl;
    } else {
      valid_subgraphs.push_back(sg);
    }
  }

  double total_metric_weight = calculateTotalMetricWeight(metrics_);
  double mu_W = total_metric_weight / target_num_subgraphs;
  std::cout << "Ideal average write load (μW) for valid subgraphs: "
            << std::fixed << std::setprecision(2) << mu_W << std::endl;

  std::unordered_map<std::string, double> metric_priority;
  for (const auto& [m_id, m_node] : G_prime_metric_map) {
    double total_corr = 0.0;
    for (const auto& valid_sg : valid_subgraphs) {
      total_corr += calculateSubgraphCorrelation(m_id, valid_sg.id);
    }
    metric_priority[m_id] = total_corr;
  }

  MaxHeap metric_heap;
  for (const auto& m_id : G_prime_metrics) {
    metric_heap.push(m_id, metric_priority[m_id]);
  }

  std::unordered_set<std::string> assigned_metrics;
  while (!metric_heap.empty()) {
    std::string current_m_id = metric_heap.top().id;
    metric_heap.pop();

    if (assigned_metrics.count(current_m_id)) continue;
    const auto& current_m = G_prime_metric_map.at(current_m_id);
    double m_weight = current_m.weight;

    std::vector<size_t> candidate_sg_indices;
    for (size_t i = 0; i < valid_subgraphs.size(); ++i) {
      if (valid_subgraphs[i].w_load + m_weight <= mu_W) {
        candidate_sg_indices.push_back(i);
      }
    }

    if (candidate_sg_indices.empty()) {
      auto min_it =
          std::min_element(valid_subgraphs.begin(), valid_subgraphs.end(),
                           [](const Subgraph& a, const Subgraph& b) {
                             return a.w_load < b.w_load;
                           });
      candidate_sg_indices.push_back(
          std::distance(valid_subgraphs.begin(), min_it));
    }

    size_t best_sg_idx = candidate_sg_indices[0];
    int max_corr = -1;
    for (size_t idx : candidate_sg_indices) {
      int curr_corr =
          calculateSubgraphCorrelation(current_m_id, valid_subgraphs[idx].id);
      if (curr_corr > max_corr ||
          (curr_corr == max_corr &&
           valid_subgraphs[idx].w_load < valid_subgraphs[best_sg_idx].w_load)) {
        max_corr = curr_corr;
        best_sg_idx = idx;
      }
    }

    auto& target_sg = valid_subgraphs[best_sg_idx];
    target_sg.metrics.insert(current_m_id);
    target_sg.w_load += m_weight;
    metric_assignments_[current_m_id] = target_sg.id;
    assigned_metrics.insert(current_m_id);

    std::cout << "Assigned Metric " << current_m_id << " (weight: " << m_weight
              << ") to Valid Subgraph " << target_sg.id
              << " (corr: " << max_corr << ", new WLoad: " << std::fixed
              << std::setprecision(2) << target_sg.w_load << ")." << std::endl;
  }

  std::cout << "\nReallocating G' Query nodes to valid subgraphs..."
            << std::endl;
  for (const auto& q_id : G_prime_queries) {
    if (queries_.find(q_id) == queries_.end()) continue;
    const auto& q_node = queries_.at(q_id);

    std::unordered_map<int, int> sg_metric_count;
    for (const auto& m_id : q_node.metric_neighbors) {
      if (metric_assignments_.find(m_id) == metric_assignments_.end()) continue;
      int sg_id = metric_assignments_.at(m_id);
      sg_metric_count[sg_id]++;
    }

    int best_sg_id = -1;
    int max_count = -1;
    double min_q_load = std::numeric_limits<double>::max();
    for (const auto& [sg_id, count] : sg_metric_count) {
      auto sg_it =
          std::find_if(valid_subgraphs.begin(), valid_subgraphs.end(),
                       [sg_id](const Subgraph& sg) { return sg.id == sg_id; });
      if (sg_it == valid_subgraphs.end()) continue;
      double sg_qload = sg_it->q_load;

      if (count > max_count || (count == max_count && sg_qload < min_q_load)) {
        max_count = count;
        min_q_load = sg_qload;
        best_sg_id = sg_id;
      }
    }

    if (best_sg_id == -1) {
      auto min_qload_it =
          std::min_element(valid_subgraphs.begin(), valid_subgraphs.end(),
                           [](const Subgraph& a, const Subgraph& b) {
                             return a.q_load < b.q_load;
                           });
      best_sg_id = min_qload_it->id;
    }

    auto& target_sg = *std::find_if(
        valid_subgraphs.begin(), valid_subgraphs.end(),
        [best_sg_id](const Subgraph& sg) { return sg.id == best_sg_id; });
    target_sg.queries.insert(q_id);
    target_sg.q_load += q_node.weight;
    query_assignments_[q_id] = best_sg_id;

    std::cout << "Assigned Query " << q_id << " (weight: " << q_node.weight
              << ") to Valid Subgraph " << best_sg_id
              << " (related metrics count: " << max_count
              << ", new QLoad: " << std::fixed << std::setprecision(2)
              << target_sg.q_load << ")." << std::endl;
  }

  subgraphs_ = std::move(valid_subgraphs);
  std::cout << "\nRPG4D completed successfully. Final valid subgraph count: "
            << subgraphs_.size() << std::endl;
}

void Algorithm::clear() {
  metrics_.clear();
  queries_.clear();
  subgraphs_.clear();
  metric_assignments_.clear();
  query_assignments_.clear();
}
