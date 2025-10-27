#pragma once

#include <functional>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "db/SpanRemoteDB.h"

struct MetricNode;
struct QueryNode;
struct Subgraph;

struct Node {
  int node_id;
  std::unordered_set<int> assigned_shards;
  std::string IP;
  int port;
  std::string sep_dbpath;
  std::string dbpath;
  std::string logpath;
  int data_point_count=0;

  Node(int id,std::string IP,int port,std::string sep_dbpath,std::string dbpath,std::string logpath);

};

struct MetricNode {
  std::string id;
  double weight;
  std::unordered_set<std::string> query_neighbors;

  MetricNode(std::string id, double weight)
      : id(id), weight(weight) {}
};

struct QueryNode {
  std::string id;
  double weight;
  std::unordered_map<std::string,std::string> q_labels;
  std::unordered_set<std::string> metric_neighbors;

  QueryNode(const std::string& id, double weight,std::unordered_map<std::string,std::string>& labels)
      : id(id), weight(weight),q_labels(labels) {}
};

struct Subgraph {
  int id;
  std::unordered_set<std::string> metrics;
  std::unordered_set<std::string> queries;
  double w_load;
  double q_load;
  int data_point_count;
  std::string IP;
  int port;

  explicit Subgraph(int id,std::string IP,int port) : id(id),IP(IP),port(port), w_load(0.0), q_load(0.0),data_point_count(0) {}
};

class Algorithm {
 public:
  std::unordered_map<std::string, MetricNode> metrics_;
  std::unordered_map<std::string, QueryNode> queries_;

  std::vector<Subgraph> subgraphs_;

  std::unordered_map<std::string, int> metric_assignments_;
  std::unordered_map<std::string, int> query_assignments_;

  int bucket_num=20;

  int calculateCorrelation(const std::string& m1, const std::string& m2);

  int calculateSubgraphCorrelation(const std::string& metric_id,
                                      int subgraph_id);

  explicit Algorithm(int num_subgraphs,int bucket_num,std::vector<std::string> IPs,std::vector<int> ports);
  void addMetricNode(const std::string& id, double weight);
  std::string addMetricNode(const tsdb::label::Labels& lset, double weight);
  void addQueryNode(const std::string& id, double weight,std::unordered_map<std::string,std::string>& q_labels);
  void addQueryNodeSD(const std::string& id, double weight,std::unordered_map<std::string,std::string>& q_labels);
  void addEdge(const std::string& metric_id, const std::string& query_id);
  std::string selectSeriesGroup(const tsdb::label::Labels& lset);
  void HPG();
  const std::vector<Subgraph>& getSubgraphs() const;
  const std::unordered_map<std::string, int>& getMetricAssignments() const;
  const std::unordered_map<std::string, int>& getQueryAssignments() const;
  void printResults() const;
  void GRP4U();
  void RPG4I(int new_num_subgraphs,std::vector<std::string> IPs,std::vector<int> ports);
  void RPG4D(const std::unordered_set<int>& failed_subgraph_ids, int target_num_subgraphs);
  void clear();
};
