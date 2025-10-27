#define GLOBAL_VALUE_DEFINE

#include <future>
#include <thread>

#include "db/SpanRemoteDB.h"
#include "gtest/gtest.h"
#include "hpg_algorithm.h"
#include "util/testutil.h"
#include <fstream>
#include "db/JSONUtils.h"
using json = nlohmann::json;

std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<> dis(1, 5);

int g_arg_1=0;
int g_arg_2=0;
int g_arg_3=0;

namespace tsdb {
std::string trim(const std::string& s) {
    auto start = s.begin();
    while (start != s.end() && std::isspace(static_cast<unsigned char>(*start))) {
        start++;
    }
    auto end = s.end();
    do {
        end--;
    } while (std::distance(start, end) > 0 && std::isspace(static_cast<unsigned char>(*end)));
    return std::string(start, end + 1);
}

void parseConfigFile(const std::string& configPath, std::vector<std::string>& ips, std::vector<int>& ports) {
  std::ifstream file(configPath);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + configPath);
  }

  json config_json;
  try {
    file >> config_json;
  } catch (const json::parse_error& e) {
    throw std::runtime_error("JSON parse error: " + std::string(e.what()));
  }
  file.close();

  if (!config_json.contains("ips") || !config_json["ips"].is_array()) {
    throw std::runtime_error("Invalid JSON: missing or invalid 'ips' array");
  }

  ips = config_json["ips"].get<std::vector<std::string>>();

  if (!config_json.contains("ports") || !config_json["ports"].is_array()) {
    throw std::runtime_error("Invalid JSON: missing or invalid 'ports' array");
  }

  ports = config_json["ports"].get<std::vector<int>>();


  if (ips.size() != ports.size()) {
    throw std::runtime_error("IP count (" + std::to_string(ips.size()) +
                            ") does not match port count (" + std::to_string(ports.size()) + ")");
  }
  if (ips.empty()) {
    throw std::runtime_error("Config file has no IP:port pairs");
  }
}

struct InfluxLine {
  std::string measurement;
  std::map<std::string, std::string> labels;
  std::map<std::string, std::string> fields;
  std::string timestamp;
};

InfluxLine parseInfluxLineUnified(const std::string& line) {
  InfluxLine result;
  std::string part1, part2, part3;

  std::istringstream iss(line);
  std::getline(iss, part1, ' ');
  std::getline(iss, part2, ' ');
  std::getline(iss, part3, ' ');

  size_t pos = part1.find(',');
  result.measurement =
      (pos == std::string::npos) ? part1 : part1.substr(0, pos);

  auto parseKeyValueList = [&](const std::string& str,
                               std::map<std::string, std::string>& target) {
    std::istringstream ss(str);
    std::string kv;
    while (std::getline(ss, kv, ',')) {
      size_t eq = kv.find('=');
      if (eq == std::string::npos) continue;
      std::string key = kv.substr(0, eq);
      std::string value = kv.substr(eq + 1);
      target[key] = value;
    }
  };

  auto parseFieldKeyValueList =
      [&](const std::string& str, std::map<std::string, std::string>& target) {
        std::istringstream ss(str);
        std::string kv;
        while (std::getline(ss, kv, ',')) {
          size_t eq = kv.find('=');
          if (eq == std::string::npos) continue;
          std::string key = kv.substr(0, eq);
          std::string value = kv.substr(eq + 1);

          if (!value.empty() && value.back() == 'i') {
            value.pop_back();
          }
          target[key] = value;
        }
      };

  if (pos != std::string::npos) {
    parseKeyValueList(part1.substr(pos + 1), result.labels);
  }
  parseFieldKeyValueList(part2, result.fields);

  result.timestamp = part3;

  return result;
}

class AlgorithmTest : public testing::Test {
 protected:
  void SetUp() override {
    parseConfigFile("../server_config.json",initial_IPs,initial_ports);
    int node_count=initial_IPs.size();
    algo5_ = std::make_unique<Algorithm>(node_count, node_count, initial_IPs, initial_ports);
    clients.clear();
    for (int i = 0; i < node_count; ++i) {
      auto cli = std::make_shared<httplib::Client>(initial_IPs[i].c_str(), initial_ports[i]);
      clients.push_back(cli);
    }
  }
  void TearDown() override {}

  std::unique_ptr<Algorithm> algo5_;

  std::vector<std::string> initial_IPs;
  std::vector<int> initial_ports;
  std::vector<std::shared_ptr<httplib::Client>> clients;
};

TEST_F(AlgorithmTest, TestDevopsSmall) {
  std::ifstream file("/home/dell/tempo/influx-data-devops-cpu-only-small");
  if (!file) {
    std::cerr << "can not open file\n";
  }

  std::string line;
  // int row_count = 315360000;
  int row_count = g_arg_1;
  int per_insert_num = g_arg_2;
  int num_q = g_arg_3;
  int num_write = clients.size();
  int cnt = 0;
  int total_insert_time = 0;
  int total_query_time = 0;
  double insert_time = 0;
  double query_time = 0;
  std::vector<long long> min_query_time(num_q,std::numeric_limits<long long>::max());
  std::vector<long long> max_query_time(num_q, 0);
  double write_io = 0;
  std::unordered_map<std::string, int> count_insert;
  std::unordered_map<std::string, int> count_query;

  for (int i = 0; i < num_q; i++) {
    count_insert["q" + std::to_string(i)] = 0;
  }

  auto start = std::chrono::high_resolution_clock::now();

  std::vector<std::unordered_map<std::string, std::string>> q_labels;

  int t_num_q = num_q;
  while (t_num_q > 0) {
    std::getline(file, line);
    InfluxLine parsed = parseInfluxLineUnified(line);
    for (auto& [k, v] : parsed.fields) {
      for (auto& [k_l, v_l] : parsed.labels) {
        std::unordered_map<std::string, std::string> label_map;
        label_map["__name__"] = k;
        label_map[k_l] = v_l;
        q_labels.push_back(label_map);
        t_num_q--;
        if (t_num_q <= 0) break;
      }
      if (t_num_q <= 0) break;
    }
  }
  t_num_q = num_q;

  for (int i = 0; i < t_num_q; i++) {
    algo5_->addQueryNode("q" + std::to_string(i), 1, q_labels[i]);
  }

  file.clear();
  file.seekg(0, std::ios::beg);

  for (int i = 0; i < row_count; i++) {
    std::getline(file, line);
    InfluxLine parsed = parseInfluxLineUnified(line);
    for (auto& [k, v] : parsed.fields) {
      tsdb::label::Labels lset;
      lset.emplace_back("__name__", k);
      for (auto& [k_l, v_l] : parsed.labels) {
        lset.emplace_back(k_l, v_l);
      }
      int random_weight = dis(gen);
      algo5_->addMetricNode(lset, 1);
      for (int j = 0; j < num_q; j++) {
        auto q_label = q_labels[j];
        bool flag = true;
        for (auto& q1_label : q_label) {
          bool found = false;
          for (auto& label : lset) {
            if (label.label == q1_label.first &&
                label.value == q1_label.second) {
              found = true;
              break;
            }
          }
          if (!found) {
            flag = false;
          }
        }
        if (flag) {
          // algo5_->addEdge(result.first,"q1");
          min_query_time[j] =
              std::min(min_query_time[j], std::stoll(parsed.timestamp));
          max_query_time[j] =
              std::max(max_query_time[j], std::stoll(parsed.timestamp));
          // std::cout<<min_query_time<<" "<<max_query_time<<std::endl;
        }
      }
    }
  }

  algo5_->HPG();
  // algo5_->printResults();

  file.clear();
  file.seekg(0, std::ios::beg);

  int total_batches = (row_count + per_insert_num - 1) / per_insert_num;

  for (int i = 0; i < total_batches; i++) {
    MasstreeWrapper<slab::SlabInfo>::ti =
        threadinfo::make(threadinfo::TI_PROCESS, 16);

    std::vector<prometheus::WriteRequest*> writeRequests;
    for (int k = 0; k < num_write; ++k) {
      writeRequests.push_back(new prometheus::WriteRequest());
    }

    for (int j = 0; j < per_insert_num; j++) {
      if (!std::getline(file, line)) {
        break;
      }
      InfluxLine parsed = parseInfluxLineUnified(line);
      for (auto& [k, v] : parsed.fields) {
        tsdb::label::Labels lset;
        lset.emplace_back("__name__", k);
        for (auto& [k_l, v_l] : parsed.labels) {
          lset.emplace_back(k_l, v_l);
        }
        for (int l = 0; l < num_q; l++) {
          auto q_label = q_labels[l];
          bool flag = true;
          for (auto& q1_label : q_label) {
            bool found = false;
            for (auto& label : lset) {
              if (label.label == q1_label.first &&
                  label.value == q1_label.second) {
                found = true;
                break;
              }
            }
            if (!found) {
              flag = false;
            }
          }
          if (flag) {
            count_insert["q" + std::to_string(l)]++;
            // std::cout<<"SG: "<<metric_id<<" "<< v << " "<<"subid:
            // "<<algo5_->metric_assignments_[metric_id]<<"time:
            // "<<std::stoll(parsed.timestamp)<<std::endl;
          }
        }
        auto start_insert = std::chrono::high_resolution_clock::now();
        auto metric_id = algo5_->selectSeriesGroup(lset);

        auto sub_id = algo5_->metric_assignments_[metric_id];
        auto end_insert = std::chrono::high_resolution_clock::now();
        auto duration_insert =
            std::chrono::duration_cast<std::chrono::milliseconds>(end_insert -
                                                                  start_insert);
        insert_time += duration_insert.count();

        algo5_->subgraphs_[sub_id].data_point_count++;
        prometheus::TimeSeries* timeSeries =
            writeRequests[sub_id]->add_timeseries();
        db::AddLabels(timeSeries, lset);
        db::AddSample(timeSeries, std::stoll(parsed.timestamp), stoll(v));

        cnt++;
      }
    }

    auto start_insert = std::chrono::high_resolution_clock::now();

    std::vector<std::future<void>> futures;
    for (int k = 0; k < num_write; k++) {
      futures.push_back(std::async(std::launch::async, [=] {
        std::string data, compressData;
        writeRequests[k]->SerializeToString(&data);
        snappy::Compress(data.data(), data.size(), &compressData);
        clients[k]->Post("/insert", compressData, "text/plain");
      }));
    }

    for (auto& fut : futures) {
      fut.get();
    }

    auto end_insert = std::chrono::high_resolution_clock::now();
    auto duration_insert =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_insert -
                                                              start_insert);
    insert_time += duration_insert.count();

    for (auto req : writeRequests) {
      delete req;
    }
    writeRequests.clear();
  }

  double mean = 0, sum_of_squares = 0, variance = 0, standard_deviation = 0;

  for (auto& sub : algo5_->subgraphs_) {
    // std::cout << "sub " << sub.id << " " << sub.data_point_count << std::endl;
    mean += sub.data_point_count;
  }
  mean /= algo5_->subgraphs_.size();
  for (auto& sub : algo5_->subgraphs_) {
    sum_of_squares += std::pow(sub.data_point_count - mean, 2);
  }
  variance = sum_of_squares / algo5_->subgraphs_.size();
  standard_deviation = sqrt(variance);

  file.close();
  // std::cout << "total lines: " << cnt << std::endl;
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  total_insert_time += duration.count();

  /**
   *
   *
   *  -------------------------------------------------------- query test
   * --------------------------------------------------------
   *
   *
   */
  for (int i = 0; i < num_q; i++) {
    auto q_label = q_labels[i];
    prometheus::ReadRequest readRequest;
    prometheus::Query* query = readRequest.add_queries();

    for (auto& [k, v] : q_label) {
      prometheus::LabelMatcher* matcher = query->add_matchers();
      matcher->set_name(k);
      matcher->set_value(v);
    }

    // query->set_start_timestamp_ms(min_query_time[i]);
    // query->set_end_timestamp_ms(max_query_time[i]);
    query->set_start_timestamp_ms(0);
    query->set_end_timestamp_ms(std::numeric_limits<long long>::max());

    std::string request_data, compressed_request_data;
    readRequest.SerializeToString(&request_data);
    snappy::Compress(request_data.data(), request_data.size(),
                     &compressed_request_data);

    std::vector<prometheus::ReadResponse> node_responses;

    start = std::chrono::high_resolution_clock::now();
    std::unordered_set<int> count_subs;
    for (auto& metric :
         algo5_->queries_.at("q" + std::to_string(i)).metric_neighbors) {
      // std::cout<<"metric: "<<metric << std::endl;
      auto sub = algo5_->subgraphs_[algo5_->metric_assignments_.at(metric)];
      if (count_subs.find(sub.id) != count_subs.end()) {
        continue;
      }
      count_subs.insert(sub.id);
      httplib::Client cli(sub.IP, sub.port);

      auto start_query = std::chrono::high_resolution_clock::now();

      httplib::Result result =
          cli.Post("/query", compressed_request_data, "text/plain");

      auto end_query = std::chrono::high_resolution_clock::now();
      auto duration_query =
          std::chrono::duration_cast<std::chrono::milliseconds>(end_query -
                                                                start_query);
      query_time += duration_query.count();
      ASSERT_TRUE(result);
      ASSERT_EQ(result->status, 200);

      std::string response_data;
      prometheus::ReadResponse readResponse;
      snappy::Uncompress(result->body.data(), result->body.size(),
                         &response_data);
      ASSERT_TRUE(readResponse.ParseFromString(response_data));
      // std::cout << "subid: " << sub.id
      //           << " size: " << readResponse.results(0).timeseries().size()
      //           << std::endl;
      for (auto& timeseries : readResponse.results(0).timeseries()) {
        count_query["q" + std::to_string(i)] += timeseries.samples_size();
      }
      node_responses.push_back(readResponse);
    }
    ASSERT_EQ(count_query["q" + std::to_string(i)],
              count_insert["q" + std::to_string(i)]);

    end = std::chrono::high_resolution_clock::now();
    duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    total_query_time += duration.count();
  }
  write_io = double(cnt) / insert_time;

  // std::cout << "total insert time: " << total_insert_time << " ms" << std::endl;
  std::cout << "insert time: " << insert_time << " ms" << std::endl;
  std::cout << "write_io: " << write_io*1000 << " samples/s" << std::endl;
  // std::cout << "total query time: " << total_query_time << " ms" << std::endl;
  std::cout << "average query time: " << query_time / num_q << " ms"
            << std::endl;
  // std::cout << "variance: " << variance << std::endl;
  std::cout << "standard deviation: " << standard_deviation << std::endl;
}

TEST_F(AlgorithmTest, TestSD) {
  std::ifstream file("/home/dell/tempo/influx-data-devops-cpu-only-small");
  if (!file) {
    std::cerr << "can not open file\n";
    return;
  }

  const int lines_per_round = g_arg_1;
  const int total_rounds = g_arg_2;
  const int num_q = g_arg_3;
  int server_size=clients.size();

  // const int lines_per_round = 10015;
  // const int total_rounds = 100;
  // const int num_q = 1013;

  std::vector<int> total_data_count_per_subgraph(server_size,0);
  std::string line;
  std::vector<double> std_devs;

  for (int round = 0; round < total_rounds; ++round) {

    std::streampos current_pos = file.tellg();

    std::vector<std::unordered_map<std::string, std::string>> q_labels;
    int t_num_q = num_q;
    while (t_num_q > 0 && std::getline(file, line)) {
      InfluxLine parsed = parseInfluxLineUnified(line);
      for (auto& [k, v] : parsed.fields) {
        for (auto& [k_l, v_l] : parsed.labels) {
          std::unordered_map<std::string, std::string> label_map;
          label_map["__name__"] = k;
          label_map[k_l] = v_l;
          q_labels.push_back(label_map);
          t_num_q--;
          if (t_num_q <= 0) break;
        }
        if (t_num_q <= 0) break;
      }
    }

    file.seekg(current_pos);

    for (int i = 0; i < num_q; i++) {
      algo5_->addQueryNodeSD("q" + std::to_string(i), 1, q_labels[i]);
    }

    int lines_read = 0;
    while (lines_read < lines_per_round) {
      if (!std::getline(file, line)) {
        file.clear();
        file.seekg(0, std::ios::beg);
        if (!std::getline(file, line)) {
          std::cerr << "file is empty or cannot reread\n";
          return;
        }
      }

      InfluxLine parsed = parseInfluxLineUnified(line);
      for (auto& [k, v] : parsed.fields) {
        tsdb::label::Labels lset;
        lset.emplace_back("__name__", k);
        for (auto& [k_l, v_l] : parsed.labels) {
          lset.emplace_back(k_l, v_l);
        }
        algo5_->addMetricNode(lset, 1);

      }
      lines_read++;
    }

    algo5_->HPG();
    // algo5_->printResults();

    double mean = 0, sum_of_squares = 0, variance = 0, standard_deviation = 0;
    for (auto& sub : algo5_->subgraphs_) {
      // std::cout << sub.w_load << std::endl;
      mean += sub.w_load;
      total_data_count_per_subgraph[sub.id]+=sub.w_load;
    }
    mean /= algo5_->subgraphs_.size();
    for (auto& sub : algo5_->subgraphs_) {
      sum_of_squares += std::pow(sub.w_load - mean, 2);
    }
    variance = sum_of_squares / algo5_->subgraphs_.size();
    standard_deviation = std::sqrt(variance);

    std_devs.push_back(standard_deviation);
    // std::cout << "Standard deviation (Round " << round + 1
    //           << "): " << standard_deviation << std::endl;

    algo5_->clear();
    for (int i = 0; i < server_size; ++i) {
      algo5_->subgraphs_.emplace_back(i, "0", 0);
    }
    algo5_->bucket_num = server_size;
  }

  double total_sd = 0;
  for (auto sd : std_devs) total_sd += sd;
  double avg_sd = total_sd / std_devs.size();
  std::cout << "Average Standard Deviation over rounds: " << avg_sd << std::endl;

  double total_mean=0,total_sum_of_squares = 0, total_variance = 0, total_standard_deviation = 0;;
  for (int i=0;i<total_data_count_per_subgraph.size();i++){
    total_mean+=total_data_count_per_subgraph[i];
  }
  total_mean/=total_data_count_per_subgraph.size();
  for (int i=0;i<total_data_count_per_subgraph.size();i++) {
    total_sum_of_squares += std::pow(total_data_count_per_subgraph[i] - total_mean, 2);
  }
  total_variance = total_sum_of_squares / total_data_count_per_subgraph.size();
  total_standard_deviation = std::sqrt(total_variance);
  // std::cout<<"total mean: "<<total_mean<<std::endl;
  // std::cout<<"total variance: "<<total_variance<<std::endl;
  // std::cout<<"size: "<<total_data_count_per_subgraph.size()<<std::endl;
  // for (int i=0;i<total_data_count_per_subgraph.size();i++){
  //   std::cout<<"subgraph "<<i<<" total data count: "<<total_data_count_per_subgraph[i]<<std::endl;
  // }
  std::cout<<"Total Standard Deviation: "<<total_standard_deviation<<std::endl;
  file.close();
}

}  // namespace tsdb

int main(int argc, char** argv) {
  int default_row_count = 100000;
  int default_per_insert_num = 100000;
  int default_num_q = 1000;

  if (argc >= 4) {
    try {
      g_arg_1 = std::stoi(argv[1]);
      g_arg_2 = std::stoi(argv[2]);
      g_arg_3 = std::stoi(argv[3]);

      if (g_arg_1 <= 0 || g_arg_2 <= 0 || g_arg_3 <= 0) {
        throw std::invalid_argument("The parameter must be a positive integer");
      }
    } catch (const std::invalid_argument& e) {
      std::cerr << "Parameter format error：" << e.what() << std::endl;
      return 1;
    } catch (const std::out_of_range& e) {
      std::cerr << "Parameter value out of range：" << e.what() << std::endl;
      return 1;
    }
  } else {
    g_arg_1 = default_row_count;
    g_arg_2 = default_per_insert_num;
    g_arg_3 = default_num_q;
    std::cout << "Using default parameters: "
              << default_row_count << " "
              << default_per_insert_num << " "
              << default_num_q << std::endl;
  }

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
