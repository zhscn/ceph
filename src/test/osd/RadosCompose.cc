#include <exception>
#include <format>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <stdint.h>

#include "include/rados/librados.hpp"

using librados::bufferlist;
using librados::snap_t;

static constexpr uint32_t MAX_SIZE = 16 * 1024 * 1024;

struct ClientException : public std::runtime_error {
  template <typename... Args>
  ClientException(std::format_string<Args...> fmt, Args &&...args)
      : std::runtime_error(
            std::vformat(fmt.get(), std::make_format_args(args...))) {}
};

#define CEX(fmt) ClientException("{} " fmt, __func__)
#define EX(fmt, ...) ClientException("{} " fmt, __func__, __VA_ARGS__)

struct ContentsGenerator {
  std::random_device rd;
  std::default_random_engine eng;
  std::uniform_int_distribution<char> dis;

  explicit ContentsGenerator(bool readable)
      : rd(), eng(rd()),
        dis(readable ? ' ' : std::numeric_limits<char>::min(),
            readable ? '~' : std::numeric_limits<char>::max()) {}

  std::string operator()(uint32_t length) {
    auto ret = std::string(length, 0);
    for (int i = 0; i < length; i++) {
      ret[i] = dis(eng);
    }
    return ret;
  }
};

struct Op {
  bool truncate;
  uint32_t offset;
  std::string data;
};

struct LocalObject {
  std::string head;
  std::map<std::string, std::string> snaps;

  explicit LocalObject() : head(), snaps() {}

  void write(uint32_t offset, const std::string &data) {
    if (data.empty()) {
      throw CEX("local write empty");
    }
    auto length = offset + data.size();
    if (length > MAX_SIZE) {
      throw EX("overflow {}", length);
    }
    if (head.size() < length) {
      truncate(length);
    }
    memcpy(head.data() + offset, data.data(), data.size());
  }

  void truncate(uint32_t size) { head.resize(size); }

  void task_snap(std::string name) { snaps[name] = head; }

  void rollback_snap(std::string name) { head = snaps[name]; }

  void remove_snap(std::string name) { snaps.erase(name); }

  std::string read() const { return head; }
};

struct SingleRemoteObject {
  librados::IoCtx &io_ctx;
  std::string name;
  SingleRemoteObject(librados::IoCtx &io_ctx, std::string n)
      : io_ctx(io_ctx), name(std::move(n)) {
    auto ret = io_ctx.create(name, true);
    if (ret < 0) {
      throw EX("create {}", name);
    }
  }

  void write(uint32_t offset, const std::string &data) {
    io_ctx.snap_set_read(librados::SNAP_HEAD);
    bufferlist bl;
    bl.append(data);
    io_ctx.write(name, bl, data.length(), offset);
  }

  void truncate(uint32_t size) {
    io_ctx.snap_set_read(librados::SNAP_HEAD);
    io_ctx.trunc(name, size);
  }

  void task_snap(const std::string &s) { io_ctx.snap_create(s.data()); }
  void rollback_snap(const std::string &s) {
    io_ctx.snap_rollback(name, s.data());
  }
  void remove_snap(const std::string &s) { io_ctx.snap_remove(s.data()); }

  std::string read() const {
    io_ctx.snap_set_read(librados::SNAP_HEAD);
    uint64_t size;
    io_ctx.stat(name, &size, nullptr);
    bufferlist bl;
    io_ctx.read(name, bl, size, 0);
    return {bl.c_str(), bl.length()};
  }
};

struct BatchRemoteObject {
  librados::IoCtx &io_ctx;
  std::string name;

  BatchRemoteObject(librados::IoCtx &io_ctx, std::string name_)
      : io_ctx(io_ctx), name(std::move(name_)) {
    auto ret = io_ctx.create(name, true);
    if (ret < 0) {
      throw EX("create {}", ret);
    }
  }

  void exec_io(std::vector<Op> ops) {
    auto c = librados::Rados::aio_create_completion();
    librados::ObjectWriteOperation w;
    for (auto &op : ops) {
      if (!op.truncate) {
        bufferlist bl;
        bl.append(op.data);
        w.write(op.offset, bl);
      } else {
        w.truncate(op.offset);
      }
    }
    io_ctx.aio_operate(name, c, &w);
    c->wait_for_complete();
  }

  void task_snap(const std::string &s) { io_ctx.snap_create(s.data()); }
  void rollback_snap(const std::string &s) {
    io_ctx.snap_rollback(name, s.data());
  }
  void remove_snap(const std::string &s) { io_ctx.snap_remove(s.data()); }

  std::string read() const {
    io_ctx.snap_set_read(librados::SNAP_HEAD);
    uint64_t size;
    io_ctx.stat(name, &size, nullptr);
    bufferlist bl;
    io_ctx.read(name, bl, size, 0);
    return std::string(bl.c_str(), bl.length());
  }
};

struct Client {
  librados::Rados cluster;
  librados::IoCtx single_io_ctx;
  // librados::IoCtx batch_io_ctx;

  using dis_t = std::uniform_int_distribution<uint32_t>;

  explicit Client() {}

  ~Client() {
    single_io_ctx.close();
    // batch_io_ctx.close();
    cluster.shutdown();
  }

  void init(const char *conf_path) {
    int ret = 0;

    ret = cluster.init2("client.admin", "ceph", 0);
    if (ret < 0) {
      throw EX("init2 {}", ret);
    }

    ret = cluster.conf_read_file(conf_path);
    if (ret < 0) {
      throw EX("conf_read_file {} {}", conf_path, ret);
    }

    ret = cluster.connect();
    if (ret < 0) {
      throw EX("connect {}", ret);
    }

    ret = cluster.pool_create("single");
    if (ret < 0) {
      throw EX("create single pool {}", ret);
    }

    // ret = cluster.pool_create("batch");
    // if (ret < 0) {
    //   throw EX("create batch pool {}", ret);
    // }

    ret = cluster.ioctx_create("single", single_io_ctx);
    if (ret < 0) {
      throw EX("create single ctx {}", ret);
    }

    // ret = cluster.ioctx_create("batch", batch_io_ctx);
    // if (ret < 0) {
    //   throw EX("{}", ret);
    // }
  }

  void run(int cycle) {
    std::random_device rd;
    std::default_random_engine eng(rd());
    auto g = ContentsGenerator(true);

    LocalObject local;
    SingleRemoteObject single(single_io_ctx, "single");
    // BatchRemoteObject batch(batch_io_ctx, "batch");
    BatchRemoteObject batch(single_io_ctx, "batch");

    dis_t write_op_gen(0, 9);
    dis_t offset_gen(0, MAX_SIZE / 4096 - 1);
    dis_t length_gen(0, MAX_SIZE / 4096);
    // dis_t cycle_gen(32, 64);
    int next_id = 0;

    auto create_snap = [&] {
      auto snap = std::to_string(next_id++);
      local.task_snap(snap);
      single.task_snap(snap);
      batch.task_snap(snap);
    };

    auto handle_snap = [&](bool rollback) {
      dis_t snap_gen(0, local.snaps.size() - 1);
      auto iter = local.snaps.begin();
      for (auto s = snap_gen(eng); s > 0; s--) {
        iter = std::next(iter);
      }
      if (iter == local.snaps.end()) {
        throw CEX("bug for finding snap");
      }
      auto sn = iter->first;
      if (rollback) {
        local.rollback_snap(sn);
        single.rollback_snap(sn);
        batch.rollback_snap(sn);
      } else {
        local.remove_snap(sn);
        single.remove_snap(sn);
        batch.remove_snap(sn);
      }
    };

    for (int c = 0; c < cycle; c++) {
      std::cout << c << '\n';
      std::vector<Op> ops;
      // auto cycle = cycle_gen(eng);
      for (int i = 0; i < 16; i++) {
        bool truncate = write_op_gen(eng) == 9;
        if (truncate) {
          uint32_t length = length_gen(eng) * 4096;
          local.truncate(length);
          single.truncate(length);
          auto local_content = local.read();
          auto single_content = single.read();
          if (local_content != single_content) {
            throw CEX("single truncate not match");
          }
          ops.push_back(Op{true, length, {}});
        } else {
          auto offset = offset_gen(eng) * 4096;
          auto length =
              std::min(MAX_SIZE - offset, std::max(1U, length_gen(eng)) * 4096);
          auto content = g(length);
          local.write(offset, content);
          single.write(offset, content);
          ops.push_back(Op{false, offset, std::move(content)});
          auto local_content = local.read();
          auto single_content = single.read();
          if (local_content != single_content) {
            throw CEX("single write not match");
          }
        }
      }
      batch.exec_io(std::move(ops));
      auto local_content = local.read();
      auto batch_content = batch.read();
      if (local_content != batch_content) {
        throw CEX("batch not match");
      }

      if (local.snaps.size() < 3) {
        create_snap();
      } else if (local.snaps.size() > 10) {
        handle_snap(false);
      } else {
        dis_t snap_gen(0, 2);
        auto op = snap_gen(eng);
        if (op == 0) {
          create_snap();
        } else if (op == 1) {
          handle_snap(true);
        } else {
          handle_snap(false);
        }
      }
      {
        auto local_content = local.read();
        auto single_content = single.read();
        if (local_content != single_content) {
          throw CEX("single snap not match");
        }
      }
      {
        auto local_content = local.read();
        auto batch_content = batch.read();
        if (local_content != batch_content) {
          throw CEX("batch snap match");
        }
      }
    }
  }
};

int main(int argc, const char **argv) {
  if (argc < 2) {
    std::cerr << "Usage: ceph_test_rados_compose cycle [ceph.conf]\n";
    return 1;
  }
  std::istringstream is(argv[1]);
  int cycle = 0;
  is >> cycle;
  try {
    Client client;
    client.init(argc == 3 ? argv[2] : "/etc/ceph/ceph.conf");
    client.run(cycle);
  } catch (const std::exception &ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
  return 0;
}
