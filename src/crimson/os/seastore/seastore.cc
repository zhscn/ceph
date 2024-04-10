// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <algorithm>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shared_mutex.hh>

#include "common/safe_io.h"
#include "include/stringify.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/object_data_handler.h"


using std::string;
using crimson::common::local_conf;

template <> struct fmt::formatter<crimson::os::seastore::op_type_t>
  : fmt::formatter<std::string_view> {
  using op_type_t =  crimson::os::seastore::op_type_t;
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(op_type_t op, FormatContext& ctx) {
    std::string_view name = "unknown";
    switch (op) {
      case op_type_t::TRANSACTION:
      name = "transaction";
      break;
    case op_type_t::READ:
      name = "read";
      break;
    case op_type_t::WRITE:
      name = "write";
      break;
    case op_type_t::GET_ATTR:
      name = "get_attr";
      break;
    case op_type_t::GET_ATTRS:
      name = "get_attrs";
      break;
    case op_type_t::STAT:
      name = "stat";
      break;
    case op_type_t::OMAP_GET_VALUES:
      name = "omap_get_values";
      break;
    case op_type_t::OMAP_LIST:
      name = "omap_list";
      break;
    case op_type_t::MAX:
      name = "unknown";
      break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

SET_SUBSYS(seastore);

namespace crimson::os::seastore {

class FileMDStore final : public SeaStore::MDStore {
  std::string root;
public:
  FileMDStore(const std::string& root) : root(root) {}

  write_meta_ret write_meta(
    const std::string& key, const std::string& value) final {
    std::string path = fmt::format("{}/{}", root, key);
    ceph::bufferlist bl;
    bl.append(value + "\n");
    return crimson::write_file(std::move(bl), path);
  }

  read_meta_ret read_meta(const std::string& key) final {
    std::string path = fmt::format("{}/{}", root, key);
    return seastar::file_exists(
      path
    ).then([path] (bool exist) {
      if (exist) {
	return crimson::read_file(path)
	  .then([] (auto tmp_buf) {
	    std::string v = {tmp_buf.get(), tmp_buf.size()};
	    std::size_t pos = v.find("\n");
	    std::string str = v.substr(0, pos);
	    return seastar::make_ready_future<std::optional<std::string>>(str);
	  });
      } else {
	return seastar::make_ready_future<std::optional<std::string>>(std::nullopt);
      }
    });
  }
};

using crimson::common::get_conf;

SeaStore::Shard::Shard(
  std::string root,
  Device* dev,
  bool is_test)
  :root(root),
   max_object_size(
     get_conf<uint64_t>("seastore_default_max_object_size")),
   is_test(is_test),
   throttler(
      get_conf<uint64_t>("seastore_max_concurrent_transactions"))
{
  device = &(dev->get_sharded_device());
  register_metrics();
}

SeaStore::SeaStore(
  const std::string& root,
  MDStoreRef mdstore)
  : root(root),
    mdstore(std::move(mdstore))
{
}

SeaStore::~SeaStore() = default;

void SeaStore::Shard::register_metrics()
{
  namespace sm = seastar::metrics;
  using op_type_t = crimson::os::seastore::op_type_t;
  std::pair<op_type_t, sm::label_instance> labels_by_op_type[] = {
    {op_type_t::TRANSACTION,     sm::label_instance("latency", "TRANSACTION")},
    {op_type_t::READ,            sm::label_instance("latency", "READ")},
    {op_type_t::WRITE,           sm::label_instance("latency", "WRITE")},
    {op_type_t::GET_ATTR,        sm::label_instance("latency", "GET_ATTR")},
    {op_type_t::GET_ATTRS,       sm::label_instance("latency", "GET_ATTRS")},
    {op_type_t::STAT,            sm::label_instance("latency", "STAT")},
    {op_type_t::OMAP_GET_VALUES, sm::label_instance("latency",  "OMAP_GET_VALUES")},
    {op_type_t::OMAP_LIST,       sm::label_instance("latency", "OMAP_LIST")},
  };

  for (auto& [op_type, label] : labels_by_op_type) {
    auto desc = fmt::format("latency of seastore operation (optype={})",
                            op_type);
    metrics.add_group(
      "seastore",
      {
        sm::make_histogram(
          "op_lat", [this, op_type=op_type] {
            return get_latency(op_type);
          },
          sm::description(desc),
          {label}
        ),
      }
    );
  }

  metrics.add_group(
    "seastore",
    {
      sm::make_gauge(
	"concurrent_transactions",
	[this] {
	  return throttler.get_current();
	},
	sm::description("transactions that are running inside seastore")
      ),
      sm::make_gauge(
	"pending_transactions",
	[this] {
	  return throttler.get_pending();
	},
	sm::description("transactions waiting to get "
		        "through seastore's throttler")
      ),
      sm::make_gauge(
	"inflight_io_count",
	[this] {
	  return stats.inflight_io_count;
	},
	sm::description("transactions that are running inside seastore")
      ),
      sm::make_gauge(
	"inflight_read_count",
	[this] {
	  return stats.inflight_read_count;
	},
	sm::description("transactions that are running inside seastore")
      )
    }
  );
}

seastar::future<> SeaStore::start()
{
  LOG_PREFIX(SeaStore::start);
  ceph_assert(seastar::this_shard_id() == primary_core);
#ifndef NDEBUG
  bool is_test = true;
#else
  bool is_test = false;
#endif
  using crimson::common::get_conf;
  std::string type = get_conf<std::string>("seastore_main_device_type");
  device_type_t d_type = string_to_device_type(type);
  assert(d_type == device_type_t::SSD ||
         d_type == device_type_t::RANDOM_BLOCK_SSD);
  type = get_conf<std::string>("seastore_main_backend_type");
  auto b_type = string_to_backend_type(type);
  INFO("main device type: {}, main backend type: {}", d_type, b_type);
  ceph_assert(root != "");
  return Device::make_device(root, d_type, b_type
  ).then([this](DeviceRef device_obj) {
    device = std::move(device_obj);
    return device->start();
  }).then([this, is_test] {
    ceph_assert(device);
    return shard_stores.start(root, device.get(), is_test);
  });
}

seastar::future<> SeaStore::test_start(DeviceRef device_obj)
{
  ceph_assert(device_obj);
  ceph_assert(root == "");
  device = std::move(device_obj);
  return shard_stores.start_single(root, device.get(), true);
}

seastar::future<> SeaStore::stop()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return seastar::do_for_each(secondaries, [](auto& sec_dev) {
    return sec_dev->stop();
  }).then([this] {
    secondaries.clear();
    if (device) {
      return device->stop();
    } else {
      return seastar::now();
    }
  }).then([this] {
    return shard_stores.stop();
  });
}

SeaStore::mount_ertr::future<> SeaStore::test_mount()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.local().mount_managers();
}

SeaStore::mount_ertr::future<> SeaStore::mount()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return device->mount(
  ).safe_then([this] {
    auto &sec_devices = device->get_sharded_device().get_secondary_devices();
    return crimson::do_for_each(sec_devices, [this](auto& device_entry) {
      device_id_t id = device_entry.first;
      magic_t magic = device_entry.second.magic;
      device_type_t dtype = device_entry.second.dtype;
      backend_type_t btype = device_entry.second.btype;
      auto btype_conf_str = get_conf<std::string>("seastore_secondary_backend_type");
      ceph_assert(string_to_backend_type(btype_conf_str) == btype);
      std::string path =
        fmt::format("{}/block.{}", root, std::to_string(id));
      return Device::make_device(path, dtype, btype
      ).then([this, path, magic](DeviceRef sec_dev) {
        return sec_dev->start(
        ).then([this, magic, sec_dev = std::move(sec_dev)]() mutable {
          return sec_dev->mount(
          ).safe_then([this, sec_dev=std::move(sec_dev), magic]() mutable {
            boost::ignore_unused(magic);  // avoid clang warning;
            assert(sec_dev->get_sharded_device().get_magic() == magic);
            secondaries.emplace_back(std::move(sec_dev));
          });
        }).safe_then([this] {
          return set_secondaries();
        });
      });
    }).safe_then([this] {
      return shard_stores.invoke_on_all([](auto &local_store) {
        return local_store.mount_managers();
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mount"
    }
  );
}

seastar::future<> SeaStore::Shard::mount_managers()
{
  init_managers();
  return transaction_manager->mount(
  ).safe_then([this] {
    if (transaction_manager->hash_multiple_tiers()) {
      return seastar::do_with(
        ghobject_t(),
	[this](ghobject_t &marker) {
	  return crimson::repeat([this, &marker] {
	    return transaction_manager->with_transaction_weak(
	      "scan_onode_tree",
	      [this, &marker](auto &t) {
		return onode_manager->scan_onodes(
		  t,
		  marker,
		  10,
		  [this, &t](laddr_t prefix) {
		    return transaction_manager->maybe_load_onode(
		      t, prefix, extent_types_t::OBJECT_DATA_BLOCK);
		  });
	      }).safe_then([&marker](auto res) {
		if (res) {
		  marker = *res;
		  return seastar::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::no);
		} else {
		  return seastar::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::yes);
		}
	      });
	  });
	});
    }
    return TransactionManager::base_ertr::make_ready_future();
  }).safe_then([this] {
    transaction_manager->start_background();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in mount_managers"
  });
}

seastar::future<> SeaStore::umount()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.invoke_on_all([](auto &local_store) {
    return local_store.umount();
  });
}

seastar::future<> SeaStore::Shard::umount()
{
  return [this] {
    if (transaction_manager) {
      return transaction_manager->close();
    } else {
      return TransactionManager::close_ertr::now();
    }
  }().safe_then([this] {
    return crimson::do_for_each(
      secondaries,
      [](auto& sec_dev) -> SegmentManager::close_ertr::future<>
    {
      return sec_dev->close();
    });
  }).safe_then([this] {
    return device->close();
  }).safe_then([this] {
    secondaries.clear();
    transaction_manager.reset();
    collection_manager.reset();
    onode_manager.reset();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::umount"
    }
  );
}

seastar::future<> SeaStore::write_fsid(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  LOG_PREFIX(SeaStore::write_fsid);
  return read_meta("fsid").then([this, FNAME, new_osd_fsid] (auto tuple) {
    auto [ret, fsid] = tuple;
    std::string str_fsid = stringify(new_osd_fsid);
    if (ret == -1) {
       return write_meta("fsid", stringify(new_osd_fsid));
    } else if (ret == 0 && fsid != str_fsid) {
       ERROR("on-disk fsid {} != provided {}",
         fsid, stringify(new_osd_fsid));
       throw std::runtime_error("store fsid error");
     } else {
      return seastar::now();
     }
   });
}

seastar::future<>
SeaStore::Shard::mkfs_managers()
{
  init_managers();
  return transaction_manager->mkfs(
  ).safe_then([this] {
    init_managers();
    return transaction_manager->mount(
    ).safe_then([this] {
      transaction_manager->start_background();
    });
  }).safe_then([this] {
    return repeat_eagain([this] {
      return transaction_manager->with_transaction_intr(
	Transaction::src_t::MUTATE,
	"mkfs_seastore",
	[this](auto& t)
      {
	return onode_manager->mkfs(t
	).si_then([this, &t] {
	  return collection_manager->mkfs(t);
	}).si_then([this, &t](auto coll_root) {
	  transaction_manager->write_collection_root(
	    t, coll_root);
	  return transaction_manager->submit_transaction(t);
	});
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in Shard::mkfs_managers"
    }
  );
}

seastar::future<> SeaStore::set_secondaries()
{
  auto sec_dev_ite = secondaries.rbegin();
  Device* sec_dev = sec_dev_ite->get();
  return shard_stores.invoke_on_all([sec_dev](auto &local_store) {
    local_store.set_secondaries(sec_dev->get_sharded_device());
  });
}

SeaStore::mkfs_ertr::future<> SeaStore::test_mkfs(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return read_meta("mkfs_done").then([this, new_osd_fsid] (auto tuple) {
    auto [done, value] = tuple;
    if (done == 0) {
      return seastar::now();
    } 
    return shard_stores.local().mkfs_managers(
    ).then([this, new_osd_fsid] {
      return prepare_meta(new_osd_fsid);
    });
  });
}

seastar::future<> SeaStore::prepare_meta(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return write_fsid(new_osd_fsid).then([this] {
    return read_meta("type").then([this] (auto tuple) {
      auto [ret, type] = tuple;
      if (ret == 0 && type == "seastore") {
	return seastar::now();
      } else if (ret == 0 && type != "seastore") {
	LOG_PREFIX(SeaStore::prepare_meta);
	ERROR("expected seastore, but type is {}", type);
	throw std::runtime_error("store type error");
      } else {
	return write_meta("type", "seastore");
      }
    });
  }).then([this] {
    return write_meta("mkfs_done", "yes");
  });
}

std::pair<bool, device_id_t> parse_device_id(const seastar::sstring &name) {
  auto ret = std::make_pair<bool, device_id_t>(false, 0);
  auto prefix_len = sizeof("block.") - 1;
  if (name.find("block.") == 0 && name.length() > prefix_len) {
    ret.first = true;
    int id = 0;
    std::string id_str = name.substr(prefix_len);
    std::istringstream iss(id_str);
    iss >> id;
    assert(id < std::numeric_limits<uint8_t>::max());
    ret.second = id;
  }
  return ret;
}

SeaStore::mkfs_ertr::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return read_meta("mkfs_done"
  ).then([this, new_osd_fsid] (auto tuple) {
    auto [done, value] = tuple;
    if (done == 0) {
      return seastar::now();
    }
    auto dtype_str = get_conf<std::string>("seastore_secondary_device_type");
    auto dtype = string_to_device_type(dtype_str);
    auto btype_str = get_conf<std::string>("seastore_secondary_backend_type");
    auto btype = string_to_backend_type(btype_str);
    ceph_assert(!root.empty());
    LOG_PREFIX(SeaStore::mkfs);
    INFO("secondary device type: {}, secondary backend type: {}", dtype, btype);
    return seastar::do_with(
      secondary_device_set_t(),
      [this, new_osd_fsid, dtype, btype](auto &sds) {
        return seastar::open_directory(root
        ).then([this, new_osd_fsid, &sds, dtype, btype](auto _root_dir) {
          auto root_dir = std::make_unique<seastar::file>(std::move(_root_dir));
          return root_dir->list_directory(
            [this, new_osd_fsid, &sds, dtype, btype]
            (seastar::directory_entry de) -> seastar::future<> {
              auto p = parse_device_id(de.name);
              if (!p.first) {
                return seastar::now();
              }
              auto path = fmt::format("{}/{}", root, de.name);
              return Device::make_device(path, dtype, btype
              ).then([this, &sds, id=p.second, dtype,
		      btype, new_osd_fsid](DeviceRef sec_dev) {
                auto p_sec_dev = sec_dev.get();
                secondaries.emplace_back(std::move(sec_dev));
                return p_sec_dev->start(
                ).then([&sds, id, dtype, btype, new_osd_fsid, p_sec_dev] {
                  magic_t magic = std::rand();
                  sds.emplace(id, device_spec_t{magic, dtype, btype, id});
                  return p_sec_dev->mkfs(device_config_t::create_secondary(
                    new_osd_fsid, id, dtype, btype, magic)
                  ).handle_error(crimson::ct_error::assert_all{"not possible"});
                });
              }).then([this] {
                return set_secondaries();
              });
            }).done().then([root_dir=std::move(root_dir)] {});
        }).then([this, new_osd_fsid, &sds] {
	  device_id_t id = 0;
	  device_type_t d_type = device->get_device_type();
	  backend_type_t b_type = device->get_backend_type();
	  assert(d_type == device_type_t::SSD ||
		 d_type == device_type_t::RANDOM_BLOCK_SSD);
	  assert(b_type != backend_type_t::NONE);
	  if (d_type == device_type_t::RANDOM_BLOCK_SSD) {
	    id = static_cast<device_id_t>(DEVICE_ID_RANDOM_BLOCK_MIN);
	  }
	  return device->mkfs(device_config_t::create_primary(
            new_osd_fsid, id, d_type, b_type, sds));
	}).safe_then([this] {
	  return crimson::do_for_each(secondaries, [](auto& sec_dev) {
	    return sec_dev->mount();
	  });
	}).safe_then([this] {
          return device->mount();
        }).safe_then([this] {
          return shard_stores.invoke_on_all([] (auto &local_store) {
            return local_store.mkfs_managers();
          });
        }).safe_then([this, new_osd_fsid] {
          return prepare_meta(new_osd_fsid);
        }).safe_then([this] {
          return umount();
        }).handle_error(
          crimson::ct_error::assert_all{
            "Invalid error in SeaStore::mkfs"
          });
        });
  });
}

using coll_core_t = FuturizedStore::coll_core_t;
seastar::future<std::vector<coll_core_t>>
SeaStore::list_collections()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.map([](auto &local_store) {
    return local_store.list_collections();
  }).then([](std::vector<std::vector<coll_core_t>> results) {
    std::vector<coll_core_t> collections;
    for (auto& colls : results) {
      collections.insert(collections.end(), colls.begin(), colls.end());
    }
    return seastar::make_ready_future<std::vector<coll_core_t>>(
      std::move(collections));
  });
}

store_statfs_t SeaStore::Shard::stat() const
{
  return transaction_manager->store_stat();
}

seastar::future<store_statfs_t> SeaStore::stat() const
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  LOG_PREFIX(SeaStore::stat);
  DEBUG("");
  return shard_stores.map_reduce0(
    [](const SeaStore::Shard &local_store) {
      return local_store.stat();
    },
    store_statfs_t(),
    [](auto &&ss, auto &&ret) {
      ss.add(ret);
      return std::move(ss);
    }
  ).then([](store_statfs_t ss) {
    return seastar::make_ready_future<store_statfs_t>(std::move(ss));
  });
}

seastar::future<store_statfs_t> SeaStore::pool_statfs(int64_t pool_id) const
{
   //TODO
   return SeaStore::stat();
}

seastar::future<> SeaStore::report_stats()
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  shard_device_stats.resize(seastar::smp::count);
  return shard_stores.invoke_on_all([this](const Shard &local_store) {
    bool report_detail = false;
    if (seastar::this_shard_id() == 0) {
      // avoid too verbose logs, only report detail in a particular shard
      report_detail = true;
    }
    shard_device_stats[seastar::this_shard_id()] =
      local_store.get_device_stats(report_detail);
  }).then([this] {
    LOG_PREFIX(SeaStore);
    auto now = seastar::lowres_clock::now();
    if (last_tp == seastar::lowres_clock::time_point::min()) {
      last_tp = now;
      return seastar::now();
    }
    std::chrono::duration<double> duration_d = now - last_tp;
    double seconds = duration_d.count();
    last_tp = now;
    device_stats_t ts = {};
    for (const auto &s : shard_device_stats) {
      ts.add(s);
    }
    constexpr const char* dfmt = "{:.2f}";
    auto d_ts_num_io = static_cast<double>(ts.num_io);
    std::ostringstream oss_iops;
    oss_iops << "device IOPS:"
             << fmt::format(dfmt, ts.num_io/seconds)
             << "(";
    std::ostringstream oss_depth;
    oss_depth << "device per-writer depth:"
              << fmt::format(dfmt, ts.total_depth/d_ts_num_io)
              << "(";
    std::ostringstream oss_bd;
    oss_bd << "device bandwidth(MiB):"
           << fmt::format(dfmt, ts.total_bytes/seconds/(1<<20))
           << "(";
    std::ostringstream oss_iosz;
    oss_iosz << "device IO size(B):"
             << fmt::format(dfmt, ts.total_bytes/d_ts_num_io)
             << "(";
    for (const auto &s : shard_device_stats) {
      auto d_s_num_io = static_cast<double>(s.num_io);
      oss_iops << fmt::format(dfmt, s.num_io/seconds) << ",";
      oss_depth << fmt::format(dfmt, s.total_depth/d_s_num_io) << ",";
      oss_bd << fmt::format(dfmt, s.total_bytes/seconds/(1<<20)) << ",";
      oss_iosz << fmt::format(dfmt, s.total_bytes/d_s_num_io) << ",";
    }
    oss_iops << ")";
    oss_depth << ")";
    oss_bd << ")";
    oss_iosz << ")";
    INFO("{}", oss_iops.str());
    INFO("{}", oss_depth.str());
    INFO("{}", oss_bd.str());
    INFO("{}", oss_iosz.str());
    return seastar::now();
  });
}

TransactionManager::read_extent_iertr::future<std::optional<unsigned>>
SeaStore::Shard::get_coll_bits(CollectionRef ch, Transaction &t) const
{
  return transaction_manager->read_collection_root(t)
    .si_then([this, ch, &t](auto coll_root) {
      return collection_manager->list(coll_root, t);
    }).si_then([ch](auto colls) {
      auto it = std::find_if(colls.begin(), colls.end(),
        [ch](const std::pair<coll_t, coll_info_t>& element) {
          return element.first == ch->get_cid();
      });
      if (it != colls.end()) {
        return TransactionManager::read_extent_iertr::make_ready_future<
          std::optional<unsigned>>(it->second.split_bits);
      } else {
        return TransactionManager::read_extent_iertr::make_ready_future<
	  std::optional<unsigned>>(std::nullopt);
      }
    });
}

col_obj_ranges_t
SeaStore::get_objs_range(CollectionRef ch, unsigned bits)
{
  col_obj_ranges_t obj_ranges;
  spg_t pgid;
  constexpr uint32_t MAX_HASH = std::numeric_limits<uint32_t>::max();
  const std::string_view MAX_NSPACE = "\xff";
  if (ch->get_cid().is_pg(&pgid)) {
    obj_ranges.obj_begin.shard_id = pgid.shard;
    obj_ranges.temp_begin = obj_ranges.obj_begin;

    obj_ranges.obj_begin.hobj.pool = pgid.pool();
    obj_ranges.temp_begin.hobj.pool = -2ll - pgid.pool();

    obj_ranges.obj_end = obj_ranges.obj_begin;
    obj_ranges.temp_end = obj_ranges.temp_begin;

    uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
    obj_ranges.obj_begin.hobj.set_bitwise_key_u32(reverse_hash);
    obj_ranges.temp_begin.hobj.set_bitwise_key_u32(reverse_hash);

    uint64_t end_hash = reverse_hash  + (1ull << (32 - bits));
    if (end_hash > MAX_HASH) {
      // make sure end hobj is even greater than the maximum possible hobj
      obj_ranges.obj_end.hobj.set_bitwise_key_u32(MAX_HASH);
      obj_ranges.temp_end.hobj.set_bitwise_key_u32(MAX_HASH);
      obj_ranges.obj_end.hobj.nspace = MAX_NSPACE;
    } else {
      obj_ranges.obj_end.hobj.set_bitwise_key_u32(end_hash);
      obj_ranges.temp_end.hobj.set_bitwise_key_u32(end_hash);
    }
  } else {
    obj_ranges.obj_begin.shard_id = shard_id_t::NO_SHARD;
    obj_ranges.obj_begin.hobj.pool = -1ull;

    obj_ranges.obj_end = obj_ranges.obj_begin;
    obj_ranges.obj_begin.hobj.set_bitwise_key_u32(0);
    obj_ranges.obj_end.hobj.set_bitwise_key_u32(MAX_HASH);
    obj_ranges.obj_end.hobj.nspace = MAX_NSPACE;
    // no separate temp section
    obj_ranges.temp_begin = obj_ranges.obj_end;
    obj_ranges.temp_end = obj_ranges.obj_end;
  }

  obj_ranges.obj_begin.generation = 0;
  obj_ranges.obj_end.generation = 0;
  obj_ranges.temp_begin.generation = 0;
  obj_ranges.temp_end.generation = 0;
  return obj_ranges;
}

static std::list<std::pair<ghobject_t, ghobject_t>>
get_ranges(CollectionRef ch,
           ghobject_t start,
           ghobject_t end,
           col_obj_ranges_t obj_ranges)
{
  ceph_assert(start <= end);
  std::list<std::pair<ghobject_t, ghobject_t>> ranges;
  if (start < obj_ranges.temp_end) {
    ranges.emplace_back(
      std::max(obj_ranges.temp_begin, start),
      std::min(obj_ranges.temp_end, end));
  }
  if (end > obj_ranges.obj_begin) {
    ranges.emplace_back(
      std::max(obj_ranges.obj_begin, start),
      std::min(obj_ranges.obj_end, end));
  }
  return ranges;
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
SeaStore::Shard::list_objects(CollectionRef ch,
                       const ghobject_t& start,
                       const ghobject_t& end,
                       uint64_t limit) const
{
  ceph_assert(start <= end);
  using list_iertr = OnodeManager::list_onodes_iertr;
  using RetType = typename OnodeManager::list_onodes_bare_ret;
  return seastar::do_with(
    RetType(std::vector<ghobject_t>(), start),
    std::move(limit),
    [this, ch, start, end](auto& ret, auto& limit) {
    return repeat_eagain([this, ch, start, end, &limit, &ret] {
      return transaction_manager->with_transaction_intr(
        Transaction::src_t::READ,
        "list_objects",
        [this, ch, start, end, &limit, &ret](auto &t)
      {
        return get_coll_bits(
          ch, t
	).si_then([this, ch, &t, start, end, &limit, &ret](auto bits) {
          if (!bits) {
            return list_iertr::make_ready_future<
              OnodeManager::list_onodes_bare_ret
	      >(std::make_tuple(
		  std::vector<ghobject_t>(),
		  ghobject_t::get_max()));
          } else {
	    LOG_PREFIX(SeaStore::list_objects);
	    DEBUGT("start {}, end {}, limit {}, bits {}",
	      t, start, end, limit, *bits);
            auto filter = SeaStore::get_objs_range(ch, *bits);
	    using list_iertr = OnodeManager::list_onodes_iertr;
	    using repeat_ret = list_iertr::future<seastar::stop_iteration>;
            return trans_intr::repeat(
              [this, &t, &ret, &limit, end,
	       filter, ranges = get_ranges(ch, start, end, filter)
	      ]() mutable -> repeat_ret {
		if (limit == 0 || ranges.empty()) {
		  return list_iertr::make_ready_future<
		    seastar::stop_iteration
		    >(seastar::stop_iteration::yes);
		}
		auto ite = ranges.begin();
		auto pstart = ite->first;
		auto pend = ite->second;
		ranges.pop_front();
		LOG_PREFIX(SeaStore::list_objects);
		DEBUGT("pstart {}, pend {}, limit {}", t, pstart, pend, limit);
		return onode_manager->list_onodes(
		  t, pstart, pend, limit
		).si_then([&limit, &ret, pend, &t, last=ranges.empty(), end]
			  (auto &&_ret) mutable {
		  auto &next_objects = std::get<0>(_ret);
		  auto &ret_objects = std::get<0>(ret);
		  ret_objects.insert(
		    ret_objects.end(),
		    next_objects.begin(),
		    next_objects.end());
		  std::get<1>(ret) = std::get<1>(_ret);
		  assert(limit >= next_objects.size());
		  limit -= next_objects.size();
		  LOG_PREFIX(SeaStore::list_objects);
		  DEBUGT("got {} objects, left limit {}",
		    t, next_objects.size(), limit);
		  assert(limit == 0 ||
			 std::get<1>(ret) == pend ||
			 std::get<1>(ret) == ghobject_t::get_max());
		  if (last && std::get<1>(ret) == pend) {
		    std::get<1>(ret) = end;
		  }
		  return list_iertr::make_ready_future<
		    seastar::stop_iteration
		    >(seastar::stop_iteration::no);
		});
	      }).si_then([&ret] {
		return list_iertr::make_ready_future<
		  OnodeManager::list_onodes_bare_ret>(std::move(ret));
	      });
          }
        });
      }).safe_then([&ret](auto&& _ret) {
        ret = std::move(_ret);
      });
    }).safe_then([&ret] {
      return std::move(ret);
    }).handle_error(
      crimson::ct_error::assert_all{
        "Invalid error in SeaStore::list_objects"
      }
    );
  });
}

seastar::future<CollectionRef>
SeaStore::Shard::create_new_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStore::create_new_collection);
  DEBUG("{}", cid);
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<CollectionRef>
SeaStore::Shard::open_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStore::open_collection);
  DEBUG("{}", cid);
  return list_collections().then([cid, this] (auto colls_cores) {
    if (auto found = std::find(colls_cores.begin(),
                               colls_cores.end(),
                               std::make_pair(cid, seastar::this_shard_id()));
      found != colls_cores.end()) {
      return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
    } else {
      return seastar::make_ready_future<CollectionRef>();
    }
  });
}

seastar::future<>
SeaStore::Shard::set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts)
{
  //TODO
  return seastar::now();
}

seastar::future<std::vector<coll_core_t>>
SeaStore::Shard::list_collections()
{
  return seastar::do_with(
    std::vector<coll_core_t>(),
    [this](auto &ret) {
      return repeat_eagain([this, &ret] {
        return transaction_manager->with_transaction_intr(
          Transaction::src_t::READ,
          "list_collections",
          [this, &ret](auto& t)
        {
          return transaction_manager->read_collection_root(t
          ).si_then([this, &t](auto coll_root) {
            return collection_manager->list(coll_root, t);
          }).si_then([&ret](auto colls) {
            ret.resize(colls.size());
            std::transform(
              colls.begin(), colls.end(), ret.begin(),
              [](auto p) {
              return std::make_pair(p.first, seastar::this_shard_id());
            });
          });
        });
      }).safe_then([&ret] {
        return seastar::make_ready_future<std::vector<coll_core_t>>(ret);
      });
    }
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::list_collections"
    }
  );
}

SeaStore::Shard::read_errorator::future<ceph::bufferlist>
SeaStore::Shard::read(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  uint32_t op_flags)
{
  LOG_PREFIX(SeaStore::read);
  DEBUG("oid {} offset {} len {}", oid, offset, len);
  return repeat_with_onode<ceph::bufferlist>(
    ch,
    oid,
    Transaction::src_t::READ,
    "read_obj",
    op_type_t::READ,
    [=, this](auto &t, auto &onode) -> ObjectDataHandler::read_ret {
      size_t size = onode.get_layout().size;

      if (offset >= size) {
	return seastar::make_ready_future<ceph::bufferlist>();
      }

      size_t corrected_len = (len == 0) ?
	size - offset :
	std::min(size - offset, len);

      return ObjectDataHandler(max_object_size).read(
        ObjectDataHandler::context_t{
          *transaction_manager,
          t,
          onode,
        },
        offset,
        corrected_len);
    });
}

SeaStore::Shard::base_errorator::future<bool>
SeaStore::Shard::exists(
  CollectionRef c,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStore::exists);
  DEBUG("oid {}", oid);
  return repeat_with_onode<bool>(
    c,
    oid,
    Transaction::src_t::READ,
    "oid_exists",
    op_type_t::READ,
    [](auto&, auto&) {
    return seastar::make_ready_future<bool>(true);
  }).handle_error(
    crimson::ct_error::enoent::handle([] {
      return seastar::make_ready_future<bool>(false);
    }),
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

SeaStore::Shard::read_errorator::future<ceph::bufferlist>
SeaStore::Shard::readv(
  CollectionRef ch,
  const ghobject_t& _oid,
  interval_set<uint64_t>& m,
  uint32_t op_flags)
{
  return seastar::do_with(
    _oid,
    ceph::bufferlist{},
    [=, this, &m](auto &oid, auto &ret) {
    return crimson::do_for_each(
      m,
      [=, this, &oid, &ret](auto &p) {
      return read(
	ch, oid, p.first, p.second, op_flags
	).safe_then([&ret](auto bl) {
        ret.claim_append(bl);
      });
    }).safe_then([&ret] {
      return read_errorator::make_ready_future<ceph::bufferlist>
        (std::move(ret));
    });
  });
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

using crimson::os::seastore::omap_manager::BtreeOMapManager;

SeaStore::Shard::get_attr_errorator::future<ceph::bufferlist>
SeaStore::Shard::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name) const
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  LOG_PREFIX(SeaStore::get_attr);
  DEBUG("{} {}", c->get_cid(), oid);
  return repeat_with_onode<ceph::bufferlist>(
    c,
    oid,
    Transaction::src_t::READ,
    "get_attr",
    op_type_t::GET_ATTR,
    [=, this](auto &t, auto& onode) -> _omap_get_value_ret {
      auto& layout = onode.get_layout();
      if (name == OI_ATTR && layout.oi_size) {
        ceph::bufferlist bl;
        bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
        return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
      }
      if (name == SS_ATTR && layout.ss_size) {
        ceph::bufferlist bl;
        bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
        return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
      }
      return _omap_get_value(
        t,
        layout.xattr_root.get(
          onode.get_metadata_hint(device->get_block_size())),
        name);
    }
  ).handle_error(
    crimson::ct_error::input_output_error::assert_failure{
      "EIO when getting attrs"},
    crimson::ct_error::pass_further_all{});
}

SeaStore::Shard::get_attrs_ertr::future<SeaStore::Shard::attrs_t>
SeaStore::Shard::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStore::get_attrs);
  auto c = static_cast<SeastoreCollection*>(ch.get());
  DEBUG("{} {}", c->get_cid(), oid);
  return repeat_with_onode<attrs_t>(
    c,
    oid,
    Transaction::src_t::READ,
    "get_addrs",
    op_type_t::GET_ATTRS,
    [=, this](auto &t, auto& onode) {
      auto& layout = onode.get_layout();
      return omap_list(onode, layout.xattr_root, t, std::nullopt,
        OMapManager::omap_list_config_t()
	  .with_inclusive(false, false)
	  .without_max()
      ).si_then([&layout, &t, FNAME](auto p) {
        auto& attrs = std::get<1>(p);
        ceph::bufferlist bl;
        if (layout.oi_size) {
          bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
          attrs.emplace(OI_ATTR, std::move(bl));
         DEBUGT("set oi from onode layout", t);
        }
        if (layout.ss_size) {
          bl.clear();
          bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
          attrs.emplace(SS_ATTR, std::move(bl));
         DEBUGT("set ss from onode layout", t);
        }
	bl.clear();
	encode(layout.local_clone_id, bl);
	attrs.emplace(LC_ATTR, std::move(bl));
        return seastar::make_ready_future<omap_values_t>(std::move(attrs));
      });
    }
  ).handle_error(
    crimson::ct_error::input_output_error::assert_failure{
      "EIO when getting attrs"},
    crimson::ct_error::pass_further_all{});
}

seastar::future<struct stat> SeaStore::Shard::stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStore::stat);
  return repeat_with_onode<struct stat>(
    c,
    oid,
    Transaction::src_t::READ,
    "stat",
    op_type_t::STAT,
    [=, this](auto &t, auto &onode) {
      struct stat st;
      auto &olayout = onode.get_layout();
      st.st_size = olayout.size;
      st.st_blksize = device->get_block_size();
      st.st_blocks = (st.st_size + st.st_blksize - 1) / st.st_blksize;
      st.st_nlink = 1;
      DEBUGT("cid {}, oid {}, return size {}", t, c->get_cid(), oid, st.st_size);
      return seastar::make_ready_future<struct stat>(st);
    }
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::stat"
    }
  );
}

SeaStore::Shard::get_attr_errorator::future<ceph::bufferlist>
SeaStore::Shard::omap_get_header(
  CollectionRef ch,
  const ghobject_t& oid)
{
  return get_attr(ch, oid, OMAP_HEADER_XATTR_KEY);
}

SeaStore::Shard::read_errorator::future<SeaStore::Shard::omap_values_t>
SeaStore::Shard::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const omap_keys_t &keys)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  return repeat_with_onode<omap_values_t>(
    c,
    oid,
    Transaction::src_t::READ,
    "omap_get_values",
    op_type_t::OMAP_GET_VALUES,
    [this, keys](auto &t, auto &onode) {
      omap_root_t omap_root = onode.get_layout().omap_root.get(
	onode.get_metadata_hint(device->get_block_size()));
      return _omap_get_values(
	t,
	std::move(omap_root),
	keys);
    });
}

SeaStore::Shard::_omap_get_value_ret
SeaStore::Shard::_omap_get_value(
  Transaction &t,
  omap_root_t &&root,
  std::string_view key) const
{
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    std::move(root),
    std::string(key),
    [&t](auto &manager, auto& root, auto& key) -> _omap_get_value_ret {
      if (root.is_null()) {
        return crimson::ct_error::enodata::make();
      }
      return manager.omap_get_value(root, t, key
      ).si_then([](auto opt) -> _omap_get_value_ret {
        if (!opt) {
          return crimson::ct_error::enodata::make();
        }
        return seastar::make_ready_future<ceph::bufferlist>(std::move(*opt));
      });
    }
  );
}

SeaStore::Shard::_omap_get_values_ret
SeaStore::Shard::_omap_get_values(
  Transaction &t,
  omap_root_t &&omap_root,
  const omap_keys_t &keys) const
{
  if (omap_root.is_null()) {
    return seastar::make_ready_future<omap_values_t>();
  }
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    std::move(omap_root),
    omap_values_t(),
    [&](auto &manager, auto &root, auto &ret) {
      return trans_intr::do_for_each(
        keys.begin(),
        keys.end(),
        [&](auto &key) {
          return manager.omap_get_value(
            root,
            t,
            key
          ).si_then([&ret, &key](auto &&p) {
            if (p) {
              bufferlist bl;
              bl.append(*p);
              ret.emplace(
                std::move(key),
                std::move(bl));
            }
            return seastar::now();
          });
        }
      ).si_then([&ret] {
        return std::move(ret);
      });
    }
  );
}

SeaStore::Shard::omap_list_ret
SeaStore::Shard::omap_list(
  Onode &onode,
  const omap_root_le_t& omap_root,
  Transaction& t,
  const std::optional<std::string>& start,
  OMapManager::omap_list_config_t config) const
{
  auto root = omap_root.get(
    onode.get_metadata_hint(device->get_block_size()));
  if (root.is_null()) {
    return seastar::make_ready_future<omap_list_bare_ret>(
      true, omap_values_t{}
    );
  }
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    root,
    start,
    std::optional<std::string>(std::nullopt),
    [&t, config](auto &manager, auto &root, auto &start, auto &end) {
      return manager.omap_list(root, t, start, end, config);
  });
}

SeaStore::Shard::omap_get_values_ret_t
SeaStore::Shard::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  LOG_PREFIX(SeaStore::omap_get_values);
  DEBUG("{} {}", c->get_cid(), oid);
  using ret_bare_t = std::tuple<bool, SeaStore::Shard::omap_values_t>;
  return repeat_with_onode<ret_bare_t>(
    c,
    oid,
    Transaction::src_t::READ,
    "omap_list",
    op_type_t::OMAP_LIST,
    [this, start](auto &t, auto &onode) {
      return omap_list(
	onode,
	onode.get_layout().omap_root,
	t,
	start,
	OMapManager::omap_list_config_t()
	  .with_inclusive(false, false)
	  .without_max());
  });
}

SeaStore::Shard::_fiemap_ret SeaStore::Shard::_fiemap(
  Transaction &t,
  Onode &onode,
  uint64_t off,
  uint64_t len) const
{
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &t, &onode] (auto &objhandler) {
    return objhandler.fiemap(
      ObjectDataHandler::context_t{
        *transaction_manager,
        t,
        onode,
      },
      off,
      len);
  });
}

SeaStore::Shard::read_errorator::future<std::map<uint64_t, uint64_t>>
SeaStore::Shard::fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  LOG_PREFIX(SeaStore::fiemap);
  DEBUG("oid: {}, off: {}, len: {} ", oid, off, len);
  return repeat_with_onode<std::map<uint64_t, uint64_t>>(
    ch,
    oid,
    Transaction::src_t::READ,
    "fiemap_read",
    op_type_t::READ,
    [=, this](auto &t, auto &onode) -> _fiemap_ret {
    size_t size = onode.get_layout().size;
    if (off >= size) {
      INFOT("fiemap offset is over onode size!", t);
      return seastar::make_ready_future<std::map<uint64_t, uint64_t>>();
    }
    size_t adjust_len = (len == 0) ?
      size - off:
      std::min(size - off, len);
    return _fiemap(t, onode, off, adjust_len);
  });
}

void SeaStore::Shard::on_error(ceph::os::Transaction &t) {
  LOG_PREFIX(SeaStore::on_error);
  ERROR(" transaction dump:\n");
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t.dump(&f);
  f.close_section();
  std::stringstream str;
  f.flush(str);
  ERROR("{}", str.str());
  abort();
}

seastar::future<> SeaStore::Shard::do_transaction_no_callbacks(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  // repeat_with_internal_context ensures ordering via collection lock
  return repeat_with_internal_context(
    _ch,
    std::move(_t),
    Transaction::src_t::MUTATE,
    "do_transaction",
    op_type_t::TRANSACTION,
    [this](auto &ctx) {
      return with_trans_intr(*ctx.transaction, [&, this](auto &t) {
        LOG_PREFIX(SeaStore::Shard::do_transaction_no_callbacks);
        SUBDEBUGT(seastore_t, "start with {} objects",
                  t, ctx.iter.objects.size());
#ifndef NDEBUG
	TRACET(" transaction dump:\n", t);
	JSONFormatter f(true);
	f.open_object_section("transaction");
	ctx.ext_transaction.dump(&f);
	f.close_section();
	std::stringstream str;
	f.flush(str);
	TRACET("{}", t, str.str());
#endif
        return seastar::do_with(
	  std::vector<OnodeRef>(ctx.iter.objects.size()),
          std::vector<OnodeRef>(ctx.iter.objects.size()),
	  std::map<ghobject_t, removed_info_t>(),
          [this, &ctx](auto& onodes, auto& d_onodes, auto& removed_info) mutable {
          return trans_intr::repeat(
            [this, &ctx, &onodes, &d_onodes, &removed_info]() mutable
            -> tm_iertr::future<seastar::stop_iteration>
            {
              if (ctx.iter.have_op()) {
                return _do_transaction_step(
                  ctx, ctx.ch, onodes, d_onodes, removed_info, ctx.iter
                ).si_then([] {
                  return seastar::make_ready_future<seastar::stop_iteration>(
                    seastar::stop_iteration::no);
                });
              } else {
                return seastar::make_ready_future<seastar::stop_iteration>(
                  seastar::stop_iteration::yes);
              };
            });
        }).si_then([this, &ctx] {
          return transaction_manager->submit_transaction(*ctx.transaction);
        });
      });
    });
}


seastar::future<> SeaStore::Shard::flush(CollectionRef ch)
{
  return seastar::do_with(
    get_dummy_ordering_handle(),
    [this, ch](auto &handle) {
      return handle.take_collection_lock(
	static_cast<SeastoreCollection&>(*ch).ordering_lock
      ).then([this, &handle] {
	return transaction_manager->flush(handle);
      });
    });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_do_transaction_step(
  internal_context_t &ctx,
  CollectionRef &col,
  std::vector<OnodeRef> &onodes,
  std::vector<OnodeRef> &d_onodes,
  std::map<ghobject_t, removed_info_t> &removed_info,
  ceph::os::Transaction::iterator &i)
{
  LOG_PREFIX(SeaStore::Shard::_do_transaction_step);
  auto op = i.decode_op();
  SUBTRACET(seastore_t, "got op {}", *ctx.transaction, op->op);

  using ceph::os::Transaction;
  if (op->op == Transaction::OP_NOP)
    return tm_iertr::now();

  switch (op->op) {
    case Transaction::OP_RMCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _remove_collection(ctx, cid);
    }
    case Transaction::OP_MKCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _create_collection(ctx, cid, op->split_bits);
    }
    case Transaction::OP_COLL_HINT:
    {
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return tm_iertr::now();
    }
  }

  using onode_iertr = OnodeManager::get_onode_iertr::extend<
    crimson::ct_error::value_too_large>;
  auto fut = onode_iertr::make_ready_future<OnodeRef>(OnodeRef());
  bool create = false;
  if (op->op == Transaction::OP_TOUCH ||
      op->op == Transaction::OP_CREATE ||
      op->op == Transaction::OP_WRITE ||
      op->op == Transaction::OP_ZERO) {
    create = true;
  }
  if (!onodes[op->oid]) {
    if (!create) {
      fut = onode_manager->get_onode(*ctx.transaction, i.get_oid(op->oid));
    } else {
      fut = onode_manager->get_or_create_onode(
        *ctx.transaction, i.get_oid(op->oid));
    }
  }
  return fut.si_then([&, op](auto get_onode) {
    OnodeRef &o = onodes[op->oid];
    if (!o) {
      assert(get_onode);
      o = get_onode;
      d_onodes[op->oid] = get_onode;
    }
    if ((op->op == Transaction::OP_CLONE
	  || op->op == Transaction::OP_CLONERANGE2
	  || op->op == Transaction::OP_COLL_MOVE_RENAME)
	&& !d_onodes[op->dest_oid]) {
      //TODO: use when_all_succeed after making onode tree
      //      support parallel extents loading
      return onode_manager->get_or_create_onode(
	*ctx.transaction, i.get_oid(op->dest_oid)
      ).si_then([&, op](auto dest_onode) {
	assert(dest_onode);
	auto &d_o = onodes[op->dest_oid];
	assert(!d_o);
	assert(!d_onodes[op->dest_oid]);
	d_o = dest_onode;
	d_onodes[op->dest_oid] = dest_onode;
	return seastar::now();
      });
    } else {
      return OnodeManager::get_or_create_onode_iertr::now();
    }
  }).si_then([&, op, this]() -> tm_ret {
    LOG_PREFIX(SeaStore::_do_transaction_step);
    try {
      switch (op->op) {
      case Transaction::OP_REMOVE:
      {
	TRACET("removing {}", *ctx.transaction, i.get_oid(op->oid));
	auto &layout = onodes[op->oid]->get_layout();
	auto laddr = layout.object_data.get().get_reserved_data_base();
	auto id = layout.local_clone_id;
	removed_info.emplace(i.get_oid(op->oid), removed_info_t{laddr, id});
        return _remove(ctx, i.get_oid(op->oid), onodes[op->oid]
	).si_then([&onodes, &d_onodes, op] {
	  onodes[op->oid].reset();
	  d_onodes[op->oid].reset();
	});
      }
      case Transaction::OP_CREATE:
      case Transaction::OP_TOUCH:
      {
	return process_touch_hint(ctx, onodes, d_onodes, removed_info, i, op
	).si_then([&ctx, &onodes, op, this](auto hint) {
	  return _touch(ctx, onodes[op->oid], hint);
	});
      }
      case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        ceph::bufferlist bl;
        i.decode_bl(bl);
        return _write(
	  ctx, onodes[op->oid], off, len, std::move(bl),
	  fadvise_flags);
      }
      case Transaction::OP_TRUNCATE:
      {
        uint64_t off = op->off;
        return _truncate(ctx, onodes[op->oid], off);
      }
      case Transaction::OP_SETATTR:
      {
        std::string name = i.decode_string();
        std::map<std::string, bufferlist> to_set;
        ceph::bufferlist& bl = to_set[name];
        i.decode_bl(bl);
        return _setattrs(ctx, onodes[op->oid], std::move(to_set));
      }
      case Transaction::OP_SETATTRS:
      {
        std::map<std::string, bufferlist> to_set;
        i.decode_attrset(to_set);
        return _setattrs(ctx, onodes[op->oid], std::move(to_set));
      }
      case Transaction::OP_RMATTR:
      {
        std::string name = i.decode_string();
        return _rmattr(ctx, onodes[op->oid], name);
      }
      case Transaction::OP_RMATTRS:
      {
        return _rmattrs(ctx, onodes[op->oid]);
      }
      case Transaction::OP_OMAP_SETKEYS:
      {
        std::map<std::string, ceph::bufferlist> aset;
        i.decode_attrset(aset);
        return _omap_set_values(ctx, onodes[op->oid], std::move(aset));
      }
      case Transaction::OP_OMAP_SETHEADER:
      {
        ceph::bufferlist bl;
        i.decode_bl(bl);
        return _omap_set_header(ctx, onodes[op->oid], std::move(bl));
      }
      case Transaction::OP_OMAP_RMKEYS:
      {
        omap_keys_t keys;
        i.decode_keyset(keys);
        return _omap_rmkeys(ctx, onodes[op->oid], std::move(keys));
      }
      case Transaction::OP_OMAP_RMKEYRANGE:
      {
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        return _omap_rmkeyrange(
	  ctx, onodes[op->oid],
	  std::move(first), std::move(last));
      }
      case Transaction::OP_OMAP_CLEAR:
      {
        return _omap_clear(ctx, onodes[op->oid]);
      }
      case Transaction::OP_ZERO:
      {
        objaddr_t off = op->off;
        extent_len_t len = op->len;
        return _zero(ctx, onodes[op->oid], off, len);
      }
      case Transaction::OP_SETALLOCHINT:
      {
        // TODO
        return tm_iertr::now();
      }
      case Transaction::OP_CLONE:
      {
	auto &src_ghobj = i.get_oid(op->oid);
	auto &dest_ghobj = i.get_oid(op->dest_oid);
	auto &src_onode = onodes[op->oid];
	auto &dest_onode = d_onodes[op->dest_oid];
	TRACET("cloning {} to {}", *ctx.transaction, src_ghobj, dest_ghobj);
	bool is_rollback = dest_ghobj.hobj.is_head();

	auto data_hint = L_ADDR_NULL;

	auto &src_layout = src_onode->get_layout();
	auto obj_data = src_layout.object_data.get();
	auto length = obj_data.get_reserved_data_len();
	if (!is_rollback) {
	  assert(!obj_data.is_null());
	  local_clone_id_t clone_id = src_layout.local_clone_id;
	  auto base = obj_data.get_reserved_data_base();
	  assert(base.get_local_clone_id() == 0);

	  // move data to hint
	  clone_id++;
	  data_hint = base.with_local_clone_id(clone_id);

	  // update max local clone id of an object, create indirect mapping to hint
	  clone_id++;
	  src_onode->update_local_clone_id(*ctx.transaction, clone_id);
	  dest_onode->update_local_clone_id(*ctx.transaction, clone_id);
	  obj_data.update_reserved(base.with_local_clone_id(clone_id), length);
	  dest_onode->update_object_data(*ctx.transaction, obj_data);
	} else {
	  auto iter = removed_info.find(dest_ghobj);
	  assert(iter != removed_info.end());
	  auto &r = iter->second;
	  obj_data.update_reserved(r.removed_laddr, length);
	  dest_onode->update_local_clone_id(*ctx.transaction, r.clone_id);
	  dest_onode->update_object_data(*ctx.transaction, obj_data);
	}
	return _clone(ctx, onodes[op->oid], d_onodes[op->dest_oid], data_hint);
      }
      case Transaction::OP_CLONERANGE2:
      {
	assert(op->off <= std::numeric_limits<extent_len_t>::max());
	assert(op->len <= std::numeric_limits<extent_len_t>::max());
	assert(op->dest_off <= std::numeric_limits<extent_len_t>::max());
        extent_len_t srcoff = (extent_len_t)op->off;
        extent_len_t len = (extent_len_t)op->len;
        extent_len_t dstoff = (extent_len_t)op->dest_off;
	return _clone_range(
	  ctx,
	  onodes[op->oid],
	  d_onodes[op->dest_oid],
	  srcoff,
	  len,
	  dstoff);
      }
      case Transaction::OP_COLL_MOVE_RENAME:
      {
	ceph_assert(op->cid == op->dest_cid);
	TRACET("renaming {} to {}",
	  *ctx.transaction,
	  i.get_oid(op->oid),
	  i.get_oid(op->dest_oid));
	return _rename(
	  ctx, onodes[op->oid], d_onodes[op->dest_oid]
	).si_then([&onodes, &d_onodes, op] {
	  onodes[op->oid].reset();
	  d_onodes[op->oid].reset();
	});
      }
      default:
        ERROR("bad op {}", static_cast<unsigned>(op->op));
        return crimson::ct_error::input_output_error::make();
      }
    } catch (std::exception &e) {
      ERROR("got exception {}", e);
      return crimson::ct_error::input_output_error::make();
    }
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::enoent::handle([op] {
      //OMAP_CLEAR, TRUNCATE, REMOVE etc ops will tolerate absent onode.
      if (op->op == Transaction::OP_CLONERANGE ||
          op->op == Transaction::OP_CLONE ||
          op->op == Transaction::OP_COLL_ADD ||
          op->op == Transaction::OP_SETATTR ||
          op->op == Transaction::OP_SETATTRS ||
          op->op == Transaction::OP_RMATTR ||
          op->op == Transaction::OP_OMAP_SETKEYS ||
          op->op == Transaction::OP_OMAP_RMKEYS ||
          op->op == Transaction::OP_OMAP_RMKEYRANGE ||
          op->op == Transaction::OP_OMAP_SETHEADER) {
        ceph_abort_msg("unexpected enoent error");
      }
      return seastar::now();
    }),
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::do_transaction_step"
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rename(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode)
{
  auto olayout = onode->get_layout();
  d_onode->update_local_clone_id(
    *ctx.transaction, olayout.local_clone_id);
  uint32_t size = olayout.size;
  auto omap_root = olayout.omap_root.get(
    d_onode->get_metadata_hint(device->get_block_size()));
  auto xattr_root = olayout.xattr_root.get(
    d_onode->get_metadata_hint(device->get_block_size()));
  auto object_data = olayout.object_data.get();
  auto oi_bl = ceph::bufferlist::static_from_mem(
    &olayout.oi[0],
    (uint32_t)olayout.oi_size);
  auto ss_bl = ceph::bufferlist::static_from_mem(
    &olayout.ss[0],
    (uint32_t)olayout.ss_size);

  d_onode->update_onode_size(*ctx.transaction, size);
  d_onode->update_omap_root(*ctx.transaction, omap_root);
  d_onode->update_xattr_root(*ctx.transaction, xattr_root);
  d_onode->update_object_info(*ctx.transaction, oi_bl);
  d_onode->update_snapset(*ctx.transaction, ss_bl);
  auto fut = TransactionManager::move_mappings_iertr::make_ready_future<
    lba_pin_list_t>();
  auto base = object_data.get_reserved_data_base();
  if (base.is_recover()) {
    auto obase = base.without_recover();
    auto olen = object_data.get_reserved_data_len();
    object_data_t n_object_data(obase, olen);
    d_onode->update_object_data(*ctx.transaction, n_object_data);
    fut = transaction_manager->merge_mappings<ObjectDataBlock>(
      *ctx.transaction, obase, base, olen
    ).si_then([&ctx, base, obase, olen, this](auto) {
      return transaction_manager->move_mappings<ObjectDataBlock>(
	*ctx.transaction, base, obase, olen, false);
    });
  } else {
    d_onode->update_object_data(*ctx.transaction, object_data);
  }
  return fut.si_then([&ctx, onode, this](auto) mutable {
    return onode_manager->erase_onode(*ctx.transaction, onode);
  }).handle_error_interruptible(
    crimson::ct_error::input_output_error::pass_further(),
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::_rename"}
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove_omaps(
  internal_context_t &ctx,
  OnodeRef &onode,
  omap_root_t &&omap_root)
{
  if (omap_root.get_location() != L_ADDR_NULL) {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      std::move(omap_root),
      [&ctx, onode](auto &omap_manager, auto &omap_root) {
      return omap_manager.omap_clear(
	omap_root,
	*ctx.transaction
      ).handle_error_interruptible(
	crimson::ct_error::input_output_error::pass_further(),
	crimson::ct_error::assert_all{
	  "Invalid error in SeaStore::_remove"
	}
      );
    });
  }
  return tm_iertr::now();
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove(
  internal_context_t &ctx,
  const ghobject_t &hobj,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_remove);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto prefix = onode->get_layout().object_data.get()
    .get_reserved_data_base().get_object_prefix();
  return _remove_omaps(
    ctx,
    onode,
    onode->get_layout().omap_root.get(
      onode->get_metadata_hint(device->get_block_size()))
  ).si_then([this, &ctx, onode]() mutable {
    return _remove_omaps(
      ctx,
      onode,
      onode->get_layout().xattr_root.get(
	onode->get_metadata_hint(device->get_block_size())));
  }).si_then([this, &ctx, onode] {
    return seastar::do_with(
      ObjectDataHandler(max_object_size),
      [=, this, &ctx](auto &objhandler) {
	return objhandler.clear(
	  ObjectDataHandler::context_t{
	    *transaction_manager,
	    *ctx.transaction,
	    *onode,
	  });
    });
  }).si_then([this, &ctx, &hobj, prefix, onode]() mutable {
    return onode_manager->erase_onode(*ctx.transaction, onode
    ).si_then([this, &ctx, &hobj, prefix] {
      return seastar::do_with(
        hobj, hobj,
	[this, &ctx, prefix](auto &start, auto &end) {
	  start.hobj.snap = 0;
	  end.hobj.snap = CEPH_SNAPDIR;
	  return onode_manager->contains_onode(
	    *ctx.transaction,
	    start,
	    end
	  ).si_then([&ctx, prefix](auto result) {
	    if (!result) {
	      ctx.transaction->update_obj_info(
	        prefix,
		extent_types_t::OBJECT_DATA_BLOCK,
		Transaction::obj_op_t::REMOVE);
	    }
	  });
	});
    });
  }).handle_error_interruptible(
    crimson::ct_error::input_output_error::pass_further(),
    crimson::ct_error::assert_all(
      "Invalid error in SeaStore::_remove"
    )
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_touch(
  internal_context_t &ctx,
  OnodeRef &onode,
  laddr_t hint)
{
  LOG_PREFIX(SeaStore::_touch);
  DEBUGT("onode={}, hint={}", *ctx.transaction, *onode, hint);
  if (!onode->get_layout().object_data.get().is_null()) {
    return tm_iertr::now();
  }

  bool determinsitic = (hint != L_ADDR_NULL);
  if (hint == L_ADDR_NULL) {
    hint = onode->get_data_hint();
  }

  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [this, &ctx, &onode, hint, determinsitic](auto &objhandler) {
      return objhandler.touch(
	ObjectDataHandler::context_t{
	  *transaction_manager,
	  *ctx.transaction,
	  *onode,
	  nullptr,
	  hint,
	  determinsitic
	});
    });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_write(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t offset, size_t len,
  ceph::bufferlist &&_bl,
  uint32_t fadvise_flags)
{
  LOG_PREFIX(SeaStore::_write);
  DEBUGT("onode={} {}~{}", *ctx.transaction, *onode, offset, len);

  auto &layout = onode->get_layout();
  if (local_clone_id_t(layout.local_clone_id) ==
      LOCAL_CLONE_ID_NULL) {
    assert(layout.object_data.get().is_null());
    onode->update_local_clone_id(*ctx.transaction, 0);
  }
  const auto &object_size = layout.size;
  if (offset + len > object_size) {
    onode->update_onode_size(
      *ctx.transaction,
      std::max<uint64_t>(offset + len, object_size));
  }
  return seastar::do_with(
    std::move(_bl),
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &bl, auto &objhandler) {
      auto laddr = L_ADDR_NULL;
      auto determinsitic = true;
      auto &layout = onode->get_layout();
      if (layout.object_data.get().is_null()) {
	ceph_assert(layout.local_clone_id == 0);
	laddr = onode->get_data_hint();
	determinsitic = false;
      }
      return objhandler.write(
        ObjectDataHandler::context_t{
          *transaction_manager,
          *ctx.transaction,
          *onode,
	  nullptr,
	  laddr,
	  determinsitic
        },
        offset,
        bl);
    });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_clone_omaps(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode,
  const omap_type_t otype)
{
  return trans_intr::repeat([&ctx, onode, d_onode, this, otype] {
    return seastar::do_with(
      std::optional<std::string>(std::nullopt),
      [&ctx, onode, d_onode, this, otype](auto &start) {
      auto& layout = onode->get_layout();
      return omap_list(
	*onode,
	otype == omap_type_t::XATTR
	  ? layout.xattr_root
	  : layout.omap_root,
	*ctx.transaction,
	start,
	OMapManager::omap_list_config_t().with_inclusive(false, false)
      ).si_then([&ctx, onode, d_onode, this, otype, &start](auto p) mutable {
	auto complete = std::get<0>(p);
	auto &attrs = std::get<1>(p);
	if (attrs.empty()) {
	  assert(complete);
	  return tm_iertr::make_ready_future<
	    seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	}
	std::string nstart = attrs.rbegin()->first;
	return _omap_set_kvs(
	  d_onode,
	  otype == omap_type_t::XATTR
	    ? d_onode->get_layout().xattr_root
	    : d_onode->get_layout().omap_root,
	  *ctx.transaction,
	  std::map<std::string, ceph::bufferlist>(attrs.begin(), attrs.end())
	).si_then([complete, nstart=std::move(nstart),
		  &start, &ctx, d_onode, otype](auto root) mutable {
	  if (root.must_update()) {
	    if (otype == omap_type_t::XATTR) {
	      d_onode->update_xattr_root(*ctx.transaction, root);
	    } else {
	      assert(otype == omap_type_t::OMAP);
	      d_onode->update_omap_root(*ctx.transaction, root);
	    }
	  }
	  if (complete) {
	    return seastar::make_ready_future<
	      seastar::stop_iteration>(
		seastar::stop_iteration::yes);
	  } else {
	    start = std::move(nstart);
	    return seastar::make_ready_future<
	      seastar::stop_iteration>(
		seastar::stop_iteration::no);
	  }
	});
      });
    });
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_clone(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode,
  laddr_t data_hint)
{
  LOG_PREFIX(SeaStore::_clone);
  DEBUGT("onode={} d_onode={} data_hint={}",
	 *ctx.transaction, *onode, *d_onode, data_hint);
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [this, &ctx, &onode, &d_onode, data_hint](auto &objHandler) {
    auto &object_size = onode->get_layout().size;
    d_onode->update_onode_size(*ctx.transaction, object_size);
    return objHandler.clone(
      ObjectDataHandler::context_t{
	*transaction_manager,
	*ctx.transaction,
	*onode,
	d_onode.get(),
	data_hint
      });
  }).si_then([&ctx, &onode, &d_onode, this] {
    return _clone_omaps(ctx, onode, d_onode, omap_type_t::XATTR);
  }).si_then([&ctx, &onode, &d_onode, this] {
    return _clone_omaps(ctx, onode, d_onode, omap_type_t::OMAP);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_clone_range(
  internal_context_t &ctx,
  OnodeRef &src_onode,
  OnodeRef &dst_onode,
  extent_len_t srcoff,
  extent_len_t length,
  extent_len_t dstoff)
{
  LOG_PREFIX(SeaStore::_clone_range);
  DEBUGT("src_onode={}, dst_onode={}, src {}~{}, dst {}",
    *ctx.transaction, *src_onode, *dst_onode, srcoff, length, dstoff);
  auto &s_layout = src_onode->get_layout();
  auto s_object_data = s_layout.object_data.get();
  auto s_lc_id = s_object_data.get_reserved_data_base().get_local_clone_id();
  auto &d_layout = dst_onode->get_layout();
  auto d_object_data = d_layout.object_data.get();
  auto d_base = d_object_data.get_reserved_data_base();
  auto d_lc_id = d_base.get_local_clone_id();
  assert(s_lc_id != d_lc_id);
  auto hint_lc_id = std::min(s_lc_id, d_lc_id) + 1;
  assert(hint_lc_id % 2 == 1);
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &ctx](auto &objHandler) {
    return objHandler.clone_range(
      ObjectDataHandler::context_t{
	*transaction_manager,
	*ctx.transaction,
	*src_onode,
	dst_onode.get(),
	d_base.with_local_clone_id(hint_lc_id)},
      srcoff,
      length,
      dstoff);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_zero(
  internal_context_t &ctx,
  OnodeRef &onode,
  objaddr_t offset,
  extent_len_t len)
{
  LOG_PREFIX(SeaStore::_zero);
  DEBUGT("onode={} {}~{}", *ctx.transaction, *onode, offset, len);
  if (offset + len >= max_object_size) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto &object_size = onode->get_layout().size;
  onode->update_onode_size(
    *ctx.transaction,
    std::max<uint64_t>(offset + len, object_size));
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &objhandler) {
      return objhandler.zero(
        ObjectDataHandler::context_t{
          *transaction_manager,
          *ctx.transaction,
          *onode,
        },
        offset,
        len);
  });
}

SeaStore::Shard::omap_set_kvs_ret
SeaStore::Shard::_omap_set_kvs(
  const OnodeRef &onode,
  const omap_root_le_t& omap_root,
  Transaction& t,
  std::map<std::string, ceph::bufferlist>&& kvs)
{
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    omap_root.get(onode->get_metadata_hint(device->get_block_size())),
    [&, keys=std::move(kvs)](auto &omap_manager, auto &root) {
      tm_iertr::future<> maybe_create_root =
        !root.is_null() ?
        tm_iertr::now() :
        omap_manager.initialize_omap(
          t, onode->get_metadata_hint(device->get_block_size())
        ).si_then([&root](auto new_root) {
          root = new_root;
        });
      return maybe_create_root.si_then(
        [&, keys=std::move(keys)]() mutable {
          return omap_manager.omap_set_keys(root, t, std::move(keys));
      }).si_then([&] {
        return tm_iertr::make_ready_future<omap_root_t>(std::move(root));
      });
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_set_values(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  LOG_PREFIX(SeaStore::_omap_set_values);
  DEBUGT("{} {} keys", *ctx.transaction, *onode, aset.size());
  return _omap_set_kvs(
    onode,
    onode->get_layout().omap_root,
    *ctx.transaction,
    std::move(aset)
  ).si_then([onode, &ctx](auto root) {
    if (root.must_update()) {
      onode->update_omap_root(*ctx.transaction, root);
    }
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_set_header(
  internal_context_t &ctx,
  OnodeRef &onode,
  ceph::bufferlist &&header)
{
  LOG_PREFIX(SeaStore::_omap_set_header);
  DEBUGT("{} {} bytes", *ctx.transaction, *onode, header.length());
  std::map<std::string, bufferlist> to_set;
  to_set[OMAP_HEADER_XATTR_KEY] = header;
  return _setattrs(ctx, onode,std::move(to_set));
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_clear(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_omap_clear);
  DEBUGT("{} {} keys", *ctx.transaction, *onode);
  return _xattr_rmattr(ctx, onode, std::string(OMAP_HEADER_XATTR_KEY))
    .si_then([this, &ctx, &onode]() -> tm_ret {
    if (auto omap_root = onode->get_layout().omap_root.get(
      onode->get_metadata_hint(device->get_block_size()));
      omap_root.is_null()) {
      return seastar::now();
    } else {
      return seastar::do_with(
        BtreeOMapManager(*transaction_manager),
        onode->get_layout().omap_root.get(
          onode->get_metadata_hint(device->get_block_size())),
        [&ctx, &onode](
        auto &omap_manager,
        auto &omap_root) {
        return omap_manager.omap_clear(
          omap_root,
          *ctx.transaction)
        .si_then([&] {
          if (omap_root.must_update()) {
	    onode->update_omap_root(*ctx.transaction, omap_root);
          }
        });
      });
    }
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_rmkeys(
  internal_context_t &ctx,
  OnodeRef &onode,
  omap_keys_t &&keys)
{
  LOG_PREFIX(SeaStore::_omap_rmkeys);
  DEBUGT("{} {} keys", *ctx.transaction, *onode, keys.size());
  auto omap_root = onode->get_layout().omap_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().omap_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(keys),
      [&ctx, &onode](
	auto &omap_manager,
	auto &omap_root,
	auto &keys) {
          return trans_intr::do_for_each(
            keys.begin(),
            keys.end(),
            [&](auto &p) {
              return omap_manager.omap_rm_key(
                omap_root,
                *ctx.transaction,
                p);
            }
          ).si_then([&] {
            if (omap_root.must_update()) {
	      onode->update_omap_root(*ctx.transaction, omap_root);
            }
          });
      }
    );
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_rmkeyrange(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string first,
  std::string last)
{
  LOG_PREFIX(SeaStore::_omap_rmkeyrange);
  DEBUGT("{} first={} last={}", *ctx.transaction, *onode, first, last);
  if (first > last) {
    ERRORT("range error, first: {} > last:{}", *ctx.transaction, first, last);
    ceph_abort();
  }
  auto omap_root = onode->get_layout().omap_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().omap_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(first),
      std::move(last),
      [&ctx, &onode](
	auto &omap_manager,
	auto &omap_root,
	auto &first,
	auto &last) {
      auto config = OMapManager::omap_list_config_t()
	.with_inclusive(true, false)
	.without_max();
      return omap_manager.omap_rm_key_range(
	omap_root,
	*ctx.transaction,
	first,
	last,
	config
      ).si_then([&] {
        if (omap_root.must_update()) {
	  onode->update_omap_root(*ctx.transaction, omap_root);
        }
      });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_truncate(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t size)
{
  LOG_PREFIX(SeaStore::_truncate);
  DEBUGT("onode={} size={}", *ctx.transaction, *onode, size);
  onode->update_onode_size(*ctx.transaction, size);
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &objhandler) {
    return objhandler.truncate(
      ObjectDataHandler::context_t{
        *transaction_manager,
        *ctx.transaction,
        *onode
      },
      size);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_setattrs(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, bufferlist>&& aset)
{
  LOG_PREFIX(SeaStore::_setattrs);
  DEBUGT("onode={}", *ctx.transaction, *onode);

  auto fut = tm_iertr::now();
  auto& layout = onode->get_layout();
  if (auto it = aset.find(OI_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_OI_LENGTH)) {

      if (!layout.oi_size) {
	// if oi was not in the layout, it probably exists in the omap,
	// need to remove it first
	fut = _xattr_rmattr(ctx, onode, OI_ATTR);
      }
      onode->update_object_info(*ctx.transaction, val);
      aset.erase(it);
      DEBUGT("set oi in onode layout", *ctx.transaction);
    } else {
      onode->clear_object_info(*ctx.transaction);
    }
  }

  if (auto it = aset.find(SS_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_SS_LENGTH)) {

      if (!layout.ss_size) {
	fut = _xattr_rmattr(ctx, onode, SS_ATTR);
      }
      onode->update_snapset(*ctx.transaction, val);
      aset.erase(it);
      DEBUGT("set ss in onode layout", *ctx.transaction);
    } else {
      onode->clear_snapset(*ctx.transaction);
    }
  }

  if (aset.empty()) {
    DEBUGT("all attrs set in onode layout", *ctx.transaction);
    return fut;
  }

  DEBUGT("set attrs in omap", *ctx.transaction);
  return fut.si_then(
    [this, onode, &ctx, aset=std::move(aset)]() mutable {
    return _omap_set_kvs(
      onode,
      onode->get_layout().xattr_root,
      *ctx.transaction,
      std::move(aset)
    ).si_then([onode, &ctx](auto root) {
      if (root.must_update()) {
	onode->update_xattr_root(*ctx.transaction, root);
      }
    });
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rmattr(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string name)
{
  LOG_PREFIX(SeaStore::_rmattr);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto& layout = onode->get_layout();
  if ((name == OI_ATTR) && (layout.oi_size > 0)) {
    onode->clear_object_info(*ctx.transaction);
    return tm_iertr::now();
  } else if ((name == SS_ATTR) && (layout.ss_size > 0)) {
    onode->clear_snapset(*ctx.transaction);
    return tm_iertr::now();
  } else {
    return _xattr_rmattr(
      ctx,
      onode,
      std::move(name));
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_xattr_rmattr(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string &&name)
{
  LOG_PREFIX(SeaStore::_xattr_rmattr);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto xattr_root = onode->get_layout().xattr_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (xattr_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().xattr_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(name),
      [&ctx, &onode](auto &omap_manager, auto &xattr_root, auto &name) {
        return omap_manager.omap_rm_key(xattr_root, *ctx.transaction, name)
          .si_then([&] {
          if (xattr_root.must_update()) {
	    onode->update_xattr_root(*ctx.transaction, xattr_root);
          }
        });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rmattrs(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_rmattrs);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  onode->clear_object_info(*ctx.transaction);
  onode->clear_snapset(*ctx.transaction);
  return _xattr_clear(ctx, onode);
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_xattr_clear(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_xattr_clear);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto xattr_root = onode->get_layout().xattr_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (xattr_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().xattr_root.get(
	onode->get_metadata_hint(device->get_block_size())),
      [&ctx, &onode](auto &omap_manager, auto &xattr_root) {
        return omap_manager.omap_clear(xattr_root, *ctx.transaction)
	  .si_then([&] {
	  if (xattr_root.must_update()) {
	    onode->update_xattr_root(*ctx.transaction, xattr_root);
          }
        });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_create_collection(
  internal_context_t &ctx,
  const coll_t& cid, int bits)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).si_then([=, this, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, this, &ctx](auto &cmroot) {
        return collection_manager->create(
          cmroot,
          *ctx.transaction,
          cid,
          bits
        ).si_then([this, &ctx, &cmroot] {
          if (cmroot.must_update()) {
            transaction_manager->write_collection_root(
              *ctx.transaction,
              cmroot);
          }
        });
      }
    );
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::_create_collection"
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove_collection(
  internal_context_t &ctx,
  const coll_t& cid)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).si_then([=, this, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, this, &ctx](auto &cmroot) {
        return collection_manager->remove(
          cmroot,
          *ctx.transaction,
          cid
        ).si_then([this, &ctx, &cmroot] {
          // param here denotes whether it already existed, probably error
          if (cmroot.must_update()) {
            transaction_manager->write_collection_root(
              *ctx.transaction,
              cmroot);
          }
        });
      });
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::_create_collection"
    }
  );
}

boost::intrusive_ptr<SeastoreCollection>
SeaStore::Shard::_get_collection(const coll_t& cid)
{
  return new SeastoreCollection{cid};
}

SeaStore::Shard::process_touch_hint_ret
SeaStore::Shard::process_touch_hint(
  internal_context_t &ctx,
  std::vector<OnodeRef> &onodes,
  std::vector<OnodeRef> &d_onodes,
  std::map<ghobject_t, removed_info_t> &removed_info,
  ceph::os::Transaction::iterator &i,
  ceph::os::Transaction::Op *op) const
{
  LOG_PREFIX(SeaStore::process_touch_hint);
  auto &ghobj = i.get_oid(op->oid);
  auto &onode = onodes[op->oid];
  const auto &layout = onode->get_layout();
  auto input_id = LOCAL_CLONE_ID_NULL;
  if (op->op == ceph::os::Transaction::OP_TOUCH) {
    ceph_le32 input_id_le;
    i.decode_u32(input_id_le);
    input_id = input_id_le;
  } else {
    ceph_assert(op->op == ceph::os::Transaction::OP_CREATE);
  }
  DEBUGT("input clone id: {}", *ctx.transaction, input_id);

  if (input_id == LOCAL_CLONE_ID_NULL) {
    // issued from clients, not from recover
    ceph_assert(ghobj.hobj.is_head());
    if (local_clone_id_t(layout.local_clone_id) != LOCAL_CLONE_ID_NULL) {
      assert(!layout.object_data.get().is_null());
      // onode extents, return directly
      return tm_iertr::make_ready_future<laddr_t>(L_ADDR_NULL);
    } else if (auto iter = removed_info.find(ghobj);
	       iter != removed_info.end()) {
      auto &info = iter->second;
      onode->update_local_clone_id(*ctx.transaction, info.clone_id);
      ceph_assert(info.removed_laddr != L_ADDR_NULL);
      return tm_iertr::make_ready_future<laddr_t>(info.removed_laddr);
    } else {
      onode->update_local_clone_id(*ctx.transaction, 0);
#ifndef NDEBUG
      return onode_manager->get_latest_snap_and_head(
	*ctx.transaction, ghobj
      ).si_then([](auto p) mutable {
	assert(!p.first);
	assert(!p.second);
	return tm_iertr::make_ready_future<laddr_t>(L_ADDR_NULL);
      });
#else
      return tm_iertr::make_ready_future<laddr_t>(L_ADDR_NULL);
#endif
    }
  } else {
    // issued from other OSD
    onode->update_local_clone_id(*ctx.transaction, input_id);
    bool is_temp_obj =
      ghobj.hobj.oid.name.starts_with(TEMP_RECOVERING_OBJ_PREFIX);
    bool target_is_head = false;
    auto p_ghobj = &ghobj;
    if (is_temp_obj) {
      auto &dst_ghobj = i.get_oid(op->dest_oid);
      p_ghobj = &dst_ghobj;
      target_is_head = dst_ghobj.hobj.is_head();
    }
    auto obj = *p_ghobj;
    obj.hobj.snap = CEPH_NOSNAP;
    return seastar::do_with(
      *p_ghobj,
      [this, &ctx, &onode, target_is_head,
       input_id, is_temp_obj](auto &src_ghobj) {
	return onode_manager->get_latest_snap_and_head(
	  *ctx.transaction, src_ghobj
	).si_then([&onode, target_is_head,
		   input_id, is_temp_obj](auto res) {
	  auto [latest, head] = std::move(res);

	  auto get_onode_base = [](const OnodeRef &onode) {
	    auto obj_data = onode->get_layout().object_data.get();
	    return obj_data.is_null()
	      ? L_ADDR_NULL
	      : obj_data.get_reserved_data_base();
	  };

	  auto hint = L_ADDR_NULL;

	  if (head) {
	    if (latest) {
	      assert(get_onode_base(latest).get_object_prefix() ==
		     get_onode_base(head).get_object_prefix());
	    }
	    assert(input_id <= head->get_layout().local_clone_id);
	    hint = get_onode_base(head).with_local_clone_id(input_id);
	  }

	  if (latest && hint == L_ADDR_NULL) {
	    hint = get_onode_base(latest).with_local_clone_id(input_id);
	  }

	  if (hint == L_ADDR_NULL) {
	    hint = onode->get_data_hint();
	    assert(hint.get_local_clone_id() == input_id);
	  }

	  if (is_temp_obj) {
	    hint = hint.with_recover();
	  }

	  if (target_is_head) {
	    hint = hint.with_local_clone_id(0);
	  }

	  ceph_assert(hint != L_ADDR_NULL);
	  return tm_iertr::make_ready_future<laddr_t>(hint);
	});
      });
  }
}

seastar::future<> SeaStore::Shard::write_meta(
  const std::string& key,
  const std::string& value)
{
  LOG_PREFIX(SeaStore::write_meta);
  DEBUG("key: {}; value: {}", key, value);
  return seastar::do_with(
      key, value,
      [this, FNAME](auto& key, auto& value) {
	return repeat_eagain([this, FNAME, &key, &value] {
	  return transaction_manager->with_transaction_intr(
	    Transaction::src_t::MUTATE,
            "write_meta",
	    [this, FNAME, &key, &value](auto& t)
          {
            DEBUGT("Have transaction, key: {}; value: {}", t, key, value);
            return transaction_manager->update_root_meta(
              t, key, value
            ).si_then([this, &t] {
              return transaction_manager->submit_transaction(t);
            });
          });
	});
      }).handle_error(
	crimson::ct_error::assert_all{"Invalid error in SeaStore::write_meta"}
      );
}

seastar::future<std::tuple<int, std::string>>
SeaStore::read_meta(const std::string& key)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  LOG_PREFIX(SeaStore::read_meta);
  DEBUG("key: {}", key);
  return mdstore->read_meta(key).safe_then([](auto v) {
    if (v) {
      return std::make_tuple(0, std::move(*v));
    } else {
      return std::make_tuple(-1, std::string(""));
    }
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::read_meta"
    }
  );
}

uuid_d SeaStore::Shard::get_fsid() const
{
  return device->get_meta().seastore_id;
}

void SeaStore::Shard::init_managers()
{
  transaction_manager.reset();
  collection_manager.reset();
  onode_manager.reset();

  transaction_manager = make_transaction_manager(
      device, secondaries, is_test);
  collection_manager = std::make_unique<collection_manager::FlatCollectionManager>(
      *transaction_manager);
  onode_manager = std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(
      *transaction_manager);
}

device_stats_t SeaStore::Shard::get_device_stats(bool report_detail) const
{
  return transaction_manager->get_device_stats(report_detail);
}

std::unique_ptr<SeaStore> make_seastore(
  const std::string &device)
{
  auto mdstore = std::make_unique<FileMDStore>(device);
  return std::make_unique<SeaStore>(
    device,
    std::move(mdstore));
}

std::unique_ptr<SeaStore> make_test_seastore(
  SeaStore::MDStoreRef mdstore)
{
  return std::make_unique<SeaStore>(
    "",
    std::move(mdstore));
}

}
