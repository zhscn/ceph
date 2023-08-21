// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

SET_SUBSYS(seastore_onode);

namespace crimson::os::seastore::onode {

FLTreeOnodeManager::contains_onode_ret FLTreeOnodeManager::contains_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  return tree.contains(trans, hoid);
}

FLTreeOnodeManager::get_onode_ret FLTreeOnodeManager::get_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  LOG_PREFIX(FLTreeOnodeManager::get_onode);
  return tree.find(
    trans, hoid
  ).si_then([this, &hoid, &trans, FNAME](auto cursor)
              -> get_onode_ret {
    if (cursor == tree.end()) {
      DEBUGT("no entry for {}", trans, hoid);
      return crimson::ct_error::enoent::make();
    }
    auto val = OnodeRef(new FLTreeOnode(
	default_data_reservation,
	default_metadata_range,
	cursor.value()));
    return get_onode_iertr::make_ready_future<OnodeRef>(
      val
    );
  });
}

FLTreeOnodeManager::get_history_ret FLTreeOnodeManager::get_history(
  Transaction &trans,
  const ghobject_t &hoid)
{
  struct state_t {
    const ghobject_t &hoid;
    ghobject_t head;
    ghobject_t start_hoid;
    ghobject_t history_start;

    onode_history_t res;

    state_t(const ghobject_t &hoid)
      : hoid(hoid), head(hoid), start_hoid(hoid),
        history_start(hoid) {
      head.hobj.snap = CEPH_NOSNAP;
      if (hoid.hobj.is_head()) {
        start_hoid.hobj.snap = CEPH_MAXSNAP + 1;
      } else {
        history_start.hobj.snap = CEPH_MAXSNAP - hoid.hobj.snap;
      }
    }

    bool is_head() const {
      return hoid.hobj.is_head();
    }

    void push(const ghobject_t &obj, OnodeTree::Cursor &cursor,
              uint32_t default_data_reservation,
              uint32_t default_metadata_range) {
      auto onode = new FLTreeOnode(
        default_data_reservation,
        default_metadata_range,
        cursor.value());
      if (obj == hoid) {
        res.onode = onode;
      } else {
        auto obj_data = onode->get_layout().object_data.get();
        res.data_onodes.emplace(obj_data.get_reserved_data_base(), onode);
      }
    }
  };

  state_t s(hoid);
  LOG_PREFIX(FLTreeOnodeManager::get_history);
  DEBUGT("hoid: {}, start_hoid: {}", trans, hoid, s.start_hoid);
  return tree.lower_bound(trans, s.start_hoid
  ).si_then([this, &trans, s=std::move(s)](OnodeTree::Cursor cursor) {
    return seastar::do_with(
      cursor,
      std::move(s),
      [this, &trans](OnodeTree::Cursor &cursor, state_t &s) mutable {
        return trans_intr::repeat([this, &trans, &cursor, &s]() mutable {
          LOG_PREFIX(FLTreeOnodeManager::get_history);
          auto obj = ghobject_t();
          if (cursor == tree.end()) {
            DEBUGT("reach tree end. hoid: {} head: {} start_hoid: {}, "
		   "history_start: {} res length: {}",
                   trans, s.hoid, s.head, s.start_hoid, s.history_start,
		   s.res.data_onodes.size());
            return get_onode_iertr::make_ready_future<
              seastar::stop_iteration>(seastar::stop_iteration::yes);
          } else {
            obj = cursor.get_ghobj();
            if ((s.is_head() && obj > s.head) ||
                (!s.is_head() && obj >= s.head)) {
              DEBUGT("cursor: {}. hoid: {} head: {} start_hoid: {}, "
		     "history_start: {} res length: {}",
                     trans, obj, s.hoid, s.head, s.start_hoid,
                     s.history_start, s.res.data_onodes.size());
              return get_onode_iertr::make_ready_future<
                seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
          }

          DEBUGT("hoid: {}", trans, obj);
          if (s.is_head() ||
              obj == s.hoid ||
              obj >= s.history_start) {
            s.push(obj, cursor,
	      default_data_reservation,
	      default_metadata_range);
          } else {
            DEBUGT("skip hoid: {}", trans, obj);
          }

          return cursor.get_next(trans).si_then([&cursor](auto n) mutable {
            cursor = n;
            return get_onode_iertr::make_ready_future<
              seastar::stop_iteration>(seastar::stop_iteration::no);
          });
        }).si_then([&s, &trans]() mutable -> get_history_ret {
          LOG_PREFIX(FLTreeOnodeManager::get_history);
          if (!s.res.onode) {
            ERRORT("not found", trans);
            return crimson::ct_error::enoent::make();
          } else {
            DEBUGT("find {} history", trans, s.res.data_onodes.size());
            return get_onode_iertr::make_ready_future<
              onode_history_t>(std::move(s.res));
          }
        });
    });
  });
}

FLTreeOnodeManager::get_or_create_onode_ret
FLTreeOnodeManager::get_or_create_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  LOG_PREFIX(FLTreeOnodeManager::get_or_create_onode);
  return tree.insert(
    trans, hoid,
    OnodeTree::tree_value_config_t{sizeof(onode_layout_t)}
  ).si_then([this, &trans, &hoid, FNAME](auto p)
              -> get_or_create_onode_ret {
    auto [cursor, created] = std::move(p);
    auto val = OnodeRef(new FLTreeOnode(
	default_data_reservation,
	default_metadata_range,
	cursor.value()));
    if (created) {
      DEBUGT("created onode for entry for {}", trans, hoid);
      val->get_mutable_layout(trans) = onode_layout_t{};
    }
    return get_or_create_onode_iertr::make_ready_future<OnodeRef>(
      val
    );
  });
}

FLTreeOnodeManager::get_or_create_onodes_ret
FLTreeOnodeManager::get_or_create_onodes(
  Transaction &trans,
  const std::vector<ghobject_t> &hoids)
{
  return seastar::do_with(
    std::vector<OnodeRef>(),
    [this, &hoids, &trans](auto &ret) {
      ret.reserve(hoids.size());
      return trans_intr::do_for_each(
        hoids,
        [this, &trans, &ret](auto &hoid) {
          return get_or_create_onode(trans, hoid
          ).si_then([&ret](auto &&onoderef) {
            ret.push_back(std::move(onoderef));
          });
        }).si_then([&ret] {
          return std::move(ret);
        });
    });
}

FLTreeOnodeManager::write_dirty_ret FLTreeOnodeManager::write_dirty(
  Transaction &trans,
  const std::vector<OnodeRef> &onodes)
{
  return trans_intr::do_for_each(
    onodes,
    [&trans, this](auto &onode) -> eagain_ifuture<> {
      if (!onode) {
	return eagain_iertr::make_ready_future<>();
      }
      auto &flonode = static_cast<FLTreeOnode&>(*onode);
      if (!flonode.is_alive()) {
	return eagain_iertr::make_ready_future<>();
      }
      switch (flonode.status) {
      case FLTreeOnode::status_t::MUTATED: {
        flonode.populate_recorder(trans);
        const auto &obj_data = flonode.get_layout().object_data.get();
        LOG_PREFIX(FLTreeOnodeManager::write_dirty);
        DEBUGT("onode_base={}, extents_count={}",
               trans, obj_data.get_reserved_data_base(),
               obj_data.get_extents_count());
        if (obj_data.get_extents_count() == 0) {
          trans.update_onode_info(
            obj_data.get_reserved_data_base(),
            obj_data.get_reserved_data_len(),
            extent_types_t::OBJECT_DATA_BLOCK,
            Transaction::onode_op_t::REMOVE_REGION);
          return tree.erase(trans, flonode);
        } else {
          assert(obj_data.get_extents_count() == -1 ||
                 obj_data.get_extents_count() > 0);
          return eagain_iertr::make_ready_future<>();
        }
      }
      case FLTreeOnode::status_t::STABLE: {
        return eagain_iertr::make_ready_future<>();
      }
      default:
        __builtin_unreachable();
      }
    });
}

FLTreeOnodeManager::erase_onode_ret FLTreeOnodeManager::erase_onode(
  Transaction &trans,
  OnodeRef &onode)
{
  auto &flonode = static_cast<FLTreeOnode&>(*onode);
  flonode.mark_delete();
  return tree.erase(trans, flonode);
}

FLTreeOnodeManager::list_onodes_ret FLTreeOnodeManager::list_onodes(
  Transaction &trans,
  const ghobject_t& start,
  const ghobject_t& end,
  uint64_t limit)
{
  LOG_PREFIX(FLTreeOnodeManager::list_onodes);
  DEBUGT("start {}, end {}, limit {}", trans, start, end, limit);
  return tree.lower_bound(trans, start
  ).si_then([this, &trans, end, limit] (auto&& cursor) {
    using crimson::os::seastore::onode::full_key_t;
    return seastar::do_with(
        limit,
        std::move(cursor),
        list_onodes_bare_ret(),
        [this, &trans, end] (auto& to_list, auto& current_cursor, auto& ret) {
      return trans_intr::repeat(
          [this, &trans, end, &to_list, &current_cursor, &ret] ()
          -> eagain_ifuture<seastar::stop_iteration> {
	LOG_PREFIX(FLTreeOnodeManager::list_onodes);
        if (current_cursor.is_end()) {
	  DEBUGT("reached the onode tree end", trans);
          std::get<1>(ret) = ghobject_t::get_max();
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        } else if (current_cursor.get_ghobj() >= end) {
	  DEBUGT("reached the end {} > {}",
	    trans, current_cursor.get_ghobj(), end);
          std::get<1>(ret) = end;
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        if (to_list == 0) {
	  DEBUGT("reached the limit", trans);
          std::get<1>(ret) = current_cursor.get_ghobj();
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        ghobject_t ghobj = current_cursor.get_ghobj();
        bool consume = ghobj.hobj.snap <= CEPH_MAXSNAP ||
          ghobj.hobj.snap >= CEPH_NOSNAP;
        if (consume) {
	  DEBUGT("found onode for {}", trans, ghobj);
          std::get<0>(ret).emplace_back(std::move(ghobj));
        }
        return tree.get_next(trans, current_cursor
        ).si_then([&to_list, &current_cursor, consume] (auto&& next_cursor) mutable {
          // we intentionally hold the current_cursor during get_next() to
          // accelerate tree lookup.
          if (consume) {
            --to_list;
          }
          current_cursor = next_cursor;
          return seastar::make_ready_future<seastar::stop_iteration>(
	        seastar::stop_iteration::no);
        });
      }).si_then([&ret] () mutable {
        return seastar::make_ready_future<list_onodes_bare_ret>(
            std::move(ret));
       //  return ret;
      });
    });
  });
}

FLTreeOnodeManager::scan_onodes_ret FLTreeOnodeManager::scan_onodes(
  Transaction &trans,
  scan_onodes_func_t &&func)
{
  return tree.lower_bound(trans, ghobject_t()
  ).si_then([this, &trans, func=std::move(func)](auto &&cursor) mutable {
    return seastar::do_with(std::move(cursor),
                            std::vector<OnodeTree::Cursor>(),
                            std::move(func),
                            [this, &trans] (auto &cursor,
                                            auto &cursors,
                                            auto &func) {
      return trans_intr::repeat([this, &trans, &cursor, &cursors, &func] {
        cursors.clear();
        return trans_intr::do_for_each(boost::make_counting_iterator(0),
                                       boost::make_counting_iterator(10),
                                       [this, &trans, &cursor, &cursors](auto) {
          if (!cursor.is_end()) {
            cursors.push_back(cursor);
            return tree.get_next(trans, cursor
            ).si_then([&cursor](auto &&next) {
              cursor = next;
              return eagain_iertr::make_ready_future();
            });
          } else {
            return eagain_iertr::make_ready_future();
          }
        }).si_then([this, &cursors, &func] {
          return trans_intr::parallel_for_each(cursors, [this, &func](auto &cursor) {
            return func(FLTreeOnode(
              default_data_reservation,
              default_metadata_range,
              cursor.value()));
          });
        }).si_then([&cursor] {
          if (cursor.is_end()) {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          } else {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
          }
        });
      });
    });
  });
}

FLTreeOnodeManager::~FLTreeOnodeManager() {}

}
