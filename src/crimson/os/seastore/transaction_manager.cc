// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "include/denc.h"
#include "include/intarith.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/object_data_handler.h"

/*
 * TransactionManager logs
 *
 * levels:
 * - INFO: major initiation, closing operations
 * - DEBUG: major extent related operations, INFO details
 * - TRACE: DEBUG details
 * - seastore_t logs
 */
SET_SUBSYS(seastore_tm);

namespace crimson::os::seastore {

TransactionManager::TransactionManager(
  JournalRef _journal,
  CacheRef _cache,
  LBAManagerRef _lba_manager,
  ExtentPlacementManagerRef &&_epm,
  BackrefManagerRef&& _backref_manager)
  : cache(std::move(_cache)),
    lba_manager(std::move(_lba_manager)),
    journal(std::move(_journal)),
    epm(std::move(_epm)),
    backref_manager(std::move(_backref_manager)),
    nv_cache(nullptr)
{
  epm->set_extent_callback(this);
  journal->set_write_pipeline(&write_pipeline);
  if (epm->has_cold_tier()) {
    nv_cache = epm->get_non_volatile_cache();
    ceph_assert(support_non_volatile_cache());
  }
}

TransactionManager::mkfs_ertr::future<> TransactionManager::mkfs()
{
  LOG_PREFIX(TransactionManager::mkfs);
  INFO("enter");
  return epm->mount(
  ).safe_then([this] {
    return journal->open_for_mkfs();
  }).safe_then([this](auto start_seq) {
    journal->get_trimmer().update_journal_tails(start_seq, start_seq);
    journal->get_trimmer().set_journal_head(start_seq);
    return epm->open_for_write();
  }).safe_then([this, FNAME]() {
    return with_transaction_intr(
      Transaction::src_t::MUTATE,
      "mkfs_tm",
      [this, FNAME](auto& t)
    {
      cache->init();
      return cache->mkfs(t
      ).si_then([this, &t] {
        return lba_manager->mkfs(t);
      }).si_then([this, &t] {
        return backref_manager->mkfs(t);
      }).si_then([this, FNAME, &t] {
        INFOT("submitting mkfs transaction", t);
        return submit_transaction_direct(t);
      });
    }).handle_error(
      crimson::ct_error::eagain::handle([] {
        ceph_assert(0 == "eagain impossible");
        return mkfs_ertr::now();
      }),
      mkfs_ertr::pass_further{}
    );
  }).safe_then([this] {
    return close();
  }).safe_then([FNAME] {
    INFO("completed");
  });
}

TransactionManager::mount_ertr::future<> TransactionManager::mount()
{
  LOG_PREFIX(TransactionManager::mount);
  INFO("enter");
  cache->init();
  return epm->mount(
  ).safe_then([this] {
    return journal->replay(
      [this](
	const auto &offsets,
	const auto &e,
	const journal_seq_t &dirty_tail,
	const journal_seq_t &alloc_tail,
	sea_time_point modify_time)
      {
	auto start_seq = offsets.write_result.start_seq;
	return cache->replay_delta(
	  start_seq,
	  offsets.record_block_base,
	  e,
	  dirty_tail,
	  alloc_tail,
	  modify_time);
      });
  }).safe_then([this] {
    return journal->open_for_mount();
  }).safe_then([this](auto start_seq) {
    journal->get_trimmer().set_journal_head(start_seq);
    return with_transaction_weak(
      "mount",
      [this](auto &t)
    {
      return cache->init_cached_extents(t, [this](auto &t, auto &e) {
        if (is_backref_node(e->get_type())) {
          return backref_manager->init_cached_extent(t, e);
        } else {
          return lba_manager->init_cached_extent(t, e);
        }
      }).si_then([this, &t] {
        epm->start_scan_space();
        return backref_manager->scan_mapped_space(
          t,
          [this](
            paddr_t paddr,
	    paddr_t backref_key,
            extent_len_t len,
            extent_types_t type,
            laddr_t laddr) {
          if (is_backref_node(type)) {
            assert(laddr == L_ADDR_NULL);
	    assert(backref_key != P_ADDR_NULL);
            backref_manager->cache_new_backref_extent(paddr, backref_key, type);
            cache->update_tree_extents_num(type, 1);
            epm->mark_space_used(paddr, len);
          } else if (laddr == L_ADDR_NULL) {
	    assert(backref_key == P_ADDR_NULL);
            cache->update_tree_extents_num(type, -1);
            epm->mark_space_free(paddr, len);
          } else {
	    assert(backref_key == P_ADDR_NULL);
            cache->update_tree_extents_num(type, 1);
            epm->mark_space_used(paddr, len);
          }
        });
      });
    });
  }).safe_then([this] {
    return epm->open_for_write();
  }).safe_then([FNAME] {
    INFO("completed");
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::all_same_way([] {
      ceph_assert(0 == "unhandled error");
      return mount_ertr::now();
    })
  );
}

TransactionManager::close_ertr::future<> TransactionManager::close() {
  LOG_PREFIX(TransactionManager::close);
  INFO("enter");
  return epm->stop_background(
  ).then([this] {
    return cache->close();
  }).safe_then([this] {
    cache->dump_contents();
    return journal->close();
  }).safe_then([this] {
    return epm->close();
  }).safe_then([FNAME] {
    INFO("completed");
    return seastar::now();
  });
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  LOG_PREFIX(TransactionManager::inc_ref);
  TRACET("{}", t, *ref);
  return lba_manager->incref_extent(t, ref->get_laddr()
  ).si_then([FNAME, ref, &t](auto result) {
    DEBUGT("extent refcount is incremented to {} -- {}",
           t, result.refcount, *ref);
    return result.refcount;
  }).handle_error_interruptible(
    ref_iertr::pass_further{},
    ct_error::all_same_way([](auto e) {
      ceph_assert(0 == "unhandled error, TODO");
    }));
}

TransactionManager::ref_ret TransactionManager::inc_ref(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(TransactionManager::inc_ref);
  TRACET("{}", t, offset);
  return lba_manager->incref_extent(t, offset
  ).si_then([FNAME, offset, &t](auto result) {
    DEBUGT("extent refcount is incremented to {} -- {}~{}, {}",
           t, result.refcount, offset, result.length, result.addr);
    return result.refcount;
  });
}

TransactionManager::dec_ret TransactionManager::dec_ref(
  Transaction &t,
  LogicalCachedExtentRef &ref)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  TRACET("{}", t, *ref);
  return lba_manager->decref_extent(t, ref->get_laddr(), true
  ).si_then([this, FNAME, &t, ref](auto result) {
    DEBUGT("extent refcount is decremented to {} -- {}",
           t, result.refcount, *ref);
    if (result.refcount == 0) {
      cache->retire_extent(t, ref);
      if (result.shadow_addr != P_ADDR_NULL) {
	return cache->retire_extent_addr(
          t, result.shadow_addr, result.length
        ).si_then([result] {
          return ref_iertr::make_ready_future<
	    dec_res_t>(result.refcount, result.shadow_addr);
        });
      }
    }
    return ref_iertr::make_ready_future<
      dec_res_t>(result.refcount, P_ADDR_NULL);
  });
}

TransactionManager::dec_ret TransactionManager::dec_ref(
  Transaction &t,
  laddr_t offset,
  bool cascade_remove)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  TRACET("{}", t, offset);
  return lba_manager->decref_extent(t, offset, cascade_remove
  ).si_then([this, FNAME, offset, &t](auto result) {
    DEBUGT("extent refcount is decremented to {} -- {}~{}, {} shadow addr {}",
           t, result.refcount, offset, result.length,
           result.addr, result.shadow_addr);
    auto fut = ref_iertr::now();
    if (result.refcount == 0) {
      if (result.addr.is_paddr() &&
          !result.addr.get_paddr().is_zero()) {
        fut = cache->retire_extent_addr(
          t, result.addr.get_paddr(), result.length
        ).si_then([this, result=std::move(result), &t] {
          if (result.shadow_addr != P_ADDR_NULL) {
            return cache->retire_extent_addr(
              t, result.shadow_addr, result.length);
          } else {
            return Cache::retire_extent_iertr::now();
          }
        });
      } else if (result.removed_intermediate_mappings){
        auto &removed = *result.removed_intermediate_mappings;
        assert(!removed.paddr.is_zero());
        fut = cache->retire_extent_addr(
          t, removed.paddr, removed.len
        ).si_then([this, removed=std::move(removed), &t] {
          if (removed.shadow_addr != P_ADDR_NULL) {
            return cache->retire_extent_addr(
              t, removed.shadow_addr, removed.len);
          } else {
            return Cache::retire_extent_iertr::now();
          }
        });
      }
    }

    return fut.si_then([result=std::move(result)] {
      return dec_res_t(result.refcount, result.shadow_addr);
    });
  });
}

TransactionManager::decs_ret TransactionManager::dec_ref(
  Transaction &t,
  std::vector<laddr_t> offsets)
{
  LOG_PREFIX(TransactionManager::dec_ref);
  DEBUG("{} offsets", offsets.size());
  return seastar::do_with(std::move(offsets), std::vector<dec_res_t>(),
      [this, &t] (auto &&offsets, auto &refcnt) {
      return trans_intr::do_for_each(offsets.begin(), offsets.end(),
        [this, &t, &refcnt] (auto &laddr) {
        return this->dec_ref(t, laddr).si_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
          return ref_iertr::now();
        });
      }).si_then([&refcnt] {
        return ref_iertr::make_ready_future<
	  std::vector<dec_res_t>>(std::move(refcnt));
      });
    });
}

TransactionManager::submit_transaction_iertr::future<>
TransactionManager::submit_transaction(
  Transaction &t)
{
  LOG_PREFIX(TransactionManager::submit_transaction);
  SUBTRACET(seastore_t, "start", t);
  return trans_intr::make_interruptible(
    t.get_handle().enter(write_pipeline.reserve_projected_usage)
  ).then_interruptible([this, FNAME, &t] {
    auto dispatch_result = epm->dispatch_delayed_extents(t);
    auto projected_usage = dispatch_result.usage;
    SUBTRACET(seastore_t, "waiting for projected_usage: {}", t, projected_usage);
    return trans_intr::make_interruptible(
      epm->reserve_projected_usage(projected_usage)
    ).then_interruptible([this, &t, dispatch_result = std::move(dispatch_result)] {
      return do_submit_transaction(t, std::move(dispatch_result));
    }).finally([this, FNAME, projected_usage, &t] {
      SUBTRACET(seastore_t, "releasing projected_usage: {}", t, projected_usage);
      epm->release_projected_usage(projected_usage);
    });
  });
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::submit_transaction_direct(
  Transaction &tref,
  std::optional<journal_seq_t> trim_alloc_to)
{
  return do_submit_transaction(
    tref,
    epm->dispatch_delayed_extents(tref),
    trim_alloc_to);
}

TransactionManager::submit_transaction_direct_ret
TransactionManager::do_submit_transaction(
  Transaction &tref,
  ExtentPlacementManager::dispatch_result_t dispatch_result,
  std::optional<journal_seq_t> trim_alloc_to)
{
  LOG_PREFIX(TransactionManager::do_submit_transaction);
  SUBTRACET(seastore_t, "start", tref);
  return trans_intr::make_interruptible(
    tref.get_handle().enter(write_pipeline.ool_writes)
  ).then_interruptible([this, FNAME, &tref,
			dispatch_result = std::move(dispatch_result)] {
    return seastar::do_with(std::move(dispatch_result),
			    [this, FNAME, &tref](auto &dispatch_result) {
      return epm->write_delayed_ool_extents(tref, dispatch_result.alloc_map
      ).si_then([this, FNAME, &tref, &dispatch_result] {
        SUBTRACET(seastore_t, "update delayed extent mappings", tref);
        return lba_manager->update_mappings(tref, dispatch_result.delayed_extents);
      }).handle_error_interruptible(
        crimson::ct_error::input_output_error::pass_further(),
        crimson::ct_error::assert_all("invalid error")
      );
    });
  }).si_then([this, FNAME, &tref] {
    auto allocated_extents = tref.get_valid_pre_alloc_list();
    auto num_extents = allocated_extents.size();
    SUBTRACET(seastore_t, "process {} allocated extents", tref, num_extents);
    return epm->write_preallocated_ool_extents(tref, allocated_extents
    ).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further(),
      crimson::ct_error::assert_all("invalid error")
    );
  }).si_then([this, FNAME, &tref] {
    SUBTRACET(seastore_t, "about to prepare", tref);
    return tref.get_handle().enter(write_pipeline.prepare);
  }).si_then([this, FNAME, &tref, trim_alloc_to=std::move(trim_alloc_to)]() mutable
	      -> submit_transaction_iertr::future<> {
    if (trim_alloc_to && *trim_alloc_to != JOURNAL_SEQ_NULL) {
      cache->trim_backref_bufs(*trim_alloc_to);
    }

    auto record = cache->prepare_record(
      tref,
      journal->get_trimmer().get_journal_head(),
      journal->get_trimmer().get_dirty_tail());

    tref.get_handle().maybe_release_collection_lock();

    SUBTRACET(seastore_t, "about to submit to journal", tref);
    return journal->submit_record(std::move(record), tref.get_handle()
    ).safe_then([this, FNAME, &tref](auto submit_result) mutable {
      SUBDEBUGT(seastore_t, "committed with {}", tref, submit_result);
      auto start_seq = submit_result.write_result.start_seq;
      journal->get_trimmer().set_journal_head(start_seq);
      cache->complete_commit(
          tref,
          submit_result.record_block_base,
          start_seq);

      if (nv_cache) {
	for (auto &info : tref.get_onode_info()) {
	  if (info.op == Transaction::onode_op_t::PROMOTE) {
	    nv_cache->move_to_top_if_not_cached(info.laddr, info.length, info.type);
	  } else if (info.op == Transaction::onode_op_t::REMOVE) {
	    nv_cache->remove(info.laddr, info.length, info.type);
	  }
	}
      }

      std::vector<CachedExtentRef> lba_to_clear;
      std::vector<CachedExtentRef> backref_to_clear;
      lba_to_clear.reserve(tref.get_retired_set().size());
      backref_to_clear.reserve(tref.get_retired_set().size());
      for (auto &e: tref.get_retired_set()) {
	if (e->is_logical() || is_lba_node(e->get_type()))
	  lba_to_clear.push_back(e);
	else if (is_backref_node(e->get_type()))
	  backref_to_clear.push_back(e);
      }

      journal->get_trimmer().update_journal_tails(
	cache->get_oldest_dirty_from().value_or(start_seq),
	cache->get_oldest_backref_dirty_from().value_or(start_seq));
      return journal->finish_commit(tref.get_src()
      ).then([&tref] {
	return tref.get_handle().complete();
      });
    }).handle_error(
      submit_transaction_iertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      })
    );
  }).finally([&tref]() {
      tref.get_handle().exit();
  });
}

seastar::future<> TransactionManager::flush(OrderingHandle &handle)
{
  LOG_PREFIX(TransactionManager::flush);
  SUBDEBUG(seastore_t, "H{} start", (void*)&handle);
  return handle.enter(write_pipeline.reserve_projected_usage
  ).then([this, &handle] {
    return handle.enter(write_pipeline.ool_writes);
  }).then([this, &handle] {
    return handle.enter(write_pipeline.prepare);
  }).then([this, &handle] {
    handle.maybe_release_collection_lock();
    return journal->flush(handle);
  }).then([FNAME, &handle] {
    SUBDEBUG(seastore_t, "H{} completed", (void*)&handle);
  });
}

TransactionManager::get_next_dirty_extents_ret
TransactionManager::get_next_dirty_extents(
  Transaction &t,
  journal_seq_t seq,
  size_t max_bytes)
{
  LOG_PREFIX(TransactionManager::get_next_dirty_extents);
  DEBUGT("max_bytes={}B, seq={}", t, max_bytes, seq);
  return cache->get_next_dirty_extents(t, seq, max_bytes);
}

TransactionManager::rewrite_extent_ret
TransactionManager::rewrite_logical_extent(
  Transaction& t,
  LogicalCachedExtentRef extent)
{
  LOG_PREFIX(TransactionManager::rewrite_logical_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("extent has been invalidated -- {}", t, *extent);
    ceph_abort();
  }
  TRACET("rewriting extent -- {}", t, *extent);

  auto lextent = extent->cast<LogicalCachedExtent>();
  bool is_tracked =
    support_non_volatile_cache() &&
    // lextent is from hot tier
    !epm->is_cold_device(lextent->get_paddr().get_device_id()) &&
    // lextent will be evicted to the cold tier
    epm->is_going_to_evict(*lextent) &&
    // lextent is cached by non volatile cache
    nv_cache->is_cached(
      lextent->get_laddr(),
      lextent->get_length(),
      lextent->get_type());

  cache->retire_extent(t, extent);
  auto nlextent = cache->alloc_new_extent_by_type(
    t,
    lextent->get_type(),
    lextent->get_length(),
    lextent->get_user_hint(),
    // get target rewrite generation
    lextent->get_rewrite_generation(),
    is_tracked)->cast<LogicalCachedExtent>();
  lextent->get_bptr().copy_out(
    0,
    lextent->get_length(),
    nlextent->get_bptr().c_str());
  nlextent->set_laddr(lextent->get_laddr());
  nlextent->set_modify_time(lextent->get_modify_time());

  DEBUGT("rewriting logical extent -- {} to {}", t, *lextent, *nlextent);

  /* This update_mapping is, strictly speaking, unnecessary for delayed_alloc
   * extents since we're going to do it again once we either do the ool write
   * or allocate a relative inline addr.  TODO: refactor AsyncCleaner to
   * avoid this complication. */
  return lba_manager->update_mapping(
    t,
    lextent->get_laddr(),
    lextent->get_paddr(),
    nlextent->get_paddr(),
    nlextent.get());
}

TransactionManager::maybe_load_onode_ret
TransactionManager::maybe_load_onode(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length,
  extent_types_t type)
{
  return lba_manager->get_mappings_with_shadow(t, laddr, length
  ).si_then([this, laddr, length, type, &t](auto pin_list) {
    LOG_PREFIX(TransactionManager::maybe_load_onode);
    for (auto &pin : pin_list) {
      TRACET("found extent: {}", t, pin->get_key());
      if (pin->is_shadow_mapping()) {
        TRACET("add onode to cache: {}", t, laddr);
	nv_cache->move_to_top(laddr, length, type);
        return seastar::now();
      }
    }
    return seastar::now();
  });
}

TransactionManager::rewrite_extent_ret TransactionManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent,
  rewrite_gen_t target_generation,
  sea_time_point modify_time)
{
  LOG_PREFIX(TransactionManager::rewrite_extent);

  {
    auto updated = cache->update_extent_from_transaction(t, extent);
    if (!updated) {
      DEBUGT("extent is already retired, skipping -- {}", t, *extent);
      return rewrite_extent_iertr::now();
    }
    extent = updated;
    ceph_assert(!extent->is_pending_io());
  }

  assert(extent->is_valid() && !extent->is_initial_pending());
  if (extent->is_dirty()) {
    extent->set_target_rewrite_generation(INIT_GENERATION);
  } else {
    extent->set_target_rewrite_generation(target_generation);
    ceph_assert(modify_time != NULL_TIME);
    extent->set_modify_time(modify_time);
  }

  t.get_rewrite_version_stats().increment(extent->get_version());

  if (is_backref_node(extent->get_type())) {
    DEBUGT("rewriting backref extent -- {}", t, *extent);
    return backref_manager->rewrite_extent(t, extent);
  }

  if (extent->get_type() == extent_types_t::ROOT) {
    DEBUGT("rewriting root extent -- {}", t, *extent);
    cache->duplicate_for_write(t, extent);
    return rewrite_extent_iertr::now();
  }

  if (extent->is_logical()) {
    return rewrite_logical_extent(t, extent->cast<LogicalCachedExtent>());
  } else {
    DEBUGT("rewriting physical extent -- {}", t, *extent);
    return lba_manager->rewrite_extent(t, extent);
  }
}

bool support_shadow_extent(const CachedExtent &extent) {
  return extent.get_type() == extent_types_t::OBJECT_DATA_BLOCK;
}

TransactionManager::promote_extent_ret
TransactionManager::promote_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  LOG_PREFIX(TransactionManager::promote_extent);
  assert(epm->get_main_backend_type() == backend_type_t::SEGMENTED);
  assert(epm->is_cold_device(extent->get_paddr().get_device_id()));

  DEBUGT("promote extent: {}", t, *extent);

  if (!support_shadow_extent(*extent)) {
    auto mtime = extent->get_modify_time();
    if (mtime == NULL_TIME) {
      mtime = seastar::lowres_system_clock::now(); // XXX: is it appropriate?
    }
    return rewrite_extent(
      t,
      extent,
      INIT_GENERATION,
      mtime);
  }
  assert(extent->is_logical());

  t.get_rewrite_version_stats().increment(extent->get_version());
  cache->retire_extent(t, extent);

  auto lextent = extent->cast<ObjectDataBlock>();
  auto cold_ext = cache->alloc_remapped_extent_by_type(
    t,
    extent->get_type(),
    lextent->get_laddr(),
    extent->get_paddr(),
    extent->get_length(),
    lextent->get_laddr(),
    extent->get_bptr())->cast<LogicalCachedExtent>();
  cold_ext->set_modify_time(extent->get_modify_time());

  assert(lextent->onode_info);

  t.update_onode_info(
    lextent->onode_info->onode_base,
    lextent->onode_info->onode_length,
    lextent->get_type(),
    Transaction::onode_op_t::PROMOTE);

  return lba_manager->alloc_shadow_extent(
    t,
    lextent->get_laddr(),
    lextent->get_length(),
    cold_ext->get_paddr(),
    cold_ext.get()
  ).si_then([lextent, cold_ext, &t, this, FNAME](auto mapping) {
    ceph_assert(mapping->is_shadow_mapping());
    DEBUGT("alloc_shadow_extent mapping: {}", t, *mapping);

    cold_ext->set_laddr(mapping->get_key());

    auto promote_ext = cache->alloc_new_extent_by_type(
      t,
      lextent->get_type(),
      lextent->get_length(),
      placement_hint_t::HOT,
      INIT_GENERATION,
      true)->cast<LogicalCachedExtent>();
    lextent->get_bptr().copy_out(
      0,
      lextent->get_length(),
      promote_ext->get_bptr().c_str());
    promote_ext->set_laddr(lextent->get_laddr());
    promote_ext->set_modify_time(lextent->get_modify_time());

    return lba_manager->update_mapping(
      t,
      lextent->get_laddr(),
      lextent->get_paddr(),
      promote_ext->get_paddr(),
      promote_ext.get());
  });
}

TransactionManager::demote_region_ret
TransactionManager::demote_region(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length,
  extent_len_t max_demote_size)
{
  return lba_manager->demote_region(
    t,
    laddr,
    length,
    max_demote_size,
    [this, &t](paddr_t paddr, extent_len_t length) {
      assert(!epm->is_cold_device(paddr.get_device_id()));
      return cache->retire_extent_addr(t, paddr, length);
    },
    [this, &t](LogicalCachedExtent *extent,
	       paddr_t paddr,
	       extent_len_t length)
    -> base_iertr::future<LogicalCachedExtent*> {
      assert(epm->is_cold_device(paddr.get_device_id()));
      if (!extent) {
	return cache->retire_extent_addr(t, paddr, length
	).si_then([&t, paddr, length, this] {
	  auto nextent = cache->alloc_remapped_extent<ObjectDataBlock>(
	    t,
	    L_ADDR_NULL,
	    paddr,
	    length,
	    L_ADDR_NULL,
	    std::nullopt)->cast<LogicalCachedExtent>();
	  return base_iertr::make_ready_future<
	    LogicalCachedExtent*>(nextent.get());
	});
      }
      cache->retire_extent(t, extent);
      auto nextent = cache->alloc_remapped_extent_by_type(
	  t,
	  extent->get_type(),
	  extent->get_laddr(),
	  extent->get_paddr(),
	  extent->get_length(),
	  extent->get_laddr(),
	  extent->get_bptr())->cast<LogicalCachedExtent>();
      nextent->set_laddr(extent->get_laddr());
      return base_iertr::make_ready_future<
	LogicalCachedExtent*>(nextent.get());
    }
  ).si_then([](auto &&res) {
    return demote_region_res_t{
      res.proceed_size,
      res.demote_size,
      res.completed
    };
  });
}

TransactionManager::get_extents_if_live_ret
TransactionManager::get_extents_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t paddr,
  laddr_t laddr,
  extent_len_t len)
{
  LOG_PREFIX(TransactionManager::get_extent_if_live);
  TRACET("{} {}~{} {}", t, type, laddr, len, paddr);

  // This only works with segments to check if alive,
  // as parallel transactions may split the extent at the same time.
  ceph_assert(paddr.get_addr_type() == paddr_types_t::SEGMENT);

  return cache->get_extent_if_cached(t, paddr, type
  ).si_then([=, this, &t](auto extent)
	    -> get_extents_if_live_ret {
    if (extent && extent->get_length() == len) {
      DEBUGT("{} {}~{} {} is live in cache -- {}",
             t, type, laddr, len, paddr, *extent);
      std::list<CachedExtentRef> res;
      res.emplace_back(std::move(extent));
      return get_extents_if_live_ret(
	interruptible::ready_future_marker{},
	res);
    }

    if (is_logical_type(type)) {
      return lba_manager->get_mappings_with_shadow(
	t,
	laddr,
	len
      ).si_then([=, this, &t](lba_pin_list_t pin_list) {
	return seastar::do_with(
	  std::list<CachedExtentRef>(),
	  [=, this, &t, pin_list=std::move(pin_list)](
            std::list<CachedExtentRef> &list) mutable
        {
          auto paddr_seg_id = paddr.as_seg_paddr().get_segment_id();
          return trans_intr::parallel_for_each(
            pin_list,
            [=, this, &list, &t](
              LBAMappingRef &pin) -> Cache::get_extent_iertr::future<>
          {
            auto pin_paddr = pin->get_val();
            auto &pin_seg_paddr = pin_paddr.as_seg_paddr();
            auto pin_paddr_seg_id = pin_seg_paddr.get_segment_id();
            auto pin_len = pin->get_length();
            if (pin_paddr_seg_id != paddr_seg_id) {
              return seastar::now();
            }
            // Only extent split can happen during the lookup
            ceph_assert(pin_seg_paddr >= paddr &&
                        pin_seg_paddr.add_offset(pin_len) <= paddr.add_offset(len));
            return read_pin_by_type(t, std::move(pin), type
            ).si_then([&list](auto ret) {
              list.emplace_back(std::move(ret));
              return seastar::now();
            });
          }).si_then([&list] {
            return get_extents_if_live_ret(
              interruptible::ready_future_marker{},
              std::move(list));
          });
        });
      }).handle_error_interruptible(crimson::ct_error::enoent::handle([] {
        return get_extents_if_live_ret(
            interruptible::ready_future_marker{},
            std::list<CachedExtentRef>());
      }), crimson::ct_error::pass_further_all{});
    } else {
      return lba_manager->get_physical_extent_if_live(
	t,
	type,
	paddr,
	laddr,
	len
      ).si_then([=, &t](auto ret) {
        std::list<CachedExtentRef> res;
        if (ret) {
          DEBUGT("{} {}~{} {} is live as physical extent -- {}",
                 t, type, laddr, len, paddr, *ret);
          res.emplace_back(std::move(ret));
        } else {
          DEBUGT("{} {}~{} {} is not live as physical extent",
                 t, type, laddr, len, paddr);
        }
        return get_extents_if_live_ret(
	  interruptible::ready_future_marker{},
	  std::move(res));
      });
    }
  });
}

void TransactionManager::set_cache_info(
  Transaction &t, CachedExtentRef extent, laddr_t laddr) {
  if (extent->get_type() == extent_types_t::OBJECT_DATA_BLOCK) {
    auto &i = t.get_cur_onode_info();
    auto o = extent->cast<ObjectDataBlock>();
    ceph_assert(i.laddr <= laddr &&
		i.laddr + i.length >= reset_shadow_mapping(laddr) + o->get_length());
    o->set_logical_cache_info(i.laddr, i.length);
  }
}

TransactionManager::~TransactionManager() {}

TransactionManagerRef make_transaction_manager(
    Device *primary_device,
    const std::vector<Device*> &secondary_devices,
    bool is_test)
{
  auto epm = std::make_unique<ExtentPlacementManager>();
  auto cache = std::make_unique<Cache>(*epm);
  auto lba_manager = lba_manager::create_lba_manager(
    *cache, !secondary_devices.empty());
  auto sms = std::make_unique<SegmentManagerGroup>();
  auto rbs = std::make_unique<RBMDeviceGroup>();
  auto backref_manager = create_backref_manager(*cache);
  SegmentManagerGroupRef cold_sms = nullptr;
  std::vector<SegmentProvider*> segment_providers_by_id{DEVICE_ID_MAX, nullptr};

  auto p_backend_type = primary_device->get_backend_type();

  if (p_backend_type == backend_type_t::SEGMENTED) {
    auto dtype = primary_device->get_device_type();
    ceph_assert(dtype != device_type_t::HDD &&
		dtype != device_type_t::EPHEMERAL_COLD);
    sms->add_segment_manager(static_cast<SegmentManager*>(primary_device));
  } else {
    auto rbm = std::make_unique<BlockRBManager>(
      static_cast<RBMDevice*>(primary_device), "", is_test);
    rbs->add_rb_manager(std::move(rbm));
  }

  for (auto &p_dev : secondary_devices) {
    if (p_dev->get_backend_type() == backend_type_t::SEGMENTED) {
      if (p_dev->get_device_type() == primary_device->get_device_type()) {
        sms->add_segment_manager(static_cast<SegmentManager*>(p_dev));
      } else {
        if (!cold_sms) {
          cold_sms = std::make_unique<SegmentManagerGroup>();
        }
        cold_sms->add_segment_manager(static_cast<SegmentManager*>(p_dev));
      }
    } else {
      auto rbm = std::make_unique<BlockRBManager>(
	static_cast<RBMDevice*>(p_dev), "", is_test);
      rbs->add_rb_manager(std::move(rbm));
    }
  }

  auto journal_type = p_backend_type;
  device_off_t roll_size;
  device_off_t roll_start;
  if (journal_type == journal_type_t::SEGMENTED) {
    roll_size = static_cast<SegmentManager*>(primary_device)->get_segment_size();
    roll_start = 0;
  } else {
    roll_size = static_cast<random_block_device::RBMDevice*>(primary_device)
		->get_journal_size() - primary_device->get_block_size();
    // see CircularBoundedJournal::get_records_start()
    roll_start = static_cast<random_block_device::RBMDevice*>(primary_device)
		 ->get_shard_journal_start() + primary_device->get_block_size();
    ceph_assert_always(roll_size <= DEVICE_OFF_MAX);
    ceph_assert_always((std::size_t)roll_size + roll_start <=
                       primary_device->get_available_size());
  }
  ceph_assert(roll_size % primary_device->get_block_size() == 0);
  ceph_assert(roll_start % primary_device->get_block_size() == 0);

  bool cleaner_is_detailed;
  SegmentCleaner::config_t cleaner_config;
  JournalTrimmerImpl::config_t trimmer_config;
  if (is_test) {
    cleaner_is_detailed = true;
    cleaner_config = SegmentCleaner::config_t::get_test();
    trimmer_config = JournalTrimmerImpl::config_t::get_test(
        roll_size, journal_type);
  } else {
    cleaner_is_detailed = false;
    cleaner_config = SegmentCleaner::config_t::get_default();
    trimmer_config = JournalTrimmerImpl::config_t::get_default(
        roll_size, journal_type);
  }

  auto journal_trimmer = JournalTrimmerImpl::create(
      *backref_manager, trimmer_config,
      journal_type, roll_start, roll_size);

  AsyncCleanerRef cleaner;
  JournalRef journal;

  SegmentCleanerRef cold_segment_cleaner = nullptr;

  if (cold_sms) {
    cold_segment_cleaner = SegmentCleaner::create(
      cleaner_config,
      std::move(cold_sms),
      *backref_manager,
      epm->get_ool_segment_seq_allocator(),
      cleaner_is_detailed,
      /* is_cold = */ true);
    if (journal_type == journal_type_t::SEGMENTED) {
      for (auto id : cold_segment_cleaner->get_device_ids()) {
        segment_providers_by_id[id] =
          static_cast<SegmentProvider*>(cold_segment_cleaner.get());
      }
    }
  }

  if (journal_type == journal_type_t::SEGMENTED) {
    cleaner = SegmentCleaner::create(
      cleaner_config,
      std::move(sms),
      *backref_manager,
      epm->get_ool_segment_seq_allocator(),
      cleaner_is_detailed);
    auto segment_cleaner = static_cast<SegmentCleaner*>(cleaner.get());
    for (auto id : segment_cleaner->get_device_ids()) {
      segment_providers_by_id[id] =
        static_cast<SegmentProvider*>(segment_cleaner);
    }
    segment_cleaner->set_journal_trimmer(*journal_trimmer);
    journal = journal::make_segmented(
      *segment_cleaner,
      *journal_trimmer);
  } else {
    cleaner = RBMCleaner::create(
      std::move(rbs),
      *backref_manager,
      cleaner_is_detailed);
    journal = journal::make_circularbounded(
      *journal_trimmer,
      static_cast<random_block_device::RBMDevice*>(primary_device),
      "");
  }

  cache->set_segment_providers(std::move(segment_providers_by_id));

  epm->init(std::move(journal_trimmer),
	    std::move(cleaner),
	    std::move(cold_segment_cleaner),
	    cache->get_memory_cache());
  epm->set_primary_device(primary_device);

  return std::make_unique<TransactionManager>(
    std::move(journal),
    std::move(cache),
    std::move(lba_manager),
    std::move(epm),
    std::move(backref_manager));
}

}
