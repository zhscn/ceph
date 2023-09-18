// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/device.h"

namespace crimson::os::seastore {
class Journal;

template <typename F>
auto repeat_eagain(F &&f) {
  return seastar::do_with(
    std::forward<F>(f),
    [](auto &f)
  {
    return crimson::repeat([&f] {
      return std::invoke(f
      ).safe_then([] {
        return seastar::stop_iteration::yes;
      }).handle_error(
        [](const crimson::ct_error::eagain &e) {
          return seastar::stop_iteration::no;
        },
        crimson::ct_error::pass_further_all{}
      );
    });
  });
}

/**
 * TransactionManager
 *
 * Abstraction hiding reading and writing to persistence.
 * Exposes transaction based interface with read isolation.
 */
class TransactionManager : public ExtentCallbackInterface {
public:
  TransactionManager(
    JournalRef journal,
    CacheRef cache,
    LBAManagerRef lba_manager,
    ExtentPlacementManagerRef &&epm,
    BackrefManagerRef&& backref_manager);

  /// Writes initial metadata to disk
  using mkfs_ertr = base_ertr;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = base_ertr;
  mount_ertr::future<> mount();

  /// Closes transaction_manager
  using close_ertr = base_ertr;
  close_ertr::future<> close();

  /// Resets transaction
  void reset_transaction_preserve_handle(Transaction &t) {
    return cache->reset_transaction_preserve_handle(t);
  }

  /**
   * get_pin
   *
   * Get the logical pin at offset
   */
  using get_pin_iertr = LBAManager::get_mapping_iertr;
  using get_pin_ret = LBAManager::get_mapping_iertr::future<LBAMappingRef>;
  get_pin_ret get_pin(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::get_pin);
    SUBTRACET(seastore_tm, "{}", t, offset);
    return lba_manager->get_mapping(t, offset);
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_iertr = LBAManager::get_mappings_iertr;
  using get_pins_ret = get_pins_iertr::future<lba_pin_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::get_pins);
    SUBDEBUGT(seastore_tm, "{}~{}", t, offset, length);
    return lba_manager->get_mappings(
      t, offset, length);
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset~length
   */
  using read_extent_iertr = get_pin_iertr;
  template <typename T>
  using read_extent_ret = read_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBTRACET(seastore_tm, "{}~{}", t, offset, length);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset, length] (auto pin)
      -> read_extent_ret<T> {
      if (length != pin->get_length() || !pin->get_val().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} len {} got wrong pin {}",
            t, offset, length, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->read_pin<T>(t, std::move(pin));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset
   */
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::read_extent);
    SUBTRACET(seastore_tm, "{}", t, offset);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset] (auto pin)
      -> read_extent_ret<T> {
      if (!pin->get_val().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} got wrong pin {}",
            t, offset, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->read_pin<T>(t, std::move(pin));
    });
  }

  template <typename T>
  base_iertr::future<TCachedExtentRef<T>> read_pin(
    Transaction &t,
    LBAMappingRef pin)
  {
    auto v = pin->get_logical_extent(t);
    if (v.has_child()) {
      return v.get_child_fut().safe_then([pin=std::move(pin)](auto extent) {
#ifndef NDEBUG
        auto lextent = extent->template cast<LogicalCachedExtent>();
        auto pin_laddr = pin->get_key();
        if (pin->is_indirect()) {
          pin_laddr = pin->get_intermediate_key();
        }
        assert(lextent->get_laddr() == pin_laddr);
#endif
	return extent->template cast<T>();
      });
    } else {
      return pin_to_extent<T>(t, std::move(pin));
    }
  }

  base_iertr::future<LogicalCachedExtentRef> read_pin_by_type(
    Transaction &t,
    LBAMappingRef pin,
    extent_types_t type)
  {
    auto v = pin->get_logical_extent(t);
    if (v.has_child()) {
      return std::move(v.get_child_fut());
    } else {
      return pin_to_extent_by_type(t, std::move(pin), type);
    }
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    LOG_PREFIX(TransactionManager::get_mutable_extent);
    auto ret = cache->duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    if (!ret->has_laddr()) {
      SUBDEBUGT(seastore_tm,
	"duplicating extent for write -- {} -> {}",
	t,
	*ref,
	*ret);
      ret->set_laddr(ref->get_laddr());
    } else {
      SUBTRACET(seastore_tm,
	"extent is already duplicated -- {}",
	t,
	*ref);
      assert(ref->is_mutable());
      assert(&*ref == &*ret);
    }
    return ret;
  }


  using ref_iertr = LBAManager::ref_iertr;
  using ref_ret = ref_iertr::future<unsigned>;

  /// Add refcount for ref
  ref_ret inc_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Add refcount for offset
  ref_ret inc_ref(
    Transaction &t,
    laddr_t offset);

  /// Remove refcount for ref
  ref_ret dec_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Remove refcount for offset
  ref_ret dec_ref(
    Transaction &t,
    laddr_t offset);

  /// remove refcount for list of offset
  using refs_ret = ref_iertr::future<std::vector<unsigned>>;
  refs_ret dec_ref(
    Transaction &t,
    std::vector<laddr_t> offsets);

  /**
   * alloc_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than laddr_hint.
   */
  using alloc_extent_iertr = LBAManager::alloc_extent_iertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len,
    placement_hint_t placement_hint = placement_hint_t::HOT) {
    LOG_PREFIX(TransactionManager::alloc_extent);
    SUBTRACET(seastore_tm, "{} len={}, placement_hint={}, laddr_hint={}",
              t, T::TYPE, len, placement_hint, laddr_hint);
    ceph_assert(is_aligned(laddr_hint, epm->get_block_size()));
    auto ext = cache->alloc_new_extent<T>(
      t,
      len,
      placement_hint,
      INIT_GENERATION);
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      len,
      ext->get_paddr(),
      P_ADDR_NULL,
      L_ADDR_NULL,
      ext.get()
    ).si_then([ext=std::move(ext), laddr_hint, &t](auto &&) mutable {
      LOG_PREFIX(TransactionManager::alloc_extent);
      SUBDEBUGT(seastore_tm, "new extent: {}, laddr_hint: {}", t, *ext, laddr_hint);
      return alloc_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  /**
   * remap_pin
   *
   * Remap original extent to new extents.
   * Return the pins of new extent.
   */
  struct remap_entry {
    extent_len_t offset;
    extent_len_t len;
    remap_entry(extent_len_t _offset, extent_len_t _len) {
      offset = _offset;
      len = _len;
    }
  };
  using remap_pin_iertr = base_iertr;
  template <std::size_t N>
  using remap_pin_ret = remap_pin_iertr::future<std::array<LBAMappingRef, N>>;
  template <typename T, std::size_t N>
  remap_pin_ret<N> remap_pin(
    Transaction &t,
    LBAMappingRef &&pin,
    std::array<remap_entry, N> remaps) {

#ifndef NDEBUG
    std::sort(remaps.begin(), remaps.end(),
      [](remap_entry x, remap_entry y) {
        return x.offset < y.offset;
    });
    auto original_len = pin->get_length();
    extent_len_t total_remap_len = 0;
    extent_len_t last_offset = 0;
    extent_len_t last_len = 0;

    for (auto &remap : remaps) {
      auto remap_offset = remap.offset;
      auto remap_len = remap.len;
      total_remap_len += remap.len;
      ceph_assert(remap_offset >= (last_offset + last_len));
      last_offset = remap_offset;
      last_len = remap_len;
    }
    ceph_assert(total_remap_len < original_len);
#endif

    // FIXME: paddr can be absolute and pending
    ceph_assert(pin->get_val().is_absolute());
    return cache->get_extent_if_cached(
      t, pin->get_val(), T::TYPE
    ).si_then([this, &t, remaps,
              original_laddr = pin->get_key(),
	      intermediate_base = pin->get_intermediate_base(),
              original_paddr = pin->get_val(),
              original_len = pin->get_length()](auto ext) {
      std::optional<ceph::bufferptr> original_bptr;
      LOG_PREFIX(TransactionManager::remap_pin);
      SUBDEBUGT(seastore_tm,
        "original laddr: {}, original paddr: {}, original length: {},"
        " remap to {} extents",
        t, original_laddr, original_paddr, original_len, remaps.size());
      if (ext) {
        // FIXME: cannot and will not remap a dirty extent for now.
        ceph_assert(!ext->is_dirty());
        ceph_assert(!ext->is_mutable());
        ceph_assert(ext->get_length() == original_len);
        original_bptr = ext->get_bptr();
      }
      return seastar::do_with(
        std::array<LBAMappingRef, N>(),
        0,
        std::move(original_bptr),
        std::vector<remap_entry>(remaps.begin(), remaps.end()),
        [this, &t, original_laddr, original_paddr, original_len, intermediate_base]
        (auto &ret, auto &count, auto &original_bptr, auto &remaps) {
        return dec_ref(t, original_laddr
        ).si_then([this, &t, &original_bptr, &ret, &count, &remaps, intermediate_base,
                   original_laddr, original_paddr, original_len](auto) {
          return trans_intr::do_for_each(
            remaps.begin(),
            remaps.end(),
            [this, &t, &original_bptr, &ret, &count, intermediate_base,
              original_laddr, original_paddr, original_len](auto &remap) {
            LOG_PREFIX(TransactionManager::remap_pin);
            auto remap_offset = remap.offset;
            auto remap_len = remap.len;
            auto remap_laddr = original_laddr + remap_offset;
            auto remap_paddr = original_paddr.add_offset(remap_offset);
            ceph_assert(remap_len < original_len);
            ceph_assert(remap_offset + remap_len <= original_len);
            ceph_assert(remap_len != 0);
            ceph_assert(remap_offset % cache->get_block_size() == 0);
            ceph_assert(remap_len % cache->get_block_size() == 0);
            SUBDEBUGT(seastore_tm,
              "remap laddr: {}, remap paddr: {}, remap length: {}", t,
              remap_laddr, remap_paddr, remap_len);
            return alloc_remapped_extent<T>(
              t,
              remap_laddr,
              remap_paddr,
              remap_len,
              original_laddr,
	      intermediate_base,
              std::move(original_bptr)
            ).si_then([&ret, &count, remap_laddr](auto &&npin) {
              ceph_assert(npin->get_key() == remap_laddr);
              ret[count++] = std::move(npin);
            });
          });
        }).handle_error_interruptible(
           remap_pin_iertr::pass_further{},
           crimson::ct_error::assert_all{
              "TransactionManager::remap_pin hit invalid error"
           }
        ).si_then([&ret, &count] {
          ceph_assert(count == N);
          return remap_pin_iertr::make_ready_future<
            std::array<LBAMappingRef, N>>(std::move(ret));
        });
      });
    });
  }

  using reserve_extent_iertr = alloc_extent_iertr;
  using reserve_extent_ret = reserve_extent_iertr::future<LBAMappingRef>;
  reserve_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    LOG_PREFIX(TransactionManager::reserve_region);
    SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}", t, len, hint);
    ceph_assert(is_aligned(hint, epm->get_block_size()));
    return lba_manager->alloc_extent(
      t,
      hint,
      len,
      P_ADDR_ZERO,
      P_ADDR_NULL,
      L_ADDR_NULL,
      nullptr);
  }

  /*
   * clone_extent
   *
   * create an indirect lba mapping pointing to the physical
   * lba mapping whose key is clone_offset. Resort to btree_lba_manager.h
   * for the definition of "indirect lba mapping" and "physical lba mapping"
   *
   */
  using clone_extent_iertr = alloc_extent_iertr;
  using clone_extent_ret = clone_extent_iertr::future<LBAMappingRef>;
  clone_extent_ret clone_extent(
    Transaction &t,
    laddr_t hint,
    const LBAMapping &mapping) {
    auto clone_offset =
      mapping.is_indirect()
	? mapping.get_intermediate_key()
	: mapping.get_key();

    LOG_PREFIX(TransactionManager::clone_extent);
    SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}, clone_offset {}",
      t, mapping.get_length(), hint, clone_offset);
    ceph_assert(is_aligned(hint, epm->get_block_size()));
    return lba_manager->alloc_extent(
      t,
      hint,
      mapping.get_length(),
      clone_offset,
      mapping.get_val(),
      clone_offset,
      nullptr
    ).si_then([this, &t, clone_offset](auto pin) {
      return inc_ref(t, clone_offset
      ).si_then([pin=std::move(pin)](auto) mutable {
	return std::move(pin);
      }).handle_error_interruptible(
	crimson::ct_error::input_output_error::pass_further(),
	crimson::ct_error::assert_all("not possible")
      );
    });
  }

  /* alloc_extents
   *
   * allocates more than one new blocks of type T.
   */
   using alloc_extents_iertr = alloc_extent_iertr;
   template<class T>
   alloc_extents_iertr::future<std::vector<TCachedExtentRef<T>>>
   alloc_extents(
     Transaction &t,
     laddr_t hint,
     extent_len_t len,
     int num) {
     LOG_PREFIX(TransactionManager::alloc_extents);
     SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}, num={}",
               t, len, hint, num);
     return seastar::do_with(std::vector<TCachedExtentRef<T>>(),
       [this, &t, hint, len, num] (auto &extents) {
       return trans_intr::do_for_each(
                       boost::make_counting_iterator(0),
                       boost::make_counting_iterator(num),
         [this, &t, len, hint, &extents] (auto i) {
         return alloc_extent<T>(t, hint, len).si_then(
           [&extents](auto &&node) {
           extents.push_back(node);
         });
       }).si_then([&extents] {
         return alloc_extents_iertr::make_ready_future
                <std::vector<TCachedExtentRef<T>>>(std::move(extents));
       });
     });
  }

  /**
   * submit_transaction
   *
   * Atomically submits transaction to persistence
   */
  using submit_transaction_iertr = base_iertr;
  submit_transaction_iertr::future<> submit_transaction(Transaction &);

  /**
   * flush
   *
   * Block until all outstanding IOs on handle are committed.
   * Note, flush() machinery must go through the same pipeline
   * stages and locks as submit_transaction.
   */
  seastar::future<> flush(OrderingHandle &handle);

  /*
   * ExtentCallbackInterface
   */

  /// weak transaction should be type READ
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name,
      bool is_weak=false) final {
    return cache->create_transaction(src, name, is_weak);
  }

  using ExtentCallbackInterface::submit_transaction_direct_ret;
  submit_transaction_direct_ret submit_transaction_direct(
    Transaction &t,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt) final;

  using ExtentCallbackInterface::get_next_dirty_extents_ret;
  get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,
    journal_seq_t seq,
    size_t max_bytes) final;

  using ExtentCallbackInterface::rewrite_extent_ret;
  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent,
    rewrite_gen_t target_generation,
    sea_time_point modify_time) final;

  using ExtentCallbackInterface::get_extents_if_live_ret;
  get_extents_if_live_ret get_extents_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t len) final;

  /**
   * read_root_meta
   *
   * Read root block meta entry for key.
   */
  using read_root_meta_iertr = base_iertr;
  using read_root_meta_bare = std::optional<std::string>;
  using read_root_meta_ret = read_root_meta_iertr::future<
    read_root_meta_bare>;
  read_root_meta_ret read_root_meta(
    Transaction &t,
    const std::string &key) {
    return cache->get_root(
      t
    ).si_then([&key, &t](auto root) {
      LOG_PREFIX(TransactionManager::read_root_meta);
      auto meta = root->root.get_meta();
      auto iter = meta.find(key);
      if (iter == meta.end()) {
        SUBDEBUGT(seastore_tm, "{} -> nullopt", t, key);
	return seastar::make_ready_future<read_root_meta_bare>(std::nullopt);
      } else {
        SUBDEBUGT(seastore_tm, "{} -> {}", t, key, iter->second);
	return seastar::make_ready_future<read_root_meta_bare>(iter->second);
      }
    });
  }

  /**
   * update_root_meta
   *
   * Update root block meta entry for key to value.
   */
  using update_root_meta_iertr = base_iertr;
  using update_root_meta_ret = update_root_meta_iertr::future<>;
  update_root_meta_ret update_root_meta(
    Transaction& t,
    const std::string& key,
    const std::string& value) {
    LOG_PREFIX(TransactionManager::update_root_meta);
    SUBDEBUGT(seastore_tm, "seastore_tm, {} -> {}", t, key, value);
    return cache->get_root(
      t
    ).si_then([this, &t, &key, &value](RootBlockRef root) {
      root = cache->duplicate_for_write(t, root)->cast<RootBlock>();

      auto meta = root->root.get_meta();
      meta[key] = value;

      root->root.set_meta(meta);
      return seastar::now();
    });
  }

  /**
   * read_onode_root
   *
   * Get onode-tree root logical address
   */
  using read_onode_root_iertr = base_iertr;
  using read_onode_root_ret = read_onode_root_iertr::future<laddr_t>;
  read_onode_root_ret read_onode_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_onode_root);
      laddr_t ret = croot->get_root().onode_root;
      SUBTRACET(seastore_tm, "{}", t, ret);
      return ret;
    });
  }

  /**
   * write_onode_root
   *
   * Write onode-tree root logical address, must be called after read.
   */
  void write_onode_root(Transaction &t, laddr_t addr) {
    LOG_PREFIX(TransactionManager::write_onode_root);
    SUBDEBUGT(seastore_tm, "{}", t, addr);
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().onode_root = addr;
  }

  /**
   * read_collection_root
   *
   * Get collection root addr
   */
  using read_collection_root_iertr = base_iertr;
  using read_collection_root_ret = read_collection_root_iertr::future<
    coll_root_t>;
  read_collection_root_ret read_collection_root(Transaction &t) {
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_collection_root);
      auto ret = croot->get_root().collection_root.get();
      SUBTRACET(seastore_tm, "{}~{}",
                t, ret.get_location(), ret.get_size());
      return ret;
    });
  }

  /**
   * write_collection_root
   *
   * Update collection root addr
   */
  void write_collection_root(Transaction &t, coll_root_t cmroot) {
    LOG_PREFIX(TransactionManager::write_collection_root);
    SUBDEBUGT(seastore_tm, "{}~{}",
              t, cmroot.get_location(), cmroot.get_size());
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().collection_root.update(cmroot);
  }

  extent_len_t get_block_size() const {
    return epm->get_block_size();
  }

  store_statfs_t store_stat() const {
    return epm->get_stat();
  }

  ~TransactionManager();

private:
  friend class Transaction;

  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  BackrefManagerRef backref_manager;

  WritePipeline write_pipeline;

  rewrite_extent_ret rewrite_logical_extent(
    Transaction& t,
    LogicalCachedExtentRef extent);

  submit_transaction_direct_ret do_submit_transaction(
    Transaction &t,
    ExtentPlacementManager::dispatch_result_t dispatch_result,
    std::optional<journal_seq_t> seq_to_trim = std::nullopt);

  /**
   * pin_to_extent
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_iertr = base_iertr;
  template <typename T>
  using pin_to_extent_ret = pin_to_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  pin_to_extent_ret<T> pin_to_extent(
    Transaction &t,
    LBAMappingRef pin) {
    LOG_PREFIX(TransactionManager::pin_to_extent);
    SUBTRACET(seastore_tm, "getting extent {}", t, *pin);
    static_assert(is_logical_type(T::TYPE));
    using ret = pin_to_extent_ret<T>;
    auto &pref = *pin;
    return cache->get_absent_extent<T>(
      t,
      pref.get_val(),
      pref.get_length(),
      [pin=std::move(pin)]
      (T &extent) mutable {
	assert(!extent.has_laddr());
	assert(!extent.has_been_invalidated());
	assert(!pin->has_been_invalidated());
	assert(pin->get_parent());
	pin->link_child(&extent);
	extent.set_laddr(
	  pin->is_indirect()
	  ? pin->get_intermediate_key()
	  : pin->get_key());
      }
    ).si_then([FNAME, &t](auto ref) mutable -> ret {
      SUBTRACET(seastore_tm, "got extent -- {}", t, *ref);
      assert(ref->is_fully_loaded());
      return pin_to_extent_ret<T>(
	interruptible::ready_future_marker{},
	std::move(ref));
    });
  }

  /**
   * pin_to_extent_by_type
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_by_type_ret = pin_to_extent_iertr::future<
    LogicalCachedExtentRef>;
  pin_to_extent_by_type_ret pin_to_extent_by_type(
      Transaction &t,
      LBAMappingRef pin,
      extent_types_t type)
  {
    LOG_PREFIX(TransactionManager::pin_to_extent_by_type);
    SUBTRACET(seastore_tm, "getting extent {} type {}", t, *pin, type);
    assert(is_logical_type(type));
    auto &pref = *pin;
    return cache->get_absent_extent_by_type(
      t,
      type,
      pref.get_val(),
      pref.get_key(),
      pref.get_length(),
      [pin=std::move(pin)](CachedExtent &extent) mutable {
	auto &lextent = static_cast<LogicalCachedExtent&>(extent);
	assert(!lextent.has_laddr());
	assert(!lextent.has_been_invalidated());
	assert(!pin->has_been_invalidated());
	assert(pin->get_parent());
	assert(!pin->get_parent()->is_pending());
	pin->link_child(&lextent);
	lextent.set_laddr(
	  pin->is_indirect()
	  ? pin->get_intermediate_key()
	  : pin->get_key());
      }
    ).si_then([FNAME, &t](auto ref) {
      SUBTRACET(seastore_tm, "got extent -- {}", t, *ref);
      assert(ref->is_fully_loaded());
      return pin_to_extent_by_type_ret(
	interruptible::ready_future_marker{},
	std::move(ref->template cast<LogicalCachedExtent>()));
    });
  }

  /**
   * alloc_remapped_extent
   *
   * Allocates a new extent at given remap_paddr that must be absolute and
   * use the buffer to fill the new extent if buffer exists. Otherwise, will
   * not read disk to fill the new extent.
   * Returns the new extent.
   *
   * Should make sure the end laddr of remap extent <= the end laddr of
   * original extent when using this method.
   */
  using alloc_remapped_extent_iertr =
    alloc_extent_iertr::extend_ertr<Device::read_ertr>;
  using alloc_remapped_extent_ret =
    alloc_remapped_extent_iertr::future<LBAMappingRef>;
  template <typename T>
  alloc_remapped_extent_ret alloc_remapped_extent(
    Transaction &t,
    laddr_t remap_laddr,
    paddr_t remap_paddr,
    extent_len_t remap_length,
    laddr_t original_laddr,
    laddr_t intermediate_base,
    std::optional<ceph::bufferptr> &&original_bptr) {
    LOG_PREFIX(TransactionManager::alloc_remapped_extent);
    SUBDEBUG(seastore_tm, "alloc remapped extent: remap_laddr: {}, "
      "remap_paddr: {}, remap_length: {}, has data in cache: {} ",
      remap_laddr, remap_paddr, remap_length,
      original_bptr.has_value() ? "true":"false");
    auto ext = cache->alloc_remapped_extent<T>(
      t,
      remap_laddr,
      remap_paddr,
      remap_length,
      original_laddr,
      std::move(original_bptr));
    return lba_manager->alloc_extent(
      t,
      remap_laddr,
      remap_length,
      remap_paddr,
      P_ADDR_NULL,
      intermediate_base,
      ext.get()
    ).si_then([remap_laddr, remap_length, remap_paddr](auto &&ref) {
      assert(ref->get_key() == remap_laddr);
      assert(ref->get_val() == remap_paddr);
      assert(ref->get_length() == remap_length);
      return alloc_remapped_extent_iertr::make_ready_future
        <LBAMappingRef>(std::move(ref));
    });
  }

public:
  // Testing interfaces
  auto get_epm() {
    return epm.get();
  }

  auto get_lba_manager() {
    return lba_manager.get();
  }

  auto get_backref_manager() {
    return backref_manager.get();
  }

  auto get_cache() {
    return cache.get();
  }
  auto get_journal() {
    return journal.get();
  }
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

TransactionManagerRef make_transaction_manager(
    Device *primary_device,
    const std::vector<Device*> &secondary_devices,
    bool is_test);
}
