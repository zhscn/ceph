// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <utility>
#include <functional>

#include "crimson/common/log.h"

#include "crimson/os/seastore/object_data_handler.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_odata);
  }
}

SET_SUBSYS(seastore_odata);

namespace crimson::os::seastore {
#define assert_aligned(x) ceph_assert(((x)%ctx.tm.get_block_size()) == 0)

using context_t = ObjectDataHandler::context_t;
using get_iertr = ObjectDataHandler::write_iertr;

/**
 * extent_to_write_t
 *
 * Encapsulates extents to be written out using do_insertions.
 * Indicates a zero/existing extent or a data extent based on whether
 * to_write is populate.
 * The meaning of existing_paddr is that the new extent to be
 * written is the part of exising extent on the disk. existing_paddr
 * must be absolute.
 */
struct extent_to_write_t {
  enum class type_t {
    DATA,
    ZERO,
    EXISTING,
    SPLIT,
    SHADOW_EXISTING
  };

  type_t type;
  laddr_t addr;
  extent_len_t len;
  laddr_t indirect_key;
  /// non-nullopt if and only if type == DATA
  std::optional<bufferlist> to_write;
  /// non-nullopt if and only if type == EXISTING
  std::optional<paddr_t> existing_paddr;
  extent_len_t right_len;

  extent_to_write_t(const extent_to_write_t &other) {
    type = other.type;
    addr = other.addr;
    len = other.len;
    indirect_key = other.indirect_key;
    to_write = other.to_write;
    existing_paddr = other.existing_paddr;
  }
  extent_to_write_t(extent_to_write_t &&) = default;

  bool is_data() const {
    return type == type_t::DATA;
  }

  bool is_zero() const {
    return type == type_t::ZERO;
  }

  bool is_existing() const {
    return type == type_t::EXISTING || type == type_t::SHADOW_EXISTING;
  }

  bool is_shadow_exsiting() const {
    return type == type_t::SHADOW_EXISTING;
  }

  bool is_split() const {
    return type == type_t::SPLIT;
  }

  laddr_t get_end_addr() const {
    return addr + len;
  }

  static extent_to_write_t create_data(
      laddr_t addr, bufferlist to_write) {
    return extent_to_write_t(addr, to_write);
  }

  static extent_to_write_t create_zero(
      laddr_t addr, extent_len_t len) {
    return extent_to_write_t(addr, len);
  }

  static extent_to_write_t create_existing(
      laddr_t addr,
      paddr_t existing_paddr,
      extent_len_t len,
      laddr_t indirect_key) {
    return extent_to_write_t(
      addr,
      existing_paddr,
      len,
      indirect_key);
  }

  static extent_to_write_t create_split(
      laddr_t addr,
      extent_len_t left_len,
      extent_len_t right_len) {
    return extent_to_write_t(
      addr,
      left_len,
      right_len);
  }

  static extent_to_write_t create_shadow_existing(
      laddr_t addr, paddr_t existing_paddr, extent_len_t len) {
    auto res = extent_to_write_t(
      addr,
      existing_paddr,
      len,
      L_ADDR_NULL,
      nullptr);
    res.type = type_t::SHADOW_EXISTING;
    return res;
  }

private:
  extent_to_write_t(laddr_t addr, bufferlist to_write)
    : type(type_t::DATA), addr(addr), len(to_write.length()),
      to_write(to_write) {}

  extent_to_write_t(laddr_t addr, extent_len_t len)
    : type(type_t::ZERO), addr(addr), len(len) {}

  extent_to_write_t(laddr_t addr, extent_len_t left_len, extent_len_t right_len)
    : type(type_t::SPLIT), addr(addr), len(left_len), right_len(right_len) {}

  extent_to_write_t(
    laddr_t addr,
    paddr_t existing_paddr,
    extent_len_t len,
    laddr_t indirect_key)
    : type(type_t::EXISTING), addr(addr), len(len),
      indirect_key(indirect_key), to_write(std::nullopt),
      existing_paddr(existing_paddr) {}
};
using extent_to_write_list_t = std::list<extent_to_write_t>;

/**
 * append_extent_to_write
 *
 * Appends passed extent_to_write_t maintaining invariant that the
 * list may not contain consecutive zero elements by checking and
 * combining them.
 */
void append_extent_to_write(
  extent_to_write_list_t &to_write, extent_to_write_t &&to_append)
{
  assert(to_write.empty() ||
         to_write.back().get_end_addr() == to_append.addr ||
	 to_append.is_shadow_exsiting() ||
	 to_write.back().is_shadow_exsiting());
  if (to_write.empty() ||
      to_write.back().is_data() ||
      to_append.is_data() ||
      to_write.back().type != to_append.type) {
    to_write.push_back(std::move(to_append));
  } else {
    to_write.back().len += to_append.len;
  }
}

/**
 * splice_extent_to_write
 *
 * splices passed extent_to_write_list_t maintaining invariant that the
 * list may not contain consecutive zero elements by checking and
 * combining them.
 */
void splice_extent_to_write(
  extent_to_write_list_t &to_write, extent_to_write_list_t &&to_splice)
{
  if (!to_splice.empty()) {
    append_extent_to_write(to_write, std::move(to_splice.front()));
    to_splice.pop_front();
    to_write.splice(to_write.end(), std::move(to_splice));
  }
}

/// Removes extents/mappings in pins
ObjectDataHandler::write_ret do_removals(
  context_t ctx,
  lba_pin_list_t &pins)
{
  return seastar::do_with(
    pins.front()->get_key(),
    pins.front()->get_key(),
    [ctx, &pins](auto &next_laddr, auto &last_laddr) {
    return trans_intr::do_for_each(
      pins,
      [ctx, &next_laddr, &last_laddr](auto &pin) {
      LOG_PREFIX(object_data_handler.cc::do_removals);
      if (pin->get_key() < next_laddr) {
	// with lba indirect in place, chances are that
	// one lba mapping corresponds to multiple pins.
	// For example:
	// 	lba_mapping for clone1: laddr_1a ~ 4096, laddr_1b ~ 4096
	// 	lba_mapping for clone2: laddr_2a ~ 8192(indirection of
	// 						laddr_2a is laddr_1a)
	// Although clone2 has only one lba_mapping laddr_2a ~ 8192, transaction
	// manager's get_pins() will return two pins: laddr_2a~4096,laddr_2a+4096~4096
	//
	// So when removing pins, we may skip some pins
	DEBUGT("decreasing ref: {}, skipped",
	       ctx.t,
	       pin->get_key());
	ceph_assert(pin->get_key() > last_laddr);
	return ObjectDataHandler::write_iertr::now();
      }
      ceph_assert(pin->get_key() == next_laddr);
      DEBUGT("decreasing ref: {}",
	     ctx.t,
	     pin->get_key());
      if (pin->is_shadow_mapping()) {
	return ObjectDataHandler::write_iertr::now();
      }
      return ctx.tm.dec_ref(
	ctx.t,
	pin->get_key()
      ).si_then(
	[&next_laddr, &last_laddr](auto result){
	  last_laddr = next_laddr;
	  next_laddr += result.length;
	},
	ObjectDataHandler::write_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "object_data_handler::do_removals invalid error"
	}
      );
    });
  });
}

/// Creates zero/data extents in to_write
ObjectDataHandler::write_ret do_insertions(
  context_t ctx,
  extent_to_write_list_t &to_write)
{
  return trans_intr::do_for_each(
    to_write,
    [ctx](auto &region) {
      LOG_PREFIX(object_data_handler.cc::do_insertions);
      if (region.is_data()) {
	assert_aligned(region.addr);
	assert_aligned(region.len);
	ceph_assert(region.len == region.to_write->length());
	DEBUGT("allocating extent: {}~{}",
	       ctx.t,
	       region.addr,
	       region.len);
	return ctx.tm.alloc_extent<ObjectDataBlock>(
	  ctx.t,
	  region.addr,
	  region.len
	).si_then([&region](auto extent) {
	  if (extent->get_laddr() != region.addr) {
	    logger().debug(
	      "object_data_handler::do_insertions alloc got addr {},"
	      " should have been {}",
	      extent->get_laddr(),
	      region.addr);
	  }
	  ceph_assert(extent->get_laddr() == region.addr);
	  ceph_assert(extent->get_length() == region.len);
	  auto iter = region.to_write->cbegin();
	  iter.copy(region.len, extent->get_bptr().c_str());
	  return ObjectDataHandler::write_iertr::now();
	});
      } else if (region.is_zero()) {
	DEBUGT("reserving: {}~{}",
	       ctx.t,
	       region.addr,
	       region.len);
	return ctx.tm.reserve_region(
	  ctx.t,
	  region.addr,
	  region.len
	).si_then([FNAME, ctx, &region](auto pin) {
	  ceph_assert(pin->get_length() == region.len);
	  if (pin->get_key() != region.addr) {
	    ERRORT(
	      "inconsistent laddr: pin: {} region {}",
	      ctx.t,
	      pin->get_key(),
	      region.addr);
	  }
	  ceph_assert(pin->get_key() == region.addr);
	  return ObjectDataHandler::write_iertr::now();
	});
      } else {
	ceph_abort();
	ceph_assert(region.is_existing());
	DEBUGT("map existing extent: laddr {} len {} {}",
	       ctx.t, region.addr, region.len, *region.existing_paddr);
	return ctx.tm.map_existing_extent<ObjectDataBlock>(
	  ctx.t, region.addr, *region.existing_paddr,
	  region.len, region.indirect_key,
	  region.is_shadow_exsiting()
	).handle_error_interruptible(
	  TransactionManager::alloc_extent_iertr::pass_further{},
	  Device::read_ertr::assert_all{"ignore read error"}
	).si_then([FNAME, ctx, &region](auto extent) {
	  if (!extent) {
	    ceph_assert(region.indirect_key != L_ADDR_NULL);
	    return ObjectDataHandler::write_iertr::now();
	  }
	  if (extent->get_laddr() != region.addr
	      && extent->get_laddr() != region.indirect_key) {
	    ERRORT(
	      "inconsistent laddr: extent: {} region {}/{}",
	      ctx.t,
	      extent->get_laddr(),
	      region.addr,
	      region.indirect_key);
	  }
	  ceph_assert(extent->get_laddr() == region.addr
	    || extent->get_laddr() == region.indirect_key);
	  return ObjectDataHandler::write_iertr::now();
	});
      }
    });
}

enum class drop_part_t : uint8_t {
  NONE,
  LEFT,
  RIGHT
};

struct split_info_t {
  paddr_t paddr;
  extent_len_t left, right;
  drop_part_t to_drop = drop_part_t::NONE;
  bool mapping_only = false;
};

ObjectDataHandler::write_ret do_split(
  context_t ctx,
  std::map<laddr_t, std::list<split_info_t>> &to_split,
  lba_pin_list_t &pins)
{
  return trans_intr::do_for_each(to_split, [ctx, &pins](auto &split_op)
                                 -> ObjectDataHandler::write_iertr::future<> {
    auto laddr = split_op.first;
    auto &lens = split_op.second;
    assert(lens.size() > 0 && lens.size() <= 2);
    if (lens.size() == 1) {
      return ctx.tm.split_extent<ObjectDataBlock>(
        ctx.t,
        laddr,
	lens.front().paddr,
        lens.front().left,
        lens.front().right,
	lens.front().mapping_only
      ).si_then([laddr, &lens, &pins](auto p) {
        assert(p.first->get_length() == lens.front().left);
        assert(p.second->get_length() == lens.front().right);
	switch (lens.front().to_drop) {
	case drop_part_t::LEFT:
	  assert(laddr == pins.back()->get_key());
	  pins.pop_back();
	  pins.emplace_back(std::move(p.first));
	  break;
	case drop_part_t::RIGHT:
	  assert(laddr == pins.front()->get_key());
	  pins.pop_front();
	  pins.emplace_front(std::move(p.second));
	  break;
	case drop_part_t::NONE:
	  break;
	default:
	  ceph_abort();
	}
      });
    } else if (lens.size() == 2) {
      assert(lens.front().left + lens.front().right ==
             lens.back().left + lens.back().right);
      assert(lens.front().left < lens.back().left);
      assert(pins.size() == 1);
      assert(laddr == pins.front()->get_key()
	|| laddr == pins.front()->get_intermediate_key());
      return ctx.tm.split_extent<ObjectDataBlock>(
        ctx.t,
        laddr,
	lens.front().paddr,
        lens.front().left,
        lens.front().right,
	lens.front().mapping_only
      ).si_then([ctx, &lens, &pins](auto p) {
        auto left_len = lens.back().left;
        auto right_len = lens.back().right;
        left_len -= lens.front().left;
        assert(left_len + right_len == p.second->get_length());
        return ctx.tm.split_extent<ObjectDataBlock>(
          ctx.t,
          p.second->get_key(),
	  p.second->get_val(),
          left_len,
          right_len,
	  lens.back().mapping_only
        ).si_then([left_len, right_len, &lens, &pins](auto p) {
          assert(p.first->get_length() == left_len);
          assert(p.second->get_length() == right_len);
	  if (lens.back().to_drop != drop_part_t::NONE) {
	    ceph_assert(lens.back().to_drop == drop_part_t::LEFT);
	    pins.pop_front();
	    pins.emplace_front(std::move(p.first));
	  }
        });
      });
    } else {
      ceph_abort();
      return ObjectDataHandler::write_iertr::now();
    }
  });
}

enum class overwrite_operation_t {
  UNKNOWN,
  OVERWRITE_ZERO,           // fill unaligned data with zero
  MERGE_EXISTING,           // if present, merge data with the clean/pending extent
  SPLIT_EXISTING,           // split the existing extent, and fill unaligned data
};

std::ostream& operator<<(
  std::ostream &out,
  const overwrite_operation_t &operation)
{
  switch (operation) {
  case overwrite_operation_t::UNKNOWN:
    return out << "UNKNOWN";
  case overwrite_operation_t::OVERWRITE_ZERO:
    return out << "OVERWRITE_ZERO";
  case overwrite_operation_t::MERGE_EXISTING:
    return out << "MERGE_EXISTING";
  case overwrite_operation_t::SPLIT_EXISTING:
    return out << "SPLIT_EXISTING";
  default:
    return out << "!IMPOSSIBLE_OPERATION";
  }
}

/**
 * overwrite_plan_t
 *
 * |<--------------------------pins_size---------------------------------------------->|
 * pin_begin(aligned)                                                   pin_end(aligned)
 *                 |<------aligned_data_size-------------------------->| (aligned-bl)
 *                 aligned_data_begin                   aligned_data_end
 *                                    |<-data_size->| (bl)
 *                                    data_begin  end
 *             left(l)                                            right(r)
 * |<l_extent_size>|<l_alignment_size>|             |<r_alignment_size>|<r_extent_size>|
 * |<-----------left_size------------>|             |<-----------right_size----------->|
 *
 * |<-----(existing left extent/pin)----->|    |<-----(existing right extent/pin)----->|
 * left_paddr                                  right_paddr
 */
struct overwrite_plan_t {
  // addresses
  laddr_t pin_begin;
  laddr_t pin_end;
  paddr_t left_paddr;
  paddr_t right_paddr;
  paddr_t shadow_left_paddr;
  paddr_t shadow_right_paddr;
  laddr_t data_begin;
  laddr_t data_end;
  laddr_t aligned_data_begin;
  laddr_t aligned_data_end;
  laddr_t left_indirect_key = L_ADDR_NULL;
  laddr_t right_indirect_key = L_ADDR_NULL;

  // operations
  overwrite_operation_t left_operation;
  overwrite_operation_t right_operation;

  // helper member
  extent_len_t block_size;

public:
  extent_len_t get_left_size() const {
    return data_begin - pin_begin;
  }

  extent_len_t get_left_extent_size() const {
    return aligned_data_begin - pin_begin;
  }

  extent_len_t get_left_alignment_size() const {
    return data_begin - aligned_data_begin;
  }

  extent_len_t get_right_size() const {
    return pin_end - data_end;
  }

  extent_len_t get_right_extent_size() const {
    assert(pin_end >= aligned_data_end);
    return pin_end - aligned_data_end;
  }

  extent_len_t get_right_alignment_size() const {
    return aligned_data_end - data_end;
  }

  extent_len_t get_aligned_data_size() const {
    return aligned_data_end - aligned_data_begin;
  }

  extent_len_t get_pins_size() const {
    return pin_end - pin_begin;
  }

  friend std::ostream& operator<<(
    std::ostream& out,
    const overwrite_plan_t& overwrite_plan) {
    return out << "overwrite_plan_t("
	       << "pin_begin=" << overwrite_plan.pin_begin
	       << ", pin_end=" << overwrite_plan.pin_end
	       << ", left_paddr=" << overwrite_plan.left_paddr
	       << ", right_paddr=" << overwrite_plan.right_paddr
	       << ", data_begin=" << overwrite_plan.data_begin
	       << ", data_end=" << overwrite_plan.data_end
	       << ", aligned_data_begin=" << overwrite_plan.aligned_data_begin
	       << ", aligned_data_end=" << overwrite_plan.aligned_data_end
	       << ", left_indirect_key=" << overwrite_plan.left_indirect_key
	       << ", right_indirect_key=" << overwrite_plan.right_indirect_key
	       << ", left_operation=" << overwrite_plan.left_operation
	       << ", right_operation=" << overwrite_plan.right_operation
	       << ", block_size=" << overwrite_plan.block_size
	       << ")";
  }

  overwrite_plan_t(laddr_t offset,
		   extent_len_t len,
		   const lba_pin_list_t& pins,
		   extent_len_t block_size) :
      pin_begin(pins.front()->get_key()),
      pin_end(L_ADDR_NULL),
      left_paddr(pins.front()->get_val()),
      right_paddr(P_ADDR_NULL),
      shadow_left_paddr(P_ADDR_NULL),
      shadow_right_paddr(P_ADDR_NULL),
      data_begin(offset),
      data_end(offset + len),
      aligned_data_begin(p2align((uint64_t)data_begin, (uint64_t)block_size)),
      aligned_data_end(p2roundup((uint64_t)data_end, (uint64_t)block_size)),
      left_indirect_key(
	pins.front()->is_indirect()
	  ? pins.front()->get_intermediate_key()
	  : L_ADDR_NULL),
      right_indirect_key(
	pins.back()->is_indirect()
	  ? pins.back()->get_intermediate_key()
	  : L_ADDR_NULL),
      left_operation(overwrite_operation_t::UNKNOWN),
      right_operation(overwrite_operation_t::UNKNOWN),
      block_size(block_size) {
    auto b = pins.begin();
    b++;
    if (b != pins.end() && (*b)->is_shadow_mapping()) {
      shadow_left_paddr = (*b)->get_val();
    }
    auto e = pins.rbegin();
    assert(e != pins.rend());
    if ((*e)->is_shadow_mapping()) {
      shadow_right_paddr = (*e)->get_val();
      e++;
      assert(e != pins.rend());
    }
    pin_end = (*e)->get_key() + (*e)->get_length();
    right_paddr = (*e)->get_val();

    validate();
    evaluate_operations();
    assert(left_operation != overwrite_operation_t::UNKNOWN);
    assert(right_operation != overwrite_operation_t::UNKNOWN);
  }

private:
  // refer to overwrite_plan_t description
  void validate() const {
    ceph_assert(pin_begin % block_size == 0);
    ceph_assert(pin_end % block_size == 0);
    ceph_assert(aligned_data_begin % block_size == 0);
    ceph_assert(aligned_data_end % block_size == 0);

    ceph_assert(pin_begin <= aligned_data_begin);
    ceph_assert(aligned_data_begin <= data_begin);
    ceph_assert(data_begin <= data_end);
    ceph_assert(data_end <= aligned_data_end);
    ceph_assert(aligned_data_end <= pin_end);
  }

  /*
   * When trying to modify a portion of an object data block, follow
   * the read-full-extent-then-merge-new-data strategy, if the write
   * amplification caused by it is not greater than
   * seastore_obj_data_write_amplification; otherwise, split the
   * original extent into at most three parts: origin-left, part-to-be-modified
   * and origin-right.
   */
  void evaluate_operations() {
    auto actual_write_size = get_pins_size();
    auto aligned_data_size = get_aligned_data_size();
    auto left_ext_size = get_left_extent_size();
    auto right_ext_size = get_right_extent_size();

    if (left_paddr.is_zero()) {
      actual_write_size -= left_ext_size;
      left_ext_size = 0;
      left_operation = overwrite_operation_t::OVERWRITE_ZERO;
    // FIXME: left_paddr can be absolute and pending
    } else if (left_paddr.is_relative() ||
	       left_paddr.is_delayed()) {
      aligned_data_size += left_ext_size;
      left_ext_size = 0;
      left_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    if (right_paddr.is_zero()) {
      actual_write_size -= right_ext_size;
      right_ext_size = 0;
      right_operation = overwrite_operation_t::OVERWRITE_ZERO;
    // FIXME: right_paddr can be absolute and pending
    } else if (right_paddr.is_relative() ||
	       right_paddr.is_delayed()) {
      aligned_data_size += right_ext_size;
      right_ext_size = 0;
      right_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    while (left_operation == overwrite_operation_t::UNKNOWN ||
           right_operation == overwrite_operation_t::UNKNOWN) {
      if (((double)actual_write_size / (double)aligned_data_size) <=
          crimson::common::get_conf<double>("seastore_obj_data_write_amplification")) {
        break;
      }
      if (left_ext_size == 0 && right_ext_size == 0) {
        break;
      }
      if (left_ext_size >= right_ext_size) {
        // split left
        assert(left_operation == overwrite_operation_t::UNKNOWN);
        actual_write_size -= left_ext_size;
        left_ext_size = 0;
        left_operation = overwrite_operation_t::SPLIT_EXISTING;
      } else { // left_ext_size < right_ext_size
        // split right
        assert(right_operation == overwrite_operation_t::UNKNOWN);
        actual_write_size -= right_ext_size;
        right_ext_size = 0;
        right_operation = overwrite_operation_t::SPLIT_EXISTING;
      }
    }

    if (left_operation == overwrite_operation_t::UNKNOWN) {
      // no split left, so merge with left
      left_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    if (right_operation == overwrite_operation_t::UNKNOWN) {
      // no split right, so merge with right
      right_operation = overwrite_operation_t::MERGE_EXISTING;
    }
  }
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::overwrite_plan_t> : fmt::ostream_formatter {};
#endif

namespace crimson::os::seastore {

/**
 * operate_left
 *
 * Proceed overwrite_plan.left_operation.
 */
struct operate_ret_bare_t {
  std::optional<extent_to_write_t> op = std::nullopt;
  std::optional<bufferptr> bp = std::nullopt;
  std::optional<extent_to_write_t> split_op = std::nullopt;
  std::optional<extent_to_write_t> shadow_op = std::nullopt;
};
using operate_ret = get_iertr::future<operate_ret_bare_t>;
operate_ret operate_left(context_t ctx, LBAMappingRef &pin, const overwrite_plan_t &overwrite_plan)
{
  if (overwrite_plan.get_left_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare_t>();
  }

  auto res = operate_ret_bare_t();

  if (overwrite_plan.left_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin->get_val().is_zero());
    auto zero_extent_len = overwrite_plan.get_left_extent_size();
    assert_aligned(zero_extent_len);
    auto zero_prepend_len = overwrite_plan.get_left_alignment_size();

    if (zero_extent_len != 0) {
      res.op.emplace(extent_to_write_t::create_zero(
           overwrite_plan.pin_begin, zero_extent_len));
    }

    if (zero_prepend_len != 0) {
      res.bp.emplace(bufferptr(
          ceph::buffer::create(zero_prepend_len, 0)));
    }

    return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
  } else if (overwrite_plan.left_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto prepend_len = overwrite_plan.get_left_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
    } else {
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([prepend_len, res=std::move(res)](auto left_extent) mutable {
	res.bp.emplace(bufferptr(
	    left_extent->get_bptr(),
            0,
            prepend_len));
        return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
      });
    }
  } else {
    assert(overwrite_plan.left_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_left_extent_size();
    assert(extent_len && extent_len < pin->get_length());

    if (overwrite_plan.left_indirect_key != L_ADDR_NULL) {
      res.split_op.emplace(extent_to_write_t::create_split(
        overwrite_plan.left_indirect_key,
        extent_len,
        pin->get_length() - extent_len));
    }

    res.op.emplace(extent_to_write_t::create_existing(
          overwrite_plan.pin_begin,
          overwrite_plan.left_paddr,
          extent_len,
          overwrite_plan.left_indirect_key));

    if (overwrite_plan.shadow_left_paddr != P_ADDR_NULL) {
      assert(overwrite_plan.shadow_left_paddr.is_absolute());
      res.shadow_op.emplace(extent_to_write_t::create_shadow_existing(
	  overwrite_plan.pin_begin,
          overwrite_plan.shadow_left_paddr,
          extent_len));
    }

    auto prepend_len = overwrite_plan.get_left_alignment_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
    } else {
      auto prepend_offset = extent_len;
      return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, pin->duplicate()
      ).si_then([prepend_offset, prepend_len, res=std::move(res)]
                (auto left_extent) mutable {
        res.bp.emplace(bufferptr(
          left_extent->get_bptr(),
          prepend_offset,
          prepend_len));
        return get_iertr::make_ready_future<
          operate_ret_bare_t>(std::move(res));
      });
    }
  }
}

/**
 * operate_right
 *
 * Proceed overwrite_plan.right_operation.
 */
operate_ret operate_right(context_t ctx, LBAMappingRef &pin, const overwrite_plan_t &overwrite_plan)
{
  auto res = operate_ret_bare_t();

  if (overwrite_plan.get_right_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
  }

  auto right_pin_begin = pin->get_key();
  assert(overwrite_plan.data_end >= right_pin_begin);
  if (overwrite_plan.right_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin->get_val().is_zero());
    auto zero_suffix_len = overwrite_plan.get_right_alignment_size();
    auto zero_extent_len = overwrite_plan.get_right_extent_size();
    assert_aligned(zero_extent_len);

    if (zero_extent_len != 0) {
      res.op.emplace(extent_to_write_t::create_zero(
          overwrite_plan.aligned_data_end, zero_extent_len));
    }

    if (zero_suffix_len != 0) {
      res.bp.emplace(bufferptr(
          ceph::buffer::create(zero_suffix_len, 0)));
    }

    return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
  } else if (overwrite_plan.right_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto append_len = overwrite_plan.get_right_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
    } else {
      auto append_offset = overwrite_plan.data_end - right_pin_begin;
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([append_offset, append_len, res=std::move(res)](auto right_extent) mutable {
	res.bp.emplace(bufferptr(
            right_extent->get_bptr(),
            append_offset,
            append_len));
        return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
      });
    }
  } else {
    assert(overwrite_plan.right_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_right_extent_size();
    assert(extent_len);

    if (overwrite_plan.right_indirect_key != L_ADDR_NULL) {
      res.split_op.emplace(extent_to_write_t::create_split(
        overwrite_plan.right_indirect_key,
        overwrite_plan.aligned_data_end - right_pin_begin,
        extent_len));
    }

    auto right_indirect_key = L_ADDR_NULL;
    if (overwrite_plan.right_indirect_key != L_ADDR_NULL) {
      right_indirect_key = overwrite_plan.right_indirect_key
        + overwrite_plan.aligned_data_end - right_pin_begin;
    }
    res.op.emplace(extent_to_write_t::create_existing(
          overwrite_plan.aligned_data_end,
          overwrite_plan.right_paddr.add_offset(
            overwrite_plan.aligned_data_end - right_pin_begin),
          extent_len,
          right_indirect_key));
    if (overwrite_plan.shadow_right_paddr != P_ADDR_NULL) {
      assert(overwrite_plan.shadow_right_paddr.is_absolute());
      res.shadow_op.emplace(extent_to_write_t::create_shadow_existing(
	  overwrite_plan.aligned_data_end,
          overwrite_plan.shadow_right_paddr.add_offset(
	    overwrite_plan.aligned_data_end - right_pin_begin),
          extent_len));
    }

    auto append_len = overwrite_plan.get_right_alignment_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare_t>(std::move(res));
    } else {
      auto append_offset = overwrite_plan.data_end - right_pin_begin;
      return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, pin->duplicate()
      ).si_then([append_offset, append_len,res=std::move(res)]
                (auto right_extent) mutable {
        res.bp.emplace(bufferptr(
          right_extent->get_bptr(),
          append_offset,
          append_len));
        return get_iertr::make_ready_future<
          operate_ret_bare_t>(std::move(res));
      });
    }
  }
}

template <typename F>
auto with_object_data(
  ObjectDataHandler::context_t ctx,
  F &&f)
{
  return seastar::do_with(
    ctx.onode.get_layout().object_data.get(),
    std::forward<F>(f),
    [ctx](auto &object_data, auto &f) {
      return std::invoke(f, object_data
      ).si_then([ctx, &object_data] {
	if (object_data.must_update()) {
	  ctx.onode.get_mutable_layout(ctx.t).object_data.update(object_data);
	}
	return seastar::now();
      });
    });
}

template <typename F>
auto with_objects_data(
  ObjectDataHandler::context_t ctx,
  F &&f)
{
  ceph_assert(ctx.d_onode);
  return seastar::do_with(
    ctx.onode.get_layout().object_data.get(),
    ctx.d_onode->get_layout().object_data.get(),
    std::forward<F>(f),
    [ctx](auto &object_data, auto &d_object_data, auto &f) {
      return std::invoke(f, object_data, d_object_data
      ).si_then([ctx, &object_data, &d_object_data] {
	if (object_data.must_update()) {
	  ctx.onode.get_mutable_layout(ctx.t).object_data.update(object_data);
	}
	if (d_object_data.must_update()) {
	  ctx.d_onode->get_mutable_layout(
	    ctx.t).object_data.update(d_object_data);
	}
	return seastar::now();
      });
    });
}

ObjectDataHandler::write_ret ObjectDataHandler::prepare_data_reservation(
  context_t ctx,
  object_data_t &object_data,
  extent_len_t size)
{
  LOG_PREFIX(ObjectDataHandler::prepare_data_reservation);
  ceph_assert(size <= max_object_size);
  if (!object_data.is_null()) {
    ceph_assert(object_data.get_reserved_data_len() == max_object_size);
    DEBUGT("reservation present: {}~{}",
           ctx.t,
           object_data.get_reserved_data_base(),
           object_data.get_reserved_data_len());
    return write_iertr::now();
  } else {
    DEBUGT("reserving: {}~{}",
           ctx.t,
           ctx.onode.get_data_hint(),
           max_object_size);
    return ctx.tm.reserve_region(
      ctx.t,
      ctx.onode.get_data_hint(),
      max_object_size
    ).si_then([max_object_size=max_object_size, &object_data](auto pin) {
      ceph_assert(pin->get_length() == max_object_size);
      object_data.update_reserved(
	pin->get_key(),
	pin->get_length());
      return write_iertr::now();
    });
  }
}

ObjectDataHandler::clear_ret ObjectDataHandler::trim_data_reservation(
  context_t ctx, object_data_t &object_data, extent_len_t size)
{
  ceph_assert(!object_data.is_null());
  ceph_assert(size <= object_data.get_reserved_data_len());
  return seastar::do_with(
    lba_pin_list_t(),
    extent_to_write_list_t(),
    [ctx, size, &object_data](auto &pins, auto &to_write) {
      LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
      DEBUGT("object_data: {}~{}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len());
      return ctx.tm.get_pins(
	ctx.t,
	object_data.get_reserved_data_base() + size,
	object_data.get_reserved_data_len() - size
      ).si_then([ctx, size, &pins, &object_data, &to_write](auto _pins) {
	_pins.swap(pins);
	ceph_assert(pins.size());
	if (!size) {
	  // no need to reserve region if we are truncating the object's
	  // size to 0
	  return clear_iertr::now();
	}
	auto &pin = *pins.front();
	ceph_assert(pin.get_key() >= object_data.get_reserved_data_base());
	ceph_assert(
	  pin.get_key() <= object_data.get_reserved_data_base() + size);
	auto pin_offset = pin.get_key() -
	  object_data.get_reserved_data_base();
	if ((pin.get_key() == (object_data.get_reserved_data_base() + size)) ||
	  (pin.get_val().is_zero())) {
	  /* First pin is exactly at the boundary or is a zero pin.  Either way,
	   * remove all pins and add a single zero pin to the end. */
	  to_write.push_back(extent_to_write_t::create_zero(
	    pin.get_key(),
	    object_data.get_reserved_data_len() - pin_offset));
	  return clear_iertr::now();
	} else {
	  /* First pin overlaps the boundary and has data, read in extent
	   * and rewrite portion prior to size */
	  return ctx.tm.read_pin<ObjectDataBlock>(
	    ctx.t,
	    pin.duplicate()
	  ).si_then([ctx, size, pin_offset, &pin, &object_data, &to_write](
		     auto extent) {
	    bufferlist bl;
	    bl.append(
	      bufferptr(
		extent->get_bptr(),
		0,
		size - pin_offset
	      ));
	    bl.append_zero(p2roundup(size, ctx.tm.get_block_size()) - size);
	    to_write.push_back(extent_to_write_t::create_data(
	      pin.get_key(),
	      bl));
	    to_write.push_back(extent_to_write_t::create_zero(
	      object_data.get_reserved_data_base() +
                p2roundup(size, ctx.tm.get_block_size()),
	      object_data.get_reserved_data_len() -
                p2roundup(size, ctx.tm.get_block_size())));
	    return clear_iertr::now();
	  });
	}
      }).si_then([ctx, &pins] {
	return do_removals(ctx, pins);
      }).si_then([ctx, &to_write] {
	return do_insertions(ctx, to_write);
      }).si_then([size, &object_data] {
	if (size == 0) {
	  object_data.clear();
	}
	return ObjectDataHandler::clear_iertr::now();
      });
    });
}

/**
 * get_to_writes_with_zero_buffer
 *
 * Returns extent_to_write_t's reflecting a zero region extending
 * from offset~len with headptr optionally on the left and tailptr
 * optionally on the right.
 */
extent_to_write_list_t get_to_writes_with_zero_buffer(
  const extent_len_t block_size,
  laddr_t offset, extent_len_t len,
  std::optional<bufferptr> &&headptr, std::optional<bufferptr> &&tailptr)
{
  auto zero_left = p2roundup(offset, (laddr_t)block_size);
  auto zero_right = p2align(offset + len, (laddr_t)block_size);
  auto left = headptr ? (offset - headptr->length()) : offset;
  auto right = tailptr ?
    (offset + len + tailptr->length()) :
    (offset + len);

  assert(
    (headptr && ((zero_left - left) ==
		 p2roundup(headptr->length(), block_size))) ^
    (!headptr && (zero_left == left)));
  assert(
    (tailptr && ((right - zero_right) ==
		 p2roundup(tailptr->length(), block_size))) ^
    (!tailptr && (right == zero_right)));

  assert(right > left);
  assert((left % block_size) == 0);
  assert((right % block_size) == 0);

  // zero region too small for a reserved section,
  // headptr and tailptr in same extent
  if (zero_right <= zero_left) {
    bufferlist bl;
    if (headptr) {
      bl.append(*headptr);
    }
    bl.append_zero(
      right - left - bl.length() - (tailptr ? tailptr->length() : 0));
    if (tailptr) {
      bl.append(*tailptr);
    }
    assert(bl.length() % block_size == 0);
    assert(bl.length() == (right - left));
    return {extent_to_write_t::create_data(left, bl)};
  } else {
    // reserved section between ends, headptr and tailptr in different extents
    extent_to_write_list_t ret;
    if (headptr) {
      bufferlist headbl;
      headbl.append(*headptr);
      headbl.append_zero(zero_left - left - headbl.length());
      assert(headbl.length() % block_size == 0);
      assert(headbl.length() > 0);
      ret.push_back(extent_to_write_t::create_data(left, headbl));
    }
    // reserved zero region
    ret.push_back(extent_to_write_t::create_zero(zero_left, zero_right - zero_left));
    assert(ret.back().len % block_size == 0);
    assert(ret.back().len > 0);
    if (tailptr) {
      bufferlist tailbl;
      tailbl.append(*tailptr);
      tailbl.append_zero(right - zero_right - tailbl.length());
      assert(tailbl.length() % block_size == 0);
      assert(tailbl.length() > 0);
      ret.push_back(extent_to_write_t::create_data(zero_right, tailbl));
    }
    return ret;
  }
}

/**
 * get_to_writes
 *
 * Returns extent_to_write_t's from bl.
 *
 * TODO: probably add some kind of upper limit on extent size.
 */
extent_to_write_list_t get_to_writes(laddr_t offset, bufferlist &bl)
{
  auto ret = extent_to_write_list_t();
  ret.push_back(extent_to_write_t::create_data(offset, bl));
  return ret;
};

ObjectDataHandler::write_ret ObjectDataHandler::overwrite(
  context_t ctx,
  laddr_t offset,
  extent_len_t len,
  std::optional<bufferlist> &&bl,
  lba_pin_list_t &&_pins)
{
  if (bl.has_value()) {
    assert(bl->length() == len);
  }
  overwrite_plan_t overwrite_plan(offset, len, _pins, ctx.tm.get_block_size());
  using split_points = std::list<split_info_t>;
  return seastar::do_with(
    std::move(_pins),
    extent_to_write_list_t(),
    std::map<laddr_t, split_points>(),
    [ctx, len, offset, overwrite_plan, bl=std::move(bl)]
    (auto &pins, auto &to_write, auto &split_ops) mutable
  {
    LOG_PREFIX(ObjectDataHandler::overwrite);
    DEBUGT("overwrite: {}~{}",
           ctx.t,
           offset,
           len);
    ceph_assert(pins.size() >= 1);
    DEBUGT("overwrite: split overwrite_plan {}", ctx.t, overwrite_plan);

    return operate_left(
      ctx,
      pins.front(),
      overwrite_plan
    ).si_then([ctx, len, offset, overwrite_plan, bl=std::move(bl),
               &to_write, &split_ops, &pins](auto p) mutable {
      if (auto &left_extent = p.op; left_extent) {
        ceph_assert(left_extent->addr == overwrite_plan.pin_begin);
	if (left_extent->is_existing()) {
	  ceph_assert(left_extent->addr == pins.front()->get_key());
	  LOG_PREFIX(ObjectDataHandler::overwrite);
	  TRACET("splitting left extent {}", ctx.t, *pins.front());
	  split_ops[left_extent->addr].emplace_back(
	    pins.front()->get_val(),
	    left_extent->len,
	    pins.front()->get_length() - left_extent->len,
	    drop_part_t::RIGHT,
	    pins.front()->is_indirect());
	} else {
	  append_extent_to_write(to_write, std::move(*left_extent));
	}
      }
      if (auto &headptr = p.bp; headptr) {
        assert(headptr->length() > 0);
      }

      if (auto &split_op = p.split_op; split_op) {
        assert(split_op->is_split());
        split_ops[split_op->addr].emplace_back(
	  pins.front()->get_val(),
	  split_op->len,
	  split_op->right_len,
	  drop_part_t::NONE);
      }

      if (auto &shadow_left_extent = p.shadow_op; shadow_left_extent) {
        ceph_assert(shadow_left_extent->existing_paddr &&
		    *shadow_left_extent->existing_paddr ==
		    overwrite_plan.shadow_left_paddr);
        append_extent_to_write(to_write, std::move(*shadow_left_extent));
      }
      auto iter = pins.rbegin();
      if (auto &pin = *iter;
	  pin->is_shadow_mapping()) {
	iter++;
	ceph_assert(iter != pins.rend());
      }
      return operate_right(
        ctx,
        *iter,
        overwrite_plan
      ).si_then([ctx, len, offset, &pins,
                 pin_begin=overwrite_plan.pin_begin,
                 pin_end=overwrite_plan.pin_end,
                 bl=std::move(bl), left_p=std::move(p),
                 &to_write, &split_ops](auto p) mutable {
        if (bl.has_value()) {
          auto write_offset = offset;
          bufferlist write_bl;
          if (auto &headptr = left_p.bp; headptr) {
            write_bl.append(*headptr);
            write_offset -= headptr->length();
            assert_aligned(write_offset);
          }
          write_bl.claim_append(*bl);
          if (auto tailptr = p.bp; tailptr) {
            write_bl.append(*tailptr);
            assert_aligned(write_bl.length());
          }
          assert_aligned(write_bl.length());
          splice_extent_to_write(
            to_write,
            get_to_writes(write_offset, write_bl));
        } else {
          splice_extent_to_write(
            to_write,
            get_to_writes_with_zero_buffer(
              ctx.tm.get_block_size(),
              offset,
              len,
              std::move(left_p.bp),
              std::move(p.bp)));
        }

        if (auto &right_extent = p.op; right_extent) {
          ceph_assert(right_extent->get_end_addr() == pin_end);
	  if (right_extent->is_existing()) {
	    assert(pins.back()->get_length() > right_extent->len);
	    LOG_PREFIX(ObjectDataHandler::overwrite);
	    TRACET("splitting right extent {}", ctx.t, *pins.back());
	    split_ops[pins.back()->get_key()].emplace_back(
	      pins.back()->get_val(),
	      pins.back()->get_length() - right_extent->len,
	      right_extent->len,
	      drop_part_t::LEFT,
	      pins.back()->is_indirect());
	  } else {
	    append_extent_to_write(to_write, std::move(*right_extent));
	  }
        }
	if (auto shadow_right_extent = p.shadow_op; shadow_right_extent) {
	  append_extent_to_write(to_write, std::move(*shadow_right_extent));
	}
        if (auto &split_op = p.split_op; split_op) {
          assert(split_op->is_split());
          split_ops[split_op->addr].emplace_back(
	    pins.back()->get_val(),
	    split_op->len,
	    split_op->right_len,
	    drop_part_t::NONE);
        }
        assert(to_write.size());
        return do_split(ctx, split_ops, pins);
      }).si_then([ctx, &pins] {
        return do_removals(ctx, pins);
      }).si_then([ctx, &to_write] {
        return do_insertions(ctx, to_write);
      });
    });
  });
}

ObjectDataHandler::zero_ret ObjectDataHandler::zero(
  context_t ctx,
  objaddr_t offset,
  extent_len_t len)
{
  return with_object_data(
    ctx,
    [this, ctx, offset, len](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::zero);
      DEBUGT("zero to {}~{}, object_data: {}~{}, is_null {}",
             ctx.t,
             offset,
             len,
             object_data.get_reserved_data_base(),
             object_data.get_reserved_data_len(),
             object_data.is_null());
      return prepare_data_reservation(
	ctx,
	object_data,
	p2roundup(offset + len, ctx.tm.get_block_size())
      ).si_then([this, ctx, offset, len, &object_data] {
	auto logical_offset = object_data.get_reserved_data_base() + offset;
	return ctx.tm.get_pins(
	  ctx.t,
	  logical_offset,
	  len
	).si_then([this, ctx, logical_offset, len](auto pins) {
	  return overwrite(
	    ctx, logical_offset, len,
	    std::nullopt, std::move(pins));
	});
      });
    });
}

ObjectDataHandler::write_ret ObjectDataHandler::write(
  context_t ctx,
  objaddr_t offset,
  const bufferlist &bl)
{
  return with_object_data(
    ctx,
    [this, ctx, offset, &bl](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::write);
      DEBUGT("writing to {}~{}, object_data: {}~{}, is_null {}",
             ctx.t,
             offset,
	     bl.length(),
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len(),
             object_data.is_null());
      return prepare_data_reservation(
	ctx,
	object_data,
	p2roundup(offset + bl.length(), ctx.tm.get_block_size())
      ).si_then([this, ctx, offset, &object_data, &bl] {
	auto logical_offset = object_data.get_reserved_data_base() + offset;
	return ctx.tm.get_pins(
	  ctx.t,
	  logical_offset,
	  bl.length()
	).si_then([this, ctx,logical_offset, &bl](
		   auto pins) {
	  return overwrite(
	    ctx, logical_offset, bl.length(),
	    bufferlist(bl), std::move(pins));
	});
      });
    });
}

ObjectDataHandler::read_ret ObjectDataHandler::read(
  context_t ctx,
  objaddr_t obj_offset,
  extent_len_t len)
{
  return seastar::do_with(
    bufferlist(),
    [ctx, obj_offset, len](auto &ret) {
      return with_object_data(
	ctx,
	[ctx, obj_offset, len, &ret](const auto &object_data) {
	  LOG_PREFIX(ObjectDataHandler::read);
	  DEBUGT("reading {}~{}",
		 ctx.t,
		 object_data.get_reserved_data_base(),
		 object_data.get_reserved_data_len());
	  /* Assumption: callers ensure that onode size is <= reserved
	   * size and that len is adjusted here prior to call */
	  ceph_assert(!object_data.is_null());
	  ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
	  ceph_assert(len > 0);
	  laddr_t loffset =
	    object_data.get_reserved_data_base() + obj_offset;
	  return ctx.tm.get_pins(
	    ctx.t,
	    loffset,
	    len
	  ).si_then([ctx, loffset, len, &ret](auto _pins) {
	    // offset~len falls within reserved region and len > 0
	    ceph_assert(_pins.size() >= 1);
	    ceph_assert((*_pins.begin())->get_key() <= loffset);
	    return seastar::do_with(
	      std::move(_pins),
	      loffset,
	      [ctx, loffset, len, &ret](auto &pins, auto &current) {
		return trans_intr::do_for_each(
		  pins,
		  [ctx, loffset, len, &current, &ret](auto &pin)
		  -> read_iertr::future<> {
		    if (pin->is_shadow_mapping()) {
		      return seastar::now();
		    }
		    ceph_assert(current <= (loffset + len));
		    ceph_assert(
		      (loffset + len) > pin->get_key());
		    laddr_t end = std::min(
		      pin->get_key() + pin->get_length(),
		      loffset + len);
		    if (pin->get_val().is_zero()) {
		      LOG_PREFIX(ObjectDataHandler::read);
		      DEBUGT("reading {}~{}, zero",
			ctx.t,
			pin->get_key(),
			pin->get_length());
		      ceph_assert(end > current); // See LBAManager::get_mappings
		      ret.append_zero(end - current);
		      current = end;
		      return seastar::now();
		    } else {
		      auto key = pin->get_key();
		      bool is_indirect = pin->is_indirect();
		      LOG_PREFIX(ObjectDataHandler::read);
		      DEBUGT("reading {}~{}, indirect: {}",
			ctx.t,
			key,
			pin->get_length(),
			is_indirect);
		      return ctx.tm.read_pin<ObjectDataBlock>(
			ctx.t,
			std::move(pin)
		      ).si_then([&ret, &current, end, key, is_indirect](auto extent) {
			ceph_assert(
			  is_indirect
			    ? (key + extent->get_length()) >= end
			    : (extent->get_laddr() + extent->get_length()) >= end);
			ceph_assert(end > current);
			ret.append(
			  bufferptr(
			    extent->get_bptr(),
			    current - (is_indirect ? key : extent->get_laddr()),
			    end - current));
			current = end;
			return seastar::now();
		      }).handle_error_interruptible(
			read_iertr::pass_further{},
			crimson::ct_error::assert_all{
			  "ObjectDataHandler::read hit invalid error"
			}
		      );
		    }
		  });
	      });
	  });
	}).si_then([&ret] {
	  return std::move(ret);
	});
    });
}

ObjectDataHandler::fiemap_ret ObjectDataHandler::fiemap(
  context_t ctx,
  objaddr_t obj_offset,
  extent_len_t len)
{
  return seastar::do_with(
    std::map<uint64_t, uint64_t>(),
    [ctx, obj_offset, len](auto &ret) {
    return with_object_data(
      ctx,
      [ctx, obj_offset, len, &ret](const auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::fiemap);
      DEBUGT(
	"{}~{}, reservation {}~{}",
        ctx.t,
        obj_offset,
        len,
        object_data.get_reserved_data_base(),
        object_data.get_reserved_data_len());
      /* Assumption: callers ensure that onode size is <= reserved
       * size and that len is adjusted here prior to call */
      ceph_assert(!object_data.is_null());
      ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
      ceph_assert(len > 0);
      laddr_t loffset =
        object_data.get_reserved_data_base() + obj_offset;
      return ctx.tm.get_pins(
        ctx.t,
        loffset,
        len
      ).si_then([loffset, len, &object_data, &ret](auto &&pins) {
	ceph_assert(pins.size() >= 1);
        ceph_assert((*pins.begin())->get_key() <= loffset);
	for (auto &&i: pins) {
	  if (!(i->get_val().is_zero())) {
	    auto ret_left = std::max(i->get_key(), loffset);
	    auto ret_right = std::min(
	      i->get_key() + i->get_length(),
	      loffset + len);
	    assert(ret_right > ret_left);
	    ret.emplace(
	      std::make_pair(
		ret_left - object_data.get_reserved_data_base(),
		ret_right - ret_left
	      ));
	  }
	}
      });
    }).si_then([&ret] {
      return std::move(ret);
    });
  });
}

ObjectDataHandler::truncate_ret ObjectDataHandler::truncate(
  context_t ctx,
  objaddr_t offset)
{
  return with_object_data(
    ctx,
    [this, ctx, offset](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::truncate);
      DEBUGT("truncating {}~{} offset: {}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len(),
	     offset);
      if (offset < object_data.get_reserved_data_len()) {
	return trim_data_reservation(ctx, object_data, offset);
      } else if (offset > object_data.get_reserved_data_len()) {
	return prepare_data_reservation(
	  ctx,
	  object_data,
	  p2roundup(offset, ctx.tm.get_block_size()));
      } else {
	return truncate_iertr::now();
      }
    });
}

ObjectDataHandler::clear_ret ObjectDataHandler::clear(
  context_t ctx)
{
  return with_object_data(
    ctx,
    [this, ctx](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::clear);
      DEBUGT("clearing: {}~{}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len());
      if (object_data.is_null()) {
	return clear_iertr::now();
      }
      return trim_data_reservation(ctx, object_data, 0);
    });
}

ObjectDataHandler::clone_ret ObjectDataHandler::clone_extents(
  context_t ctx,
  object_data_t &object_data,
  lba_pin_list_t &pins,
  laddr_t data_base)
{
  LOG_PREFIX(ObjectDataHandler::clone_extents);
  TRACET(" object_data: {}~{}, data_base: {}",
    ctx.t,
    object_data.get_reserved_data_base(),
    object_data.get_reserved_data_len(),
    data_base);
  return ctx.tm.dec_ref(
    ctx.t,
    object_data.get_reserved_data_base()
  ).si_then(
    [&pins, &object_data, ctx, data_base](auto) mutable {
      return seastar::do_with(
	(extent_len_t)0,
	[&object_data, ctx, data_base, &pins](auto &last_pos) {
	return trans_intr::do_for_each(
	  pins,
	  [&last_pos, &object_data, ctx, data_base](auto &pin) {
	  auto offset = pin->get_key() - data_base;
	  auto fut = TransactionManager::reserve_extent_iertr
	    ::make_ready_future<LBAMappingRef>();
	  if (offset != last_pos) {
	    ceph_assert(last_pos < offset);
	    fut = ctx.tm.reserve_region(
	      ctx.t,
	      object_data.get_reserved_data_base() + last_pos,
	      offset - last_pos);
	  }
	  return fut.si_then(
	    [&pin, ctx, &object_data,
	    &last_pos, offset](auto) {
	    return ctx.tm.clone_extent(
	      ctx.t,
	      object_data.get_reserved_data_base() + offset,
	      pin->is_indirect()
		? pin->get_intermediate_key()
		: pin->get_key(),
	      pin->get_length(),
	      pin->get_val()
	    ).si_then([&last_pos, &pin, offset](auto) {
	      last_pos = offset + pin->get_length();
	      return seastar::now();
	    }).handle_error_interruptible(
	      crimson::ct_error::input_output_error::pass_further(),
	      crimson::ct_error::assert_all("not possible")
	    );
	  });
	}).si_then([&last_pos, &object_data, ctx] {
	  if (last_pos != object_data.get_reserved_data_len()) {
	    return ctx.tm.reserve_region(
	      ctx.t,
	      object_data.get_reserved_data_base() + last_pos,
	      object_data.get_reserved_data_len() - last_pos
	    ).si_then([](auto) {
	      return seastar::now();
	    });
	  }
	  return TransactionManager::reserve_extent_iertr::now();
	});
      });
    },
    ObjectDataHandler::write_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "object_data_handler::clone invalid error"
    }
  );
}

ObjectDataHandler::clone_ret ObjectDataHandler::clone(
  context_t ctx)
{
  // the whole clone procedure can be seperated into the following steps:
  // 	1. let clone onode(d_object_data) take the head onode's
  // 	   object data base;
  // 	2. reserve a new region in lba tree for the head onode;
  // 	3. clone all extents of the clone onode, see transaction_manager.h
  // 	   for the details of clone_extent;
  // 	4. reserve the space between the head onode's size and its reservation
  // 	   length.
  return with_objects_data(
    ctx,
    [ctx, this](auto &object_data, auto &d_object_data) {
    ceph_assert(d_object_data.is_null());
    return prepare_data_reservation(
      ctx,
      d_object_data,
      object_data.get_reserved_data_len()
    ).si_then([&object_data, &d_object_data, ctx, this] {
      auto base = object_data.get_reserved_data_base();
      auto len = object_data.get_reserved_data_len();
      object_data.clear();
      LOG_PREFIX(ObjectDataHandler::clone);
      DEBUGT("cloned obj reserve_data_base: {}, len {}",
	ctx.t,
	d_object_data.get_reserved_data_base(),
	d_object_data.get_reserved_data_len());
      return prepare_data_reservation(
	ctx,
	object_data,
	d_object_data.get_reserved_data_len()
      ).si_then([&d_object_data, ctx, &object_data, base, len, this] {
	LOG_PREFIX("ObjectDataHandler::clone");
	DEBUGT("head obj reserve_data_base: {}, len {}",
	  ctx.t,
	  object_data.get_reserved_data_base(),
	  object_data.get_reserved_data_len());
	return ctx.tm.get_pins(ctx.t, base, len
	).si_then([ctx, &object_data, &d_object_data, base, this](auto pins) {
	  return seastar::do_with(
	    std::move(pins),
	    [ctx, &object_data, &d_object_data, base, this](auto &pins) {
	    return clone_extents(ctx, object_data, pins, base
	    ).si_then([ctx, &d_object_data, base, &pins, this] {
	      return clone_extents(ctx, d_object_data, pins, base);
	    }).si_then([&pins, ctx] {
	      return do_removals(ctx, pins);
	    });
	  });
	});
      });
    });
  });
}

} // namespace crimson::os::seastore
