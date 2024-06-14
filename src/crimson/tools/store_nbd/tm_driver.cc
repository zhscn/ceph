// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tm_driver.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

seastar::future<> TMDriver::write(
  off_t offset,
  bufferptr ptr)
{
  logger().debug("Writing offset {}", offset);
  assert(offset % device->get_block_size() == 0);
  assert((ptr.length() % device->get_block_size()) == 0);
  return seastar::do_with(ptr, [this, offset](auto& ptr) {
    return repeat_eagain([this, offset, &ptr] {
      return tm->with_transaction_intr(
        Transaction::src_t::MUTATE,
        "write",
        [this, offset, &ptr](auto& t)
      {
	auto laddr = laddr_t::get_hint_from_offset(offset);
        return tm->remove(t, laddr
        ).discard_result().handle_error_interruptible(
          crimson::ct_error::enoent::handle([](auto) { return seastar::now(); }),
          crimson::ct_error::pass_further_all{}
        ).si_then([this, laddr, &t, &ptr] {
          logger().debug("dec_ref complete");
          return tm->alloc_data_extents<TestBlock>(t, laddr, ptr.length());
        }).si_then([this, offset, &t, &ptr](auto extents) mutable {
	  boost::ignore_unused(offset);  // avoid clang warning;
	  auto off = offset;
	  auto left = ptr.length();
	  size_t written = 0;
	  for (auto &ext : extents) {
	    assert(ext->get_laddr().get_original_offset() == (size_t)off);
	    assert(ext->get_bptr().length() <= left);
	    ptr.copy_out(written, ext->get_length(), ext->get_bptr().c_str());
	    off += ext->get_length();
	    left -= ext->get_length();
	  }
	  assert(!left);
          logger().debug("submitting transaction");
          return tm->submit_transaction(t);
        });
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{"store-nbd write"}
  );
}

TMDriver::read_extents_ret TMDriver::read_extents(
  Transaction &t,
  off_t offset,
  extent_len_t length)
{
  return seastar::do_with(
    lba_pin_list_t(),
    lextent_list_t<TestBlock>(),
    [this, &t, offset, length](auto &pins, auto &ret) {
      auto laddr = laddr_t::get_hint_from_offset(offset);
      return tm->get_pins(
	t, laddr, length
      ).si_then([this, &t, &pins, &ret](auto _pins) {
	_pins.swap(pins);
	logger().debug("read_extents: mappings {}", pins);
	return trans_intr::do_for_each(
	  pins.begin(),
	  pins.end(),
	  [this, &t, &ret](auto &&pin) {
	    logger().debug(
	      "read_extents: get_extent {}~{}",
	      pin->get_val(),
	      pin->get_length());
	    return tm->read_pin<TestBlock>(
	      t,
	      std::move(pin)
	    ).si_then([&ret](auto ref) mutable {
	      ret.push_back(std::make_pair(ref->get_laddr(), ref));
	      logger().debug(
		"read_extents: got extent {}",
		*ref);
	      return seastar::now();
	    });
	  }).si_then([&ret] {
	    return std::move(ret);
	  });
      });
    });
}

seastar::future<bufferlist> TMDriver::read(
  off_t offset,
  size_t size)
{
  logger().debug("Reading offset {}", offset);
  assert(offset % device->get_block_size() == 0);
  assert(size % device->get_block_size() == 0);
  auto blptrret = std::make_unique<bufferlist>();
  auto &blret = *blptrret;
  return repeat_eagain([=, &blret, this] {
    return tm->with_transaction_intr(
      Transaction::src_t::READ,
      "read",
      [=, &blret, this](auto& t)
    {
      return read_extents(t, offset, size
      ).si_then([=, &blret](auto ext_list) {
        size_t cur = offset;
        for (auto &i: ext_list) {
	  auto offset = i.first.get_original_offset();
          if (cur != offset) {
            assert(cur < offset);
            blret.append_zero(offset - cur);
            cur = offset;
          }
          blret.append(i.second->get_bptr());
          cur += i.second->get_bptr().length();
        }
        if (blret.length() != size) {
          assert(blret.length() < size);
          blret.append_zero(size - blret.length());
        }
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{"store-nbd read"}
  ).then([blptrret=std::move(blptrret)]() mutable {
    logger().debug("read complete");
    return std::move(*blptrret);
  });
}

void TMDriver::init()
{
  std::vector<Device*> sec_devices;
#ifndef NDEBUG
  tm = make_transaction_manager(device.get(), sec_devices, true);
#else
  tm = make_transaction_manager(device.get(), sec_devices, false);
#endif
}

void TMDriver::clear()
{
  tm.reset();
}

size_t TMDriver::get_size() const
{
  return device->get_available_size() * .5;
}

seastar::future<> TMDriver::mkfs()
{
  assert(config.path);
  logger().debug("mkfs");
  return Device::make_device(*config.path, device_type_t::SSD
  ).then([this](DeviceRef dev) {
    device = std::move(dev);
    seastore_meta_t meta;
    meta.seastore_id.generate_random();
    return device->mkfs(
      device_config_t{
        true,
        (magic_t)std::rand(),
        device_type_t::SSD,
        0,
        meta,
        secondary_device_set_t()});
  }).safe_then([this] {
    logger().debug("device mkfs done");
    return device->mount();
  }).safe_then([this] {
    init();
    logger().debug("tm mkfs");
    return tm->mkfs();
  }).safe_then([this] {
    logger().debug("tm close");
    return tm->close();
  }).safe_then([this] {
    logger().debug("sm close");
    return device->close();
  }).safe_then([this] {
    clear();
    device.reset();
    logger().debug("mkfs complete");
    return TransactionManager::mkfs_ertr::now();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::mkfs"
    }
  );
}

seastar::future<> TMDriver::mount()
{
  return (config.mkfs ? mkfs() : seastar::now()
  ).then([this] {
    return Device::make_device(*config.path, device_type_t::SSD);
  }).then([this](DeviceRef dev) {
    device = std::move(dev);
    return device->mount();
  }).safe_then([this] {
    init();
    return tm->mount();
  }).safe_then([this] {
    tm->start_background();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::mount"
    }
  );
};

seastar::future<> TMDriver::close()
{
  return tm->close().safe_then([this] {
    clear();
    return device->close();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::close"
    }
  );
}
