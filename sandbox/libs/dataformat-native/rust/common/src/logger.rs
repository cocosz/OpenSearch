/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Rust→Java logging via FFM callback.
//!
//! Java registers a function pointer at startup via `native_logger_init`.
//! Rust calls that pointer to log. No JNI.

use std::sync::atomic::{AtomicPtr, Ordering};

/// Callback signature: `void log(int level, const char* msg, long msg_len)`
type LogCallback = unsafe extern "C" fn(i32, *const u8, i64);

static LOG_CALLBACK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Error = 2,
}

/// A `log` crate backend that forwards to our native bridge callback.
/// This makes `log::info!()` from any crate (including liquid-cache)
/// appear in OpenSearch's log file via the Java RustLoggerBridge.
struct NativeBridgeLogger;

impl ::log::Log for NativeBridgeLogger {
    fn enabled(&self, metadata: &::log::Metadata) -> bool {
        metadata.level() <= ::log::Level::Info
    }

    fn log(&self, record: &::log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let level = match record.level() {
            ::log::Level::Error => LogLevel::Error,
            ::log::Level::Warn | ::log::Level::Info => LogLevel::Info,
            _ => LogLevel::Debug,
        };
        let msg = format!("{}", record.args());
        log(level, &msg);
    }

    fn flush(&self) {}
}

static NATIVE_BRIDGE_LOGGER: NativeBridgeLogger = NativeBridgeLogger;

/// Called by Java at startup to register the log callback.
#[no_mangle]
pub unsafe extern "C" fn native_logger_init(callback: LogCallback) {
    LOG_CALLBACK.store(callback as *mut (), Ordering::Release);
    // Initialize the `log` crate facade so that log::info!() from any
    // dependency (e.g. liquid-cache) routes through our native bridge.
    let _ = ::log::set_logger(&NATIVE_BRIDGE_LOGGER);
    ::log::set_max_level(::log::LevelFilter::Info);
    log(LogLevel::Info, "Native logger initialized successfully");
}

pub fn log(level: LogLevel, message: &str) {
    let ptr = LOG_CALLBACK.load(Ordering::Acquire);
    if ptr.is_null() {
        eprintln!("[RUST_LOG_FALLBACK] {:?}: {}", level, message);
        return;
    }
    let callback: LogCallback = unsafe { std::mem::transmute(ptr) };
    unsafe { callback(level as i32, message.as_ptr(), message.len() as i64) };
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Debug, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Info, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::Error, &format!($($arg)*))
    };
}
