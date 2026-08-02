//! The cross-DSO format-driver ABI — this crate's `adbc.h`. A connector library invokes a format
//! library it never links against: the format's Java facade hands the connector the address of one
//! exported init function (the ADBC driver-manager pattern), the connector calls it with the ABI
//! version it was compiled to speak, and the format either fills the requested vtable or refuses.
//! Everything crossing the boundary is C: this struct, function pointers, opaque `i64` handles, and
//! Arrow C Data addresses whose release callbacks carry buffer ownership back into the library that
//! allocated them (divergences/25). A refusal (or an absent init) is a graceful fallback to the
//! JVM-mediated decode, so mixed-version deployments lose speed, never correctness.
//!
//! Bump [`FORMAT_DRIVER_VERSION_1`]-style constants and extend [`FormatDriver`] only additively;
//! any incompatible change gets a new version constant that old drivers will refuse.

/// Decodes one binary body batch (Arrow C Data in) into a typed batch (Arrow C Data out) with an
/// opaque decoder handle created and owned by the format's Java facade. Returns 0 on success.
pub(crate) type DecodeBodyBatch = extern "C" fn(
    decoder_handle: i64,
    in_array_address: i64,
    in_schema_address: i64,
    out_array_address: i64,
    out_schema_address: i64,
) -> i32;

/// Returns the UTF-8 error text of the calling thread's most recent failed [`DecodeBodyBatch`],
/// writing its byte length to `len_out` (null/0 when there is none). Failure-path only — a
/// successful decode neither writes nor invalidates it — and the pointer stays valid on the
/// calling thread until its next failed decode, so the connector must read it immediately after
/// the nonzero return, on the same thread.
pub(crate) type DecodeLastError =
    extern "C" fn(decoder_handle: i64, len_out: *mut i32) -> *const u8;

/// The driver vtable a format fills for a connector. Version 1 is the `decode_body_batch` prefix;
/// version 2 appends `decode_last_error`. Init fills only the requested version's prefix — a
/// version-1 caller's struct ends at the version-1 fields, so writing past them would trample its
/// stack.
#[repr(C)]
pub(crate) struct FormatDriver {
    pub(crate) decode_body_batch: DecodeBodyBatch,
    /// Version 2 and above; untouched by a version-1 init.
    pub(crate) decode_last_error: DecodeLastError,
}

/// Signature of the exported init: `streamfusion_format_driver_init(version, driver)`. The caller
/// states the version it wants and passes the matching vtable to fill; nonzero means unsupported.
pub(crate) type FormatDriverInit = extern "C" fn(version: i32, driver: *mut FormatDriver) -> i32;

/// ABI revision 1: `decode_body_batch` only; a failed decode reports just its nonzero code.
pub(crate) const FORMAT_DRIVER_VERSION_1: i32 = 1;

/// ABI revision 2: adds `decode_last_error`, the failure path's error-message channel. A
/// connector asks for this first and falls back to requesting version 1 from an older format.
pub(crate) const FORMAT_DRIVER_VERSION_2: i32 = 2;
