#[macro_use]
extern crate common;
#[macro_use]
extern crate fomat_macros;
extern crate futures;
extern crate fxhash;
#[macro_use]
extern crate gstuff;
extern crate lazy_static;
extern crate libc;
extern crate serde;
//extern crate serde_derive;
#[macro_use]
extern crate serde_json;
extern crate serde_bencode;
#[macro_use]
extern crate unwrap;

#[doc(hidden)]
pub mod tests;

use common::bits256;
use common::log::{StatusHandle, TagParam};
use common::mm_ctx::{from_ctx, MmArc};
use fxhash::FxHashMap;
use libc::{c_char, c_void};
use serde::Serialize;
use std::ffi::{CStr, CString};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Any unprocessed libtorrent alers are logged if this knob is set to `true`.
const LOG_UNHANDLED_ALERTS: bool = false;

// NB: C++ structures and functions are defined in "dht.cc".

#[repr(C)]
struct dugout_t {
    err: *const c_char,  // strdup of a C++ exception `what`.
    sett: *mut c_void,  // `lt::settings_pack*` (from C++ `new`).
    session: *mut c_void,  // `lt::session*` (from C++ `new`).
}
impl dugout_t {
    fn has_err (&self) -> Option<&str> {
        if !self.err.is_null() {
            let what = if let Ok (msg) = unsafe {CStr::from_ptr (self.err)} .to_str() {msg} else {"Non-unicode `what`"};
            Some (what)
        } else {
            None
        }
    }
}
impl Drop for dugout_t {
    fn drop (&mut self) {
        let err = unsafe {delete_dugout (self)};
        if !err.is_null() {
            let what = unwrap! (unsafe {CStr::from_ptr (err)} .to_str());
            log! ("delete_dugout error: " (what));
            unsafe {libc::free (err as *mut c_void);}
        }
    }
}

enum Alert {}

extern "C" {
    fn delete_dugout (dugout: *mut dugout_t) -> *const c_char;
    fn dht_init (listen_interfaces: *const c_char) -> dugout_t;
    fn enable_dht (dugout: *mut dugout_t);
    fn dht_alerts (dugout: *mut dugout_t, cb: extern fn (cbctx: *mut c_void, alert: *mut Alert), cbctx: *mut c_void);
    fn alert_message (alert: *const Alert) -> *const c_char;
    fn is_dht_bootstrap_alert (alert: *const Alert) -> bool;
    fn as_listen_succeeded_alert (alert: *const Alert) -> *const c_char;
    fn as_listen_failed_alert (alert: *const Alert) -> *const c_char;
}

/// The peer-to-peer and connectivity information local to the MM2 instance.
pub struct PeersContext {
    dht_thread: Mutex<Option<thread::JoinHandle<()>>>,
    sock2peer: Mutex<FxHashMap<i32, bits256>>
}

impl PeersContext {
    /// Obtains a reference to this crate context, creating it if necessary.
    pub fn from_ctx (ctx: &MmArc) -> Result<Arc<PeersContext>, String> {
        Ok (try_s! (from_ctx (&ctx.peers_ctx, move || {
            Ok (PeersContext {
                dht_thread: Mutex::new (None),
                sock2peer: Mutex::new (FxHashMap::default())
            })
        })))
    }
}

/// I've noticed that if we create a libtorrent session (`lt::session`) and destroy it right away
/// then it will often crash. Apparently we're catching it unawares during some initalization procedures.
/// This seems like a good enough reason to use a separate thread for managing the libtorrent,
/// allowing it to initialize and then stop at its own pace.
fn dht_thread (ctx: MmArc, _netid: u16, _our_public_key: bits256, preferred_port: u16) {
    let listen_interfaces = fomat! ("0.0.0.0:" (preferred_port) ",[::]:" (preferred_port));
    log! ("preferred_port: " (preferred_port) "; listen_interfaces: " (listen_interfaces));
    let listen_interfaces = unwrap! (CString::new (listen_interfaces));
    let mut dugout = unsafe {dht_init (listen_interfaces.as_ptr())};
    if let Some (err) = dugout.has_err() {
        // TODO: User-friendly log message (`LogState::log`).
        log! ("dht_init error: " (err));
        return
    }
       
    // Skip DHT bootstrapping if we're already stopping. But give libtorrent a bit of time first, just in case.
    if ctx.is_stopping() {thread::sleep (Duration::from_millis (200)); return}

    let mut bootstrap_status = ctx.log.status_handle();
    let status_tags: &[&TagParam] = &[&"dht-boot"];
    bootstrap_status.status (status_tags, "DHT bootstrap ...");
    unsafe {enable_dht (&mut dugout)};
    if let Some (err) = dugout.has_err() {
        bootstrap_status.status (status_tags, &fomat! ("DHT bootstrap error: " (err)));
        return
    }

    struct CbCtx<'a> {
        bootstrap_status: Option<StatusHandle<'a>>
    }
    let mut cbctx = CbCtx {
        bootstrap_status: Some (bootstrap_status)
    };

    loop {
        extern fn cb (cbctx: *mut c_void, alert: *mut Alert) {
            let cbctx = cbctx as *mut CbCtx;
            let cbctx: &mut CbCtx = unsafe {&mut *cbctx};

            if unsafe {is_dht_bootstrap_alert (alert)} {
                if let Some (status) = cbctx.bootstrap_status.take() {
                    status.append (" Done.")
                }
                return
            }

            // NB: Looks like libtorrent automatically tries the next port number when it can't bind on the `preferred_port`.
            let endpoint_cs = unsafe {as_listen_succeeded_alert (alert)};
            if !endpoint_cs.is_null() {
                let endpoint = unwrap! (unsafe {CStr::from_ptr (endpoint_cs)} .to_str());
                log! ("Listening on " (endpoint));
                unsafe {libc::free (endpoint_cs as *mut c_void)}
                return
            }

            let endpoint_cs = unsafe {as_listen_failed_alert (alert)};
            if !endpoint_cs.is_null() {
                let endpoint = unwrap! (unsafe {CStr::from_ptr (endpoint_cs)} .to_str());
                log! ("Can't listen on " (endpoint));
                return
            }

            if LOG_UNHANDLED_ALERTS {
                let cs = unsafe {alert_message (alert)};
                if let Ok (alert_message) = unsafe {CStr::from_ptr (cs)} .to_str() {log! ("libtorrent alert: " (alert_message))}
                unsafe {libc::free (cs as *mut c_void)}
            }
        }
        unsafe {dht_alerts (&mut dugout, cb, &mut cbctx as *mut CbCtx as *mut c_void)};
        if let Some (err) = dugout.has_err() {
            // TODO: User-friendly log message (`LogState::log`).
            log! ("dht_alerts error: " (err));
            return
        }

        if ctx.is_stopping() {break}
        thread::sleep (Duration::from_millis (50));
    }
}

/// * `netid` - We ignore the peers not matching the `netid`. Usually 0.
/// * `our_public_key` - Aka `LP_mypub25519`. This is our ID, allowing us to be different from other peers
///                      and to prove our identity (ownership of the corresponding private key) to a peer.
/// * `preferred_port` - We'll try to open an UDP endpoint on this port,
///                      which might help if the user configured this port in firewall and forwarding rules.
///                      We're not limited to this port though and might try other ports as well.
/// * `session_id` - Identifies our incarnation, allowing other peers to know if they're talking with the same instance.
pub fn initialize (ctx: &MmArc, netid: u16, our_public_key: bits256, preferred_port: u16, _session_id: u32) -> Result<(), String> {
    // NB: From the `fn test_trade` logs it looks like the `session_id` isn't shared with the peers currently.
    log! ("initialize] netid " (netid) " public key " (our_public_key) " preferred port " (preferred_port));
    if !our_public_key.nonz() {return ERR! ("No public key")}

    let pctx = try_s! (PeersContext::from_ctx (&ctx));
    *try_s! (pctx.dht_thread.lock()) =
        Some (try_s! (thread::Builder::new().name ("dht".into()) .spawn ({
            let ctx = ctx.clone();
            move || dht_thread (ctx, netid, our_public_key, preferred_port)
        })));
    ctx.on_stop ({
        let ctx = ctx.clone();
        let pctx = pctx.clone();
        Box::new (move || -> Result<(), String> {
            if let Ok (mut dht_thread) = pctx.dht_thread.lock() {
                if let Some (dht_thread) = dht_thread.take() {
                    let join_status = ctx.log.status (&[&"dht-stop"], "Waiting for the dht_thread to stop...");
                    let _ = dht_thread.join();
                    join_status.append (" Done.");
                }
            }
            Ok(())
        })
    });

    *try_s! (common::for_c::PEERS_SEND_COMPAT.lock()) = Some (peers_send_compat);
    *try_s! (common::for_c::PEERS_RECV_COMPAT.lock()) = Some (peers_recv_compact);

    Ok(())
}

/// Try to reach a peer and establish connectivity with it while knowing no more than its port and IP.
/// 
/// * `ip` - The public IP where the peer is supposedly listens for incoming connections.
/// * `preferred_port` - The preferred port of the peer.
pub fn investigate_peer (_ctx: &MmArc, ip: &str, preferred_port: u16) -> Result<(), String> {
    log! ("investigate_peer] ip " (ip) " preferred port " (preferred_port));
    Ok(())
}

/// Leave a message for the peer.
/// 
/// The message might be sent across a number of different delivery methods.
/// As of now we're going to send it via the Bittorrent DHT.
/// 
/// Delivery is not guaranteed (to check delivery we should manually get a reply from the peer).
/// 
/// * `to` - Recipient of the message (`LP_mypub25519` of the receiving MM2 instance).
/// * `payload` - Contents of the message.
pub fn send<T: Serialize> (_ctx: &MmArc, _to: bits256, _payload: &T) -> Result<(), String> {
    ERR! ("TBD")
}

/// Associate a nanomsg socket with a p2p `LP_mypub25519` identifier of the peer.
pub fn bind (ctx: &MmArc, sock: i32, peer: bits256) -> Result<(), String> {
    log! ("bind] sock " (sock) " = peer " (peer));
    let pctx = try_s! (PeersContext::from_ctx (ctx));
    let mut sock2peer = try_s! (pctx.sock2peer.lock());
    sock2peer.insert (sock, peer);
    Ok(())
}

/// TBD
/// 
/// * `ctx` - `MmCtx` handler.
/// * `sock` - 
/// * `` - 
/// * `` - 
/// 
/// Returns 0 if successfull, negative number if not.
fn peers_send_compat (ctx: u32, sock: i32, _data: *const u8, datalen: i32) -> i32 {
    if let Err (err) = (move || -> Result<(), String> {
        let ctx = try_s! (MmArc::from_ffi_handle (ctx));
        let pctx = try_s! (PeersContext::from_ctx (&ctx));
        let sock2peer = try_s! (pctx.sock2peer.lock());
        let peer = sock2peer.get (&sock);
        log! ("peers_send_compat] sock: " (sock) "; datalen: " (datalen) "; peer " if let Some (p) = peer {(p)} else {'-'});
        ERR! ("TBD")
    })() {log! ("peers_send_compat] error: " (err)); -1} else {0}
}

/// TBD
/// 
/// * `ctx` - `MmCtx` handler.
/// * `sock` - 
/// 
/// Returns the length of the `data` buffer allocated with `malloc`,
/// or `0` if no data was received,
/// or a negative number if there was an error.
fn peers_recv_compact (ctx: u32, sock: i32, _data: *mut *mut u8) -> i32 {
    match (move || -> Result<i32, String> {
        let ctx = try_s! (MmArc::from_ffi_handle (ctx));
        let pctx = try_s! (PeersContext::from_ctx (&ctx));
        let sock2peer = try_s! (pctx.sock2peer.lock());
        let peer = sock2peer.get (&sock);
        log! ("peers_recv_compact] sock: " (sock) "; peer " if let Some (p) = peer {(p)} else {'-'});
        ERR! ("TBD")
    })() {
        Ok (l) => l,
        Err (err) => {log! ("peers_recv_compact] error: " (err)); -1}
    }
}
