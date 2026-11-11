#[cfg(feature = "systemd")]
pub fn notify_systemd(state: &str) {
    let Some(raw_socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let _ = send_to_notify_socket(&raw_socket, state.as_bytes());
}

#[cfg(not(feature = "systemd"))]
pub fn notify_systemd(_state: &str) {}

#[cfg(any(feature = "systemd", test))]
pub(crate) fn send_to_notify_socket(
    raw_socket: &std::ffi::OsStr,
    payload: &[u8],
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::{SocketAddr, UnixDatagram};
    use std::path::Path;

    let bytes = raw_socket.as_bytes();
    if bytes.is_empty() {
        return Ok(());
    }

    let sock = UnixDatagram::unbound()?;

    #[cfg(target_os = "linux")]
    if let Some(abstract_name) = bytes.strip_prefix(b"@") {
        use std::os::linux::net::SocketAddrExt;
        let addr = SocketAddr::from_abstract_name(abstract_name)?;
        let _ = sock.send_to_addr(payload, &addr)?;
        return Ok(());
    }

    let _ = sock.send_to(payload, Path::new(raw_socket))?;
    Ok(())
}
