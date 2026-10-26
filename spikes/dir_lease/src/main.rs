//! THROWAWAY spike: directory-fd flock as lease + openat/renameat-anchored writes.
//! Not for merge. See README.md.

use std::ffi::CString;
use std::fs::{File, TryLockError};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------- syscall helpers (the calls the daemon would use) ----------

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(-1)
}

fn ename(e: i32) -> String {
    let n = match e {
        libc::ENOENT => "ENOENT",
        libc::EWOULDBLOCK => "EWOULDBLOCK",
        libc::EBADF => "EBADF",
        libc::EISDIR => "EISDIR",
        libc::EACCES => "EACCES",
        libc::EPERM => "EPERM",
        libc::EEXIST => "EEXIST",
        libc::ENOTDIR => "ENOTDIR",
        libc::EINVAL => "EINVAL",
        _ => "?",
    };
    format!("{}({})", n, e)
}

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn cpath(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

fn write_all_fd(fd: RawFd, data: &[u8]) -> Result<(), i32> {
    let mut off = 0;
    while off < data.len() {
        let n = unsafe {
            libc::write(fd, data[off..].as_ptr() as *const libc::c_void, data.len() - off)
        };
        if n < 0 {
            let e = errno();
            if e == libc::EINTR {
                continue;
            }
            return Err(e);
        }
        off += n as usize;
    }
    Ok(())
}

/// openat(dirfd, name, O_CREAT|O_WRONLY|O_TRUNC|O_CLOEXEC, 0600) + write + fsync.
fn openat_write(dirfd: RawFd, name: &str, data: &[u8]) -> Result<(), i32> {
    let n = cstr(name);
    let fd = unsafe {
        libc::openat(
            dirfd,
            n.as_ptr(),
            libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC | libc::O_CLOEXEC,
            0o600 as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(errno());
    }
    let r = write_all_fd(fd, data).and_then(|_| {
        if unsafe { libc::fsync(fd) } != 0 {
            Err(errno())
        } else {
            Ok(())
        }
    });
    unsafe { libc::close(fd) };
    r
}

/// Durable replace: openat temp (O_EXCL) + write + fsync + renameat(dirfd,tmp,dirfd,final) + fsync(dirfd).
fn durable_replace(dirfd: RawFd, name: &str, tmp: &str, data: &[u8]) -> Result<(), i32> {
    let t = cstr(tmp);
    let fd = unsafe {
        libc::openat(
            dirfd,
            t.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY | libc::O_CLOEXEC,
            0o600 as libc::c_uint,
        )
    };
    if fd < 0 {
        return Err(errno());
    }
    let r = write_all_fd(fd, data).and_then(|_| {
        if unsafe { libc::fsync(fd) } != 0 {
            Err(errno())
        } else {
            Ok(())
        }
    });
    unsafe { libc::close(fd) };
    if let Err(e) = r {
        unsafe { libc::unlinkat(dirfd, t.as_ptr(), 0) };
        return Err(e);
    }
    let f = cstr(name);
    if unsafe { libc::renameat(dirfd, t.as_ptr(), dirfd, f.as_ptr()) } != 0 {
        let e = errno();
        unsafe { libc::unlinkat(dirfd, t.as_ptr(), 0) };
        return Err(e);
    }
    if unsafe { libc::fsync(dirfd) } != 0 {
        return Err(errno());
    }
    Ok(())
}

/// Custody check: fstat(dirfd) vs stat(path) on (dev, ino). Ok(true) = same inode.
fn custody(dirfd: RawFd, path: &CString) -> Result<bool, i32> {
    let mut a: libc::stat = unsafe { std::mem::zeroed() };
    let mut b: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(dirfd, &mut a) } != 0 {
        return Err(errno());
    }
    if unsafe { libc::stat(path.as_ptr(), &mut b) } != 0 {
        return Err(errno());
    }
    Ok(a.st_dev == b.st_dev && a.st_ino == b.st_ino)
}

fn fd_cloexec(fd: RawFd) -> bool {
    let fl = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    fl >= 0 && (fl & libc::FD_CLOEXEC) != 0
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Std,
    Flock,
}

/// Lock a fresh open of `dir`. Returns (File, description).
fn lock_dir(dir: &Path, mode: Mode) -> (Option<File>, String) {
    let f = match File::open(dir) {
        Ok(f) => f,
        Err(e) => return (None, format!("OPENERR {}", e)),
    };
    match mode {
        Mode::Std => match f.try_lock() {
            Ok(()) => (Some(f), "ACQUIRED".into()),
            Err(TryLockError::WouldBlock) => (None, "WOULDBLOCK".into()),
            Err(TryLockError::Error(e)) => (None, format!("ERR {} (os {:?})", e, e.raw_os_error())),
        },
        Mode::Flock => {
            let r = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if r == 0 {
                (Some(f), "ACQUIRED".into())
            } else {
                let e = errno();
                if e == libc::EWOULDBLOCK {
                    (None, "WOULDBLOCK".into())
                } else {
                    (None, format!("ERR {}", ename(e)))
                }
            }
        }
    }
}

// ---------- child roles ----------

fn role_probe(dir: &Path, mode: Mode) {
    let (_f, d) = lock_dir(dir, mode);
    println!("{}", d);
}

fn role_sleeper(dir: &Path, out: &Path, secs: u64) {
    // Scan our fd table for anything that is the locked directory inode.
    let mut target: libc::stat = unsafe { std::mem::zeroed() };
    let cp = cpath(dir);
    let have = unsafe { libc::stat(cp.as_ptr(), &mut target) } == 0;
    let mut hits = vec![];
    let mut open_fds = vec![];
    for fd in 0..1024 {
        let mut s: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut s) } == 0 {
            open_fds.push(fd);
            if have && s.st_dev == target.st_dev && s.st_ino == target.st_ino {
                hits.push(fd);
            }
        }
    }
    let _ = std::fs::write(
        out,
        format!("open_fds={:?} dir_inode_fds={:?}\n", open_fds, hits),
    );
    std::thread::sleep(Duration::from_secs(secs));
}

fn role_holder(dir: &Path, mode: Mode) {
    let exe = std::env::current_exe().unwrap();
    let (f, d) = lock_dir(dir, mode);
    let f = match f {
        Some(f) => f,
        None => {
            println!("LOCKFAIL {}", d);
            return;
        }
    };
    let dirfd = f.as_raw_fd();
    println!("LOCKED cloexec={}", fd_cloexec(dirfd));
    std::io::stdout().flush().unwrap();
    let stdin = std::io::stdin();
    let mut seq = 0u64;
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        let reply = match parts[0] {
            "write" => match openat_write(dirfd, parts[1], b"holder-write\n") {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("ERR {}", ename(e)),
            },
            "replace" => {
                seq += 1;
                let tmp = format!(".{}.tmp-{}-{}", parts[1], std::process::id(), seq);
                match durable_replace(dirfd, parts[1], &tmp, b"holder-replace\n") {
                    Ok(()) => "OK".to_string(),
                    Err(e) => format!("ERR {}", ename(e)),
                }
            }
            // NEGATIVE CONTROL: the legacy pathname style (create_dir_all + write by absolute path).
            "pathwrite" => {
                let pth = Path::new(parts[1]);
                let _ = std::fs::create_dir_all(pth.parent().unwrap());
                match std::fs::write(pth, b"pathname-write\n") {
                    Ok(()) => "OK".to_string(),
                    Err(e) => format!("ERR {}", e),
                }
            }
            "check" => match custody(dirfd, &cstr(parts[1])) {
                Ok(true) => "SAME".to_string(),
                Ok(false) => "CHANGED".to_string(),
                Err(e) => format!("ERR {}", ename(e)),
            },
            "churn" => {
                let n: u64 = parts[1].parse().unwrap();
                let mut err = None;
                for i in 0..n {
                    seq += 1;
                    let tmp = format!(".churn.tmp-{}-{}", std::process::id(), seq);
                    if let Err(e) = durable_replace(dirfd, "churn.state", &tmp, format!("{}\n", i).as_bytes()) {
                        err = Some(e);
                        break;
                    }
                    if let Err(e) = openat_write(dirfd, &format!("churn-{}.f", i % 7), b"x") {
                        err = Some(e);
                        break;
                    }
                }
                match err {
                    None => format!("OK cloexec_still={}", fd_cloexec(dirfd)),
                    Some(e) => format!("ERR {}", ename(e)),
                }
            }
            "spawn" => {
                let out = parts[1];
                match Command::new(&exe)
                    .args(["sleeper", dir.to_str().unwrap(), out, "60"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                {
                    Ok(c) => format!("SPAWNED {}", c.id()),
                    Err(e) => format!("ERR {}", e),
                }
            }
            "exit" => {
                println!("BYE");
                std::io::stdout().flush().unwrap();
                std::process::exit(0);
            }
            other => format!("ERR unknown {}", other),
        };
        println!("{}", reply);
        std::io::stdout().flush().unwrap();
    }
    drop(f);
}

// ---------- parent-side plumbing ----------

struct Holder {
    child: Child,
    stdin: ChildStdin,
    out: BufReader<ChildStdout>,
    pub hello: String,
}

impl Holder {
    fn start(dir: &Path, mode: Mode) -> Holder {
        let exe = std::env::current_exe().unwrap();
        let m = if mode == Mode::Std { "std" } else { "flock" };
        let mut child = Command::new(exe)
            .args(["holder", dir.to_str().unwrap(), m])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let mut out = BufReader::new(child.stdout.take().unwrap());
        let mut hello = String::new();
        out.read_line(&mut hello).unwrap();
        Holder { child, stdin, out, hello: hello.trim().to_string() }
    }
    fn cmd(&mut self, c: &str) -> String {
        writeln!(self.stdin, "{}", c).unwrap();
        self.stdin.flush().unwrap();
        let mut s = String::new();
        self.out.read_line(&mut s).unwrap();
        s.trim().to_string()
    }
    fn exit(mut self) {
        let _ = self.cmd("exit");
        let _ = self.child.wait();
    }
}

fn probe(dir: &Path, mode: Mode) -> String {
    let exe = std::env::current_exe().unwrap();
    let m = if mode == Mode::Std { "std" } else { "flock" };
    let o = Command::new(exe).args(["probe", dir.to_str().unwrap(), m]).output().unwrap();
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

struct Results {
    rows: Vec<(String, bool, String)>,
    raw: Vec<String>,
}

impl Results {
    fn log(&mut self, s: String) {
        println!("  {}", s);
        self.raw.push(s);
    }
    fn row(&mut self, name: &str, pass: bool, detail: String) {
        println!("[{}] {} -- {}", if pass { "PASS" } else { "FAIL" }, name, detail);
        self.rows.push((name.to_string(), pass, detail));
    }
}

fn pct(v: &mut Vec<u128>, p: f64) -> u128 {
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn fresh(base: &Path, name: &str) -> PathBuf {
    let d = base.join(name);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode_of = |s: &str| if s == "flock" { Mode::Flock } else { Mode::Std };
    if args.len() > 1 {
        match args[1].as_str() {
            "probe" => return role_probe(Path::new(&args[2]), mode_of(&args[3])),
            "holder" => return role_holder(Path::new(&args[2]), mode_of(&args[3])),
            "sleeper" => {
                return role_sleeper(Path::new(&args[2]), Path::new(&args[3]), args[4].parse().unwrap())
            }
            _ => {}
        }
    }

    let base_root = std::env::var("SPIKE_BASE").unwrap_or_else(|_| "/var/tmp".into());
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let base = PathBuf::from(base_root).join(format!("dir-lease-spike-{}-{}", std::process::id(), nanos));
    std::fs::create_dir_all(&base).unwrap();
    println!("scratch base: {}", base.display());
    println!("os: {} {}", std::env::consts::OS, std::env::consts::ARCH);

    let mut r = Results { rows: vec![], raw: vec![] };

    // Decide the lock mode the holder uses: std try_lock if it works on a directory, else flock.
    let probe_dir = fresh(&base, "s0");
    let std_on_dir = probe(&probe_dir, Mode::Std);
    let flock_on_dir = probe(&probe_dir, Mode::Flock);
    r.log(format!("S0 std File::open(dir)+try_lock on an unlocked directory: {}", std_on_dir));
    r.log(format!("S0 libc::flock(LOCK_EX|LOCK_NB) on an unlocked directory: {}", flock_on_dir));
    let mode = if std_on_dir == "ACQUIRED" { Mode::Std } else { Mode::Flock };
    r.row(
        "S0 std try_lock accepts a directory fd",
        std_on_dir == "ACQUIRED",
        format!("std={} flock={}", std_on_dir, flock_on_dir),
    );

    // S1 exclusivity
    {
        let d = fresh(&base, "s1");
        let a = Holder::start(&d, mode);
        let a_hello = a.hello.clone();
        r.log(format!("S1 A: {}", a_hello));
        let b_std = probe(&d, Mode::Std);
        let b_flock = probe(&d, Mode::Flock);
        r.log(format!("S1 B while A holds: std={} flock={}", b_std, b_flock));
        a.exit();
        let b_after = probe(&d, Mode::Std);
        r.log(format!("S1 B after A exit: std={}", b_after));
        r.row(
            "S1 exclusivity (cross-process)",
            a_hello.starts_with("LOCKED") && b_std == "WOULDBLOCK" && b_flock == "WOULDBLOCK" && b_after == "ACQUIRED",
            format!("while-held std={} flock={}; after-exit={}", b_std, b_flock, b_after),
        );
    }

    // S2 openat write lands in locked dir
    {
        let d = fresh(&base, "s2");
        let mut a = Holder::start(&d, mode);
        let w = a.cmd("write s2.txt");
        let content = std::fs::read_to_string(d.join("s2.txt")).unwrap_or_default();
        r.log(format!("S2 write={} content={:?}", w, content));
        a.exit();
        r.row("S2 openat write lands in locked dir", w == "OK" && content == "holder-write\n", w);
    }

    // S3 move + recreate
    {
        let d = fresh(&base, "s3");
        let moved = base.join("s3.moved");
        let mut a = Holder::start(&d, mode);
        let pre = a.cmd(&format!("check {}", d.display()));
        std::fs::rename(&d, &moved).unwrap();
        std::fs::create_dir(&d).unwrap();
        let w = a.cmd("write s3.txt");
        let rp = a.cmd("replace s3.state");
        let in_moved = moved.join("s3.txt").exists() && moved.join("s3.state").exists();
        let in_new = d.join("s3.txt").exists() || d.join("s3.state").exists();
        let post = a.cmd(&format!("check {}", d.display()));
        let ctl = a.cmd(&format!("pathwrite {}", d.join("ctl.txt").display()));
        let ctl_in_new = d.join("ctl.txt").exists();
        r.log(format!("S3 NEGATIVE CONTROL pathname write after move: {} landed-in-NEW-D={} (expected true: the hazard openat avoids)", ctl, ctl_in_new));
        let b_new = probe(&d, Mode::Std);
        let b_moved = probe(&moved, Mode::Std);
        r.log(format!(
            "S3 pre-check={} write={} replace={} in_moved={} in_new={} post-check={} B-lock-new-D={} B-lock-moved-D={}",
            pre, w, rp, in_moved, in_new, post, b_new, b_moved
        ));
        a.exit();
        r.row(
            "S3 move+recreate: writes follow moved inode",
            w == "OK" && rp == "OK" && in_moved && !in_new && pre == "SAME" && post == "CHANGED" && ctl_in_new,
            format!(
                "writes->moved={} new-D-untouched={} custody {}->{}; B can lock NEW D: {} (expected, new inode); moved D still held: {}",
                in_moved, !in_new, pre, post, b_new, b_moved
            ),
        );
    }

    // S4 rm -rf
    {
        let d = fresh(&base, "s4");
        let mut a = Holder::start(&d, mode);
        let _ = a.cmd("write before.txt");
        std::fs::remove_dir_all(&d).unwrap();
        let w = a.cmd("write s4.txt");
        let rp = a.cmd("replace s4.state");
        let chk = a.cmd(&format!("check {}", d.display()));
        let recreated = d.exists();
        let ctl = a.cmd(&format!("pathwrite {}", d.join("ctl.txt").display()));
        let resurrected = d.join("ctl.txt").exists();
        r.log(format!("S4 NEGATIVE CONTROL pathname create_dir_all+write after rm -rf: {} resurrected={} (expected true: the hazard openat avoids)", ctl, resurrected));
        r.log(format!("S4 after rm -rf: write={} replace={} check={} D-exists={}", w, rp, chk, recreated));
        a.exit();
        let enoent = format!("ERR {}", ename(libc::ENOENT));
        r.row(
            "S4 rm -rf: holder writes fail, no resurrection",
            w == enoent && rp == enoent && !recreated && resurrected,
            format!("openat={} replace={} custody={} D-recreated={}", w, rp, chk, recreated),
        );
    }

    // S5 renameat durable replace, torn-read check
    {
        let d = fresh(&base, "s5");
        let f = File::open(&d).unwrap();
        let dirfd = f.as_raw_fd();
        const SZ: usize = 64 * 1024;
        durable_replace(dirfd, "final.bin", ".final.bin.tmp-0", &vec![0u8; SZ]).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicU64::new(0));
        let torn = Arc::new(AtomicU64::new(0));
        let missing = Arc::new(AtomicU64::new(0));
        let (s2, rd2, t2, m2, p2) = (stop.clone(), reads.clone(), torn.clone(), missing.clone(), d.join("final.bin"));
        let reader = std::thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                match std::fs::read(&p2) {
                    Ok(b) => {
                        rd2.fetch_add(1, Ordering::Relaxed);
                        if b.len() != SZ || b.iter().any(|&x| x != b[0]) {
                            t2.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        m2.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        let mut errs = 0;
        for v in 1..=200u32 {
            let tmp = format!(".final.bin.tmp-{}", v);
            if durable_replace(dirfd, "final.bin", &tmp, &vec![(v % 251) as u8; SZ]).is_err() {
                errs += 1;
            }
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();
        let leftovers: Vec<String> = std::fs::read_dir(&d)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n != "final.bin")
            .collect();
        let last = std::fs::read(d.join("final.bin")).unwrap();
        let (rd, tn, ms) = (reads.load(Ordering::Relaxed), torn.load(Ordering::Relaxed), missing.load(Ordering::Relaxed));
        r.log(format!(
            "S5 200 replaces errs={} concurrent reads={} torn={} missing={} leftovers={:?} final-byte={}",
            errs, rd, tn, ms, leftovers, last[0]
        ));
        r.row(
            "S5 renameat durable replace, never torn",
            errs == 0 && tn == 0 && ms == 0 && leftovers.is_empty() && last[0] == (200 % 251) as u8 && rd > 0,
            format!("200 replaces, {} concurrent reads, torn={} missing={} temp-leftovers={}", rd, tn, ms, leftovers.len()),
        );
    }

    // S6 lock survives holder activity
    {
        let d = fresh(&base, "s6");
        let mut a = Holder::start(&d, mode);
        let c = a.cmd("churn 200");
        let b = probe(&d, Mode::Std);
        let bf = probe(&d, Mode::Flock);
        r.log(format!("S6 churn(200 renameat + 200 openat)={} then B std={} flock={}", c, b, bf));
        a.exit();
        r.row(
            "S6 lock survives own renameat/openat churn",
            c.starts_with("OK") && b == "WOULDBLOCK" && bf == "WOULDBLOCK",
            format!("{}; B std={} flock={}", c, b, bf),
        );
    }

    // S7 fd not inherited by children
    {
        let d = fresh(&base, "s7");
        let outf = base.join("s7.sleeper.out");
        let mut a = Holder::start(&d, mode);
        let cloexec = a.hello.clone();
        let sp = a.cmd(&format!("spawn {}", outf.display()));
        let mut wait = 0;
        while !outf.exists() && wait < 100 {
            std::thread::sleep(Duration::from_millis(20));
            wait += 1;
        }
        std::thread::sleep(Duration::from_millis(50));
        let b_during = probe(&d, Mode::Std);
        a.exit();
        let b_after = probe(&d, Mode::Std);
        let sl = std::fs::read_to_string(&outf).unwrap_or_default();
        if let Some(pid) = sp.strip_prefix("SPAWNED ") {
            let pid: i32 = pid.parse().unwrap();
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        r.log(format!(
            "S7 A {}; {}; B while A alive={}; A exited (child alive) B={}; child fd scan: {}",
            cloexec, sp, b_during, b_after, sl.trim()
        ));
        r.row(
            "S7 dir fd not inherited (O_CLOEXEC)",
            cloexec.contains("cloexec=true") && b_during == "WOULDBLOCK" && b_after == "ACQUIRED" && sl.contains("dir_inode_fds=[]"),
            format!("{} B-after-holder-exit={} child: {}", cloexec, b_after, sl.trim()),
        );
    }

    // S8 timing
    {
        let d = fresh(&base, "s8");
        let (f, _) = lock_dir(&d, mode);
        let f = f.unwrap();
        let dirfd = f.as_raw_fd();
        let cp = cpath(&d);
        let n = 100_000;
        let t0 = Instant::now();
        let mut ok = 0;
        for _ in 0..n {
            if custody(dirfd, &cp) == Ok(true) {
                ok += 1;
            }
        }
        let ns_op = t0.elapsed().as_nanos() / n as u128;
        let data = vec![b'a'; 4096];
        let mut w: Vec<u128> = Vec::with_capacity(500);
        for i in 0..500 {
            let t = Instant::now();
            openat_write(dirfd, &format!("t{}.f", i % 10), &data).unwrap();
            w.push(t.elapsed().as_micros());
        }
        let mut rp: Vec<u128> = Vec::with_capacity(500);
        for i in 0..500 {
            let t = Instant::now();
            durable_replace(dirfd, "state.bin", &format!(".state.bin.tmp-{}", i), &data).unwrap();
            rp.push(t.elapsed().as_micros());
        }
        let (w50, w99) = (pct(&mut w, 0.50), pct(&mut w, 0.99));
        let (r50, r99) = (pct(&mut rp, 0.50), pct(&mut rp, 0.99));
        r.log(format!("S8 custody check: {} ns/op over {} iters ({} SAME)", ns_op, n, ok));
        r.log(format!("S8 openat+write(4KiB)+fsync x500: p50={}us p99={}us", w50, w99));
        r.log(format!("S8 durable replace (openat tmp+write+fsync+renameat+fsync(dirfd), 4KiB) x500: p50={}us p99={}us", r50, r99));
        r.row(
            "S8 timing",
            ok == n,
            format!("custody {} ns/op; openat+write+fsync p50 {}us p99 {}us; replace p50 {}us p99 {}us", ns_op, w50, w99, r50, r99),
        );
        drop(f);
    }

    let _ = std::fs::remove_dir_all(&base);

    println!("\n================ RESULT TABLE ================");
    println!("| Scenario | Result | Detail |");
    println!("|---|---|---|");
    for (n, p, d) in &r.rows {
        println!("| {} | {} | {} |", n, if *p { "PASS" } else { "FAIL" }, d.replace('|', "/"));
    }
    let fails = r.rows.iter().filter(|x| !x.1).count();
    println!("\n{} scenarios, {} FAIL; holder lock mode used: {}", r.rows.len(), fails, if mode == Mode::Std { "std File::try_lock" } else { "libc::flock fallback" });
    if fails > 0 {
        std::process::exit(1);
    }
}
