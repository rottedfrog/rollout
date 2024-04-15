use std::{
    collections::VecDeque,
    fs::{rename, DirEntry, File},
    io::{self, stdin, ErrorKind, Read, Write},
    os::unix::fs::MetadataExt,
    path::Path,
    process::exit,
};

const HELP: &str = "\
A rolling logfile appender

Takes stdin and appends to logfiles in <dir>. Reads Logs are broken at a newline as close as possible to the specified size
Any errors deemed unrecoverable and it immediately exits with a return value of 1.

Log files are of the format {prefix}.{n}.log where {prefix} is specified on the command line and {n} is a zero padded 3 digit number, starting at 1.


USAGE:
  rollout [OPTIONS] <dir>

FLAGS:
  -h, --help            Prints help information

OPTIONS:
  -s, --size                Max log size in KB (default 10240)
  -k, --keep                Max number to keep (default 5, max 999)
  -r, --rotate-on-start     Rotate to a new log file on startup
  -p, --prefix              Log file prefix
ARGS:
  <INPUT>
";

struct Args {
    dir: String,
    size_bytes: u64,
    to_keep: u32,
    rotate: bool,
    prefix: String,
}

enum ArgError {
    ExpectedNumber(String),
    UnknownArgument(String),
    UnableToFindOrCreateDir(String),
    UnexpectedPositionalArg(String),
    MissingDir,
    MissingNumber,
    MissingPrefix,
}

fn parse_args() -> Result<Args, ArgError> {
    enum ArgState {
        None,
        Size,
        Keep,
        Prefix,
    }

    let mut dir = None;
    let mut state = ArgState::None;
    let mut size_bytes = 10 * 1024 * 1024;
    let mut to_keep = 5;
    let mut rotate = false;
    let mut prefix = None;

    for arg in std::env::args().skip(1) {
        match state {
            ArgState::Size => {
                if let Ok(size) = arg.parse::<u64>() {
                    size_bytes = 1024 * size;
                    state = ArgState::None;
                    continue;
                } else {
                    return Err(ArgError::ExpectedNumber(arg));
                }
            }
            ArgState::Keep => {
                if let Ok(keep) = arg.parse() {
                    to_keep = keep;
                    state = ArgState::None;
                    continue;
                } else {
                    return Err(ArgError::ExpectedNumber(arg));
                }
            }
            ArgState::Prefix => {
                prefix = Some(arg);
                state = ArgState::None;
                continue;
            }
            ArgState::None => {}
        }

        match arg.as_str() {
            "-s" | "--size" => state = ArgState::Size,
            "-k" | "--keep" => state = ArgState::Keep,
            "-p" | "--prefix" => state = ArgState::Prefix,
            "-r" | "--rotate-on-start" => rotate = true,
            _ => {
                if arg.starts_with('-') {
                    return Err(ArgError::UnknownArgument(arg));
                }
                if dir.is_some() {
                    return Err(ArgError::UnexpectedPositionalArg(arg));
                }
                dir = Some(arg)
            }
        }
    }
    let ArgState::None = state else {
        return Err(ArgError::MissingNumber);
    };

    let Some(dir) = dir else {
        return Err(ArgError::MissingDir);
    };
    let Some(prefix) = prefix else {
        return Err(ArgError::MissingPrefix);
    };

    if std::fs::create_dir_all(&dir).is_err() {
        return Err(ArgError::UnableToFindOrCreateDir(dir));
    }

    Ok(Args {
        dir,
        size_bytes,
        to_keep,
        rotate,
        prefix,
    })
}

fn main() {
    match parse_args() {
        Ok(args) => run(args),
        Err(e) => {
            match e {
                ArgError::ExpectedNumber(a) => eprintln!("Expected number, found '{a}'"),
                ArgError::UnknownArgument(a) => eprintln!("Unknown argument '{a}'"),
                ArgError::UnableToFindOrCreateDir(a) => {
                    eprintln!("Unable to find or create directory '{a}'")
                }
                ArgError::MissingDir => eprintln!("Log directory not specified"),
                ArgError::UnexpectedPositionalArg(a) => eprintln!("Unexpected argument '{a}'"),
                ArgError::MissingNumber => eprintln!("Expected number"),
                ArgError::MissingPrefix => eprintln!("Missing prefix"),
            }
            eprintln!();
            eprintln!("{HELP}");
        }
    }
}

fn open_current() -> io::Result<(u64, File)> {
    let f = File::options().append(true).create(true).open("current")?;
    Ok((f.metadata()?.size(), f))
}

fn rotate(mut current: File, lm: &mut LogManager, to_keep: u32) -> File {
    resolve_io(|| current.flush());
    drop(current);

    resolve_io(|| rename("current", lm.next_logfile()));
    lm.cleanup_old(to_keep as usize);
    resolve_io(|| {
        File::options()
            .write(true)
            .truncate(true)
            .create(true)
            .open("current")
    })
}

fn resolve_io<T>(mut f: impl FnMut() -> io::Result<T>) -> T {
    loop {
        match (f)() {
            Ok(t) => return t,
            Err(e) => match e.kind() {
                ErrorKind::Interrupted | ErrorKind::WouldBlock => {}
                _ => exit(1), // Any other error is probably not recoverable, so exit.
            },
        }
    }
}

fn run(args: Args) -> ! {
    let Ok(()) = std::env::set_current_dir(args.dir) else {
        exit(1)
    };
    let mut lm = LogManager::new(args.prefix).unwrap();

    let Ok((mut file_size, mut outp)) = open_current() else {
        exit(1)
    };
    if args.rotate {
        outp = rotate(outp, &mut lm, args.to_keep);
    }
    let mut buf = [0; 1024];
    let mut inp = stdin().lock();
    loop {
        let sz = resolve_io(|| inp.read(&mut buf[..]));
        file_size += sz as u64;
        if file_size >= args.size_bytes {
            if let Some(b) = buf.iter().position(|b| *b == b'\n') {
                let (first, second) = buf.split_at(b + 1);
                resolve_io(|| outp.write_all(first));
                outp = rotate(outp, &mut lm, args.to_keep);
                resolve_io(|| outp.write_all(second));
                file_size = second.len() as u64;
                continue;
            }
        }
        resolve_io(|| outp.write_all(&buf[..sz]))
    }
}

fn log_index(de: &DirEntry, prefix: &str) -> Option<u32> {
    let f = de.file_name();
    let p: &Path = f.as_ref();
    if p.starts_with(prefix) && p.extension().is_some_and(|ext| ext == "log") {
        let p = p.to_str()?;
        p[prefix.len()..p.len() - 4].parse().ok()
    } else {
        None
    }
}

struct LogManager {
    prefix: String,
    log_indices: VecDeque<u32>,
}

impl LogManager {
    fn new(prefix: String) -> io::Result<Self> {
        let mut log_indices: Vec<_> = std::fs::read_dir(".")?
            .flatten()
            .filter_map(|de| log_index(&de, prefix.as_str()))
            .collect();
        log_indices.sort_unstable();

        Ok(LogManager {
            prefix,
            log_indices: log_indices.into(),
        })
    }

    fn next_logfile(&mut self) -> String {
        let i = self.next_index();
        self.filename(i)
    }

    fn next_index(&mut self) -> u32 {
        let i = self.log_indices.back().copied().unwrap_or(1);
        self.log_indices.push_back(i);
        i
    }

    fn filename(&self, index: u32) -> String {
        format!("{}{}.log", self.prefix, index)
    }

    fn cleanup_old(&mut self, to_keep: usize) {
        while self.log_indices.len() > to_keep {
            let index = self.log_indices.pop_front().unwrap();
            let _ = std::fs::remove_file(self.filename(index));
        }
    }
}
