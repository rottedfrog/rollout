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
  -s, --size                Max log size in KB (default: 10240)
  -k, --keep                Max number to keep (default: 5, max: 999)
  -r, --rotate-on-start     Rotate to a new log file on startup
  -p, --prefix              Log file prefix (required)
ARGS:
  <dir>                     The directory in which to place the logfiles
";

#[derive(PartialEq, Eq, Debug)]
struct Args {
    dir: String,
    size_bytes: u64,
    to_keep: u32,
    rotate: bool,
    prefix: String,
}

#[derive(PartialEq, Eq, Debug)]
enum ArgError {
    ExpectedNumber(String),
    UnknownArgument(String),
    UnableToFindOrCreateDir(String),
    UnexpectedPositionalArg(String),
    ExpectedArgumentFoundFlag { flag: &'static str, found: String },
    MissingDir,
    MissingNumber,
    MissingPrefix,
    MissingArgument,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, ArgError> {
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

    for arg in args.skip(1) {
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
                if arg.starts_with('-') {
                    return Err(ArgError::ExpectedArgumentFoundFlag {
                        flag: "--prefix",
                        found: arg,
                    });
                }
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

    match state {
        ArgState::None => {}
        ArgState::Size | ArgState::Keep => return Err(ArgError::MissingNumber),
        ArgState::Prefix => return Err(ArgError::MissingArgument),
    }

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
    match parse_args(std::env::args()) {
        Ok(args) => run(args),
        Err(e) => {
            match e {
                ArgError::ExpectedNumber(a) => eprintln!("Expected number, found '{a}'"),
                ArgError::UnknownArgument(a) => eprintln!("Unknown argument '{a}'"),
                ArgError::ExpectedArgumentFoundFlag { flag, found } => {
                    eprintln!("Expected argument for {flag}, found {found}")
                }
                ArgError::UnableToFindOrCreateDir(a) => {
                    eprintln!("Unable to find or create directory '{a}'")
                }
                ArgError::MissingDir => eprintln!("Log directory not specified"),
                ArgError::UnexpectedPositionalArg(a) => eprintln!("Unexpected argument '{a}'"),
                ArgError::MissingNumber => eprintln!("Expected number"),
                ArgError::MissingPrefix => eprintln!("Missing prefix"),
                ArgError::MissingArgument => eprintln!("Missing argument"),
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
    if args.rotate && file_size > 0 {
        outp = rotate(outp, &mut lm, args.to_keep);
        file_size = 0;
    }
    let mut buf = [0; 1024];
    let mut inp = stdin().lock();
    loop {
        let sz = resolve_io(|| inp.read(&mut buf[..]));
        if sz == 0 {
            exit(0);
        }
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
    let f = f.to_str().unwrap_or_default();
    let p: &Path = f.as_ref();
    if f.starts_with(prefix) && p.extension().is_some_and(|ext| ext == "log") {
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
        let i = self.log_indices.back().copied().unwrap_or(0);
        self.log_indices.push_back(i + 1);
        i + 1
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_fails_if_no_dir_set() {
        let result = parse_args(
            ["rollout", "-p", "foo", "-s", "100", "-k", "5", "-r"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(result, Err(ArgError::MissingDir));
    }

    #[test]
    fn parse_args_fails_if_no_prefix_set() {
        let result = parse_args(
            ["rollout", "-s", "100", "-k", "5", "-r", "logs"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(result, Err(ArgError::MissingPrefix));
    }

    #[test]
    fn parse_args_fails_if_prefix_missing_arg() {
        let result = parse_args(["rollout", "logs", "-p"].iter().map(ToString::to_string));

        assert_eq!(result, Err(ArgError::MissingArgument));
    }
    #[test]
    fn parse_args_fails_if_prefix_arg_starts_with_hyphen() {
        let result = parse_args(
            ["rollout", "-p", "-s", "100", "-k", "5", "-r", "logs"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(
            result,
            Err(ArgError::ExpectedArgumentFoundFlag {
                flag: "--prefix",
                found: "-s".to_string()
            })
        );
    }

    #[test]
    fn parse_args_fails_if_keep_missing_arg() {
        let result = parse_args(
            ["rollout", "-p", "foo", "logs", "-k"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(result, Err(ArgError::MissingNumber));
    }
    #[test]
    fn parse_args_fails_if_size_missing_arg() {
        let result = parse_args(
            ["rollout", "-p", "foo", "logs", "-s"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(result, Err(ArgError::MissingNumber));
    }
    #[test]
    fn parse_args_fails_if_keep_arg_is_not_number() {
        let result = parse_args(["rollout", "-k", "NaN"].iter().map(ToString::to_string));

        assert_eq!(result, Err(ArgError::ExpectedNumber("NaN".to_string())));
    }

    #[test]
    fn parse_args_fails_if_size_arg_is_not_number() {
        let result = parse_args(["rollout", "-s", "NaN"].iter().map(ToString::to_string));

        assert_eq!(result, Err(ArgError::ExpectedNumber("NaN".to_string())));
    }

    #[test]
    fn parse_args_fails_if_unknown_argument() {
        let result = parse_args(["rollout", "--unknown"].iter().map(ToString::to_string));

        assert_eq!(
            result,
            Err(ArgError::UnknownArgument("--unknown".to_string()))
        );
    }

    #[test]
    fn parse_args_fails_if_too_many_positional_args() {
        let result = parse_args(
            ["rollout", "-p", "foo", "logs", "extra"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(
            result,
            Err(ArgError::UnexpectedPositionalArg("extra".to_string()))
        );
    }

    #[test]
    fn parse_args_succeeds_with_long_form_args() {
        let result = parse_args(
            [
                "rollout",
                "--prefix",
                "foo",
                "--size",
                "100",
                "--keep",
                "6",
                "--rotate-on-start",
                "logs",
            ]
            .iter()
            .map(ToString::to_string),
        );

        assert_eq!(
            result,
            Ok(Args {
                dir: "logs".to_string(),
                size_bytes: 100 * 1024,
                to_keep: 6,
                rotate: true,
                prefix: "foo".to_string(),
            }),
        );
    }

    #[test]
    fn parse_args_succeeds_with_short_form_args() {
        let result = parse_args(
            ["rollout", "-p", "foo", "-s", "100", "-k", "6", "-r", "logs"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(
            result,
            Ok(Args {
                dir: "logs".to_string(),
                size_bytes: 100 * 1024,
                to_keep: 6,
                rotate: true,
                prefix: "foo".to_string(),
            }),
        );
    }

    #[test]
    fn parse_args_default_args_are_correct() {
        let result = parse_args(
            ["rollout", "-p", "foo", "logs"]
                .iter()
                .map(ToString::to_string),
        );

        assert_eq!(
            result,
            Ok(Args {
                dir: "logs".to_string(),
                size_bytes: 10 * 1024 * 1024,
                to_keep: 5,
                rotate: false,
                prefix: "foo".to_string(),
            }),
        );
    }
}
