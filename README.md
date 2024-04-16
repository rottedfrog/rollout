# Rollout

Rollout is a rolling logfile appender that takes data from stdin and appends to a rolling logfile. There are a number of command line options that you can use to change how the log files are created. It is deliberately very simple, to limit the overhead it requires.

## Log File Names and Location

Log files are stored in a specified directory in files with a well defined filename. The current journal is called `current`. When full, it is renamed in the format `{prefix}.{n}.log` where `{prefix}` is specified on the command line and `{n}` is a monotonically increasing integer, starting at 1 if no existing logs are present.

## Log File Format and Size

Input on stdin is assumed to be ascii/utf-8.
The size of each log file is by default 10MB and can be specified on the command line. This is not a hard limit. The logger will attempt to break the log on a new line before it hits the limit, but for some particularly long lines this will result in the log file being bigger than the limit.

## Failure

Any irrecoverable errors cause rollout to exit with a non-zero return code. Examples of irrecoverable errors include:

    - Failing to open a journal file
    - the input stream failing with an error
    - running out of disk space

## Command Line Options

| Option                  | Description                                   |
|-------------------------|-----------------------------------------------|
| `-h, --help`            | Prints help information                       |
| `-s, --size`            | Max log size in KB (default 10240)            |
| `-k, --keep`            | Max number to keep (default 0 (all), max 999) |
| `-r, --rotate-on-start` | Rotate to a new log file on startup           |
| `-p, --prefix`          | Log file prefix (required)                    |

## Examples

```sh
loggingprocess | rollout -p foo logfiles
```
Creates log files `foo.1.log`, `foo.2.log` etc. in the `logfiles` folder.

```sh
cat somebiglog | rollout -p bar -k 2 -s 1024 logs
```
Creates log files `bar.1.log`, `bar.2.log` etc. in the `logfiles` folder. Only keep the last 2 complete logs, and each log file should be around 1024KB in size.