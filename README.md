# Redis-to-KeyDB
A Redis database to KeyDB database migrator tool. Or you can think of it as a RDB -> RDB migrator.

## How to build
* Required packages
    * `rustc`
    * `cargo`
    * `redis`
    * `keydb` (optional)
* Building
```
git clone https://github.com/FireMario211/Redis-to-KeyDB
cd Redis-to-KeyDB
cargo build --release
```

## How to use
* `--help`, `-h`
	* Print help
* `--version`, `-V`
	* Print version
* `--redis-url <REDIS URL>`, `-r` (default: 127.0.0.1:6379)
	* The URL to migrate from.
* `--keydb-url <KEYDB URL>`, `-k`
    * The URL to migrate to.
* `--batch-size`, `-b` (default: 100)
    * How many keys to migrate in each batch during the migration process.
* `--verbose`, `-v`
    * Makes each step of the process "verbose" (logging)

I made this project because I haven't figured out how to run newer RDB versions with KeyDB
