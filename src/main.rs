// Redis To KeyDB (v1.0.0) by Fire
// do not judge my coding here, I'm rusty in rust

use clap::{CommandFactory, Parser};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, RedisResult, Value};
use tokio;

#[derive(Parser)]
#[command(name = "Redis to KeyDB Migrator")]
#[command(version = "1.0.0")]
#[command(about = "Migrates a Redis database over to a KeyDB database.", long_about = None)]
struct Cli {
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:6379",
        value_name = "REDIS URL"
    )]
    redis_url: String,

    #[arg(short, long, value_name = "KEYDB URL")]
    keydb_url: String,

    #[arg(short, long, default_value = "100")]
    batch_size: usize,

    #[arg(short, long)]
    verbose: bool,
}

fn debug_log(input: String, verbose: bool) {
    if verbose {
        println!("[VERBOSE] {}", input);
    }
}

async fn migrate_key(
    redis_con: &mut MultiplexedConnection,
    keydb_con: &mut MultiplexedConnection,
    key: &str,
    verbose: bool,
) -> RedisResult<()> {
    let key_type: String = redis_con.key_type(key).await?;
    //println!("The key IS {}", key_type);

    match key_type.as_str() {
        "string" => {
            let value: String = redis_con.get(key).await?;
            debug_log(format!("SET {} {:?}", key, value), verbose);
            keydb_con.set(key, value).await?;
        }
        "list" => {
            let values: Vec<String> = redis_con.lrange(key, 0, -1).await?;
            /*if keydb_con.exists(key).await? {
                keydb_con.del(key).await?;
            }*/
            for value in values {
                debug_log(format!("RPUSH {} {:?}", key, value), verbose);
                keydb_con.rpush(key, value).await?;
            }
        }
        "set" => {
            let values: Vec<String> = redis_con.smembers(key).await?;
            for value in values {
                debug_log(format!("SADD {} {:?}", key, value), verbose);
                keydb_con.sadd(key, value).await?;
            }
        }
        "hash" => {
            let entries: Vec<(String, String)> = redis_con.hgetall(key).await?;
            for (field, value) in entries {
                debug_log(format!("HSET {} {:?} {:?}", key, field, value), verbose);
                keydb_con.hset(key, field, value).await?;
            }
        }
        _ => {
            let value: Value = redis_con.get(key).await?;
            match value {
                Value::Data(data) => {
                    debug_log(format!("SET {} {:?}", key, data), verbose);
                    keydb_con.set(key, data).await?;
                }
                Value::Int(data) => {
                    debug_log(format!("SET {} {:?}", key, data), verbose);
                    keydb_con.set(key, data).await?;
                }
                Value::Bulk(ref items) => {
                    for item in items {
                        if let Value::Data(data) = item {
                            debug_log(format!("[rpush] SET {} {:?}", key, data), verbose);
                            let _: () = keydb_con.rpush(key, data).await?;
                        }
                    }
                }
                _ => {
                    println!("Could not migrate invalid key: {}", key);
                } // nil should NOT happen.
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> RedisResult<()> {
    if std::env::args().len() == 1 {
        Cli::command().print_help().unwrap_or_default();
    } else {
        let cli = Cli::parse();
        println!("Starting migration...");
        debug_log(
            format!(
                "Migrating RDB ({}) -> KDB ({})",
                cli.redis_url, cli.keydb_url,
            ),
            cli.verbose,
        );

        let redis_url = format!("redis://{}", cli.redis_url);
        let keydb_url = format!("redis://{}", cli.keydb_url);

        let redis_client = Client::open(redis_url)?;
        let mut redis_con = redis_client.get_multiplexed_async_connection().await?;
        debug_log(
            "Made a connection with Redis server".to_string(),
            cli.verbose,
        );

        let keydb_client = Client::open(keydb_url)?;
        let keydb_con = keydb_client.get_multiplexed_async_connection().await?;
        debug_log(
            "Made a connection with KeyDB server".to_string(),
            cli.verbose,
        );

        // actual migration part
        let keys: Vec<String> = redis_con.keys("*").await?;
        let key_batches = keys.chunks(cli.batch_size);
        let mut batch_iteration: u64 = 0;
        //let value: Value = redis_con.get(key).await;
        //
        for batch in key_batches {
            batch_iteration += 1;
            debug_log(format!("BATCH {}", batch_iteration), cli.verbose);
            let mut tasks = vec![];
            for key in batch {
                let mut keydb_con_clone = keydb_con.to_owned();
                let mut rdb_con_clone = redis_con.to_owned();
                let key_clone = key.clone();

                let task = tokio::spawn(async move {
                    if let Err(e) = migrate_key(
                        &mut rdb_con_clone,
                        &mut keydb_con_clone,
                        &key_clone,
                        cli.verbose,
                    )
                    .await
                    {
                        eprintln!("Failed to migrate key {}: {:?}", key_clone, e);
                    }
                });
                tasks.push(task);
            }
            for task in tasks {
                if let Err(e) = task.await {
                    eprintln!("Task error: {:?}", e);
                }
            }
        }

        println!("Data migration complete!");
    }
    Ok(())
}
