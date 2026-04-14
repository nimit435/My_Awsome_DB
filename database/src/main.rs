use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Read, Write};
use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};

mod cli;
mod io_setup;
mod tree_modification;
mod basic_func;
mod process_query;
fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    // Use the ctx to the tables and stats
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    // Setups and provides handler to talk with disk and monitor
    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Use buffered reader to read lines easier
    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);

    // Temporary variable to read a line of input
    let mut input_line = String::new();

    // Read query form monitor
    monitor_buf_reader.read_line(&mut input_line)?;
    let query: Query = serde_json::from_str(&input_line).unwrap();
    println!("Input query is: {:#?}", query);

    // Interacting with with Disk

    // Get block size
    // disk_out.write_all("get block-size\n".as_bytes())?;
    // disk_out.flush()?;

    // input_line.clear();
    // disk_buf_reader.read_line(&mut input_line)?;
    // let block_size: u64 = input_line.trim().parse()?;

    // println!("block size is {}", block_size);

    // disk_out.write_all("get block 0 1\n".as_bytes())?;
    // disk_out.flush()?;

    // let mut buf = vec![0u8; block_size as usize];
    // disk_buf_reader.read_exact(&mut buf)?;

    // println!(
    //     "First few bytes of block 0 contains {:?}",
    //     String::from_utf8_lossy(&buf[..50])
    // );
    input_line.clear();
    disk_out.write_all(format!{"get anon-start-block"}.as_bytes()).map_err(|e| e.to_string());
    disk_out.flush();
    disk_buf_reader.read_line(&mut input_line).map_err(|e| e.to_string());
    let mut write_block : usize = input_line.trim().parse().expect("Error in parsing");
    let (strt_block , size , ctx) = process_query::master(&mut write_block , &query.root , &mut disk_buf_reader , &mut disk_out , &ctx).unwrap();

    // Get memory limit from monitor
    monitor_out.write_all("get_memory_limit\n".as_bytes())?;
    monitor_out.flush()?;
    monitor_buf_reader.read_line(&mut input_line)?;
    let memory_limit_mb: u32 = input_line.trim().parse()?;
    println!("Memory limit is set to {} MB", memory_limit_mb);

    // Send result of query to monitor for validation
    monitor_out.write_all("validate\n".as_bytes())?;
    for i in 0..size {
        let res = basic_func::read_block(strt_block  + i, &ctx , &mut disk_out  , &mut disk_buf_reader).unwrap();
        for j in 0..res.len() {
            for k in 0..res[j].len() {
                monitor_out.write_all(format!{"{}|" , res[j][k]}.as_bytes())?;
            }
            monitor_out.write_all("!\n".as_bytes())?;
        }
    }
    monitor_out.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
