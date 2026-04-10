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
    for table_spec in ctx.get_table_specs() {
        println!("Table: {}", table_spec.name);
        println!("File id: {}", table_spec.file_id);
        for column_spec in &table_spec.column_specs {
            println!(
                "\tColumn: {} ({:?})",
                column_spec.column_name, column_spec.data_type
            );
        }
        println!();
    }

    // Setups and provides handler to talk with disk and monitor
    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    // Use buffered reader to read lines easier
    let mut disk_buf_reader = BufReader::new(disk_in);
    let mut monitor_buf_reader = BufReader::new(monitor_in);
    let mut n_ctx = Vec::new();
    let c_tb = ctx.get_table_specs();
    for tb in &c_tb[0].column_specs {
        n_ctx.push(tb.data_type.clone());
    }
    let mut vec = basic_func::read_block(0 as usize , &n_ctx , &mut disk_out , &mut disk_buf_reader).unwrap();
    
    // Print to your terminal so you can see it
    for i in &vec {
        for j in i {
            print!("{}|", j);
        }
        println!();
    }

    // --- MUST UNCOMMENT AND FIX THIS TO TALK TO THE MONITOR ---
    // let mut input_line = String::new();
    
    // // 1. Read the query from the monitor first (Required)
    // monitor_buf_reader.read_line(&mut input_line)?;
    
    // // 2. Tell the monitor you are about to send validation data
    // monitor_out.write_all(b"validate\n")?;
    
    // // 3. Loop through your Vec<Vec<String>> and send it to the monitor!
    // for row in &vec {
    //     let mut row_string = String::new();
    //     for col in row {
    //         row_string.push_str(col);
    //         row_string.push('|');
    //     }
    //     row_string.push('\n');
        
    //     // Send this row to the monitor
    //     monitor_out.write_all(row_string.as_bytes())?;
    // }
    
    // // 4. Send the '!' character to tell the monitor you are finished
    // monitor_out.write_all(b"!\n")?;
    // monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}
