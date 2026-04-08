use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use serde_json::to_string;
use std::io::{BufRead, BufReader, Read, Write};
use std::num::ParseIntError;
use crate::tree_modification;

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};

pub fn get_ind(ctx : &DbContext , file_id : &String) -> u64{
    let mut ans = 0;
    for i in ctx.get_table_specs() {
        if i.file_id == *file_id {
            break;
        }
    }
    ans
}

pub fn read_block(file_id : &String , row_id : u64 , number_of_rows : u64 , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , ctx : &DbContext) -> Result<Vec<Vec<String>>,String> {
    disk_out.write_all(format!{"get file start-block {}\n",file_id}.as_bytes()).map_err(|e| e.to_string())?;
    disk_out.flush().map_err(|e| e.to_string())?;
    let mut input = String::new();
    disk_buf.read_line(&mut input).map_err(|e| e.to_string())?;
    let strt_ind : usize = input.trim().parse().expect("File id does not exist");
    input.clear();

    let ind :u64 = strt_ind as u64 + row_id;
    let act_ind = get_ind(ctx , file_id);
    let table = &ctx.get_table_specs()[act_ind as usize];
    disk_out.write_all(String::from("get block-size\n").as_bytes()).map_err(|e| e.to_string())?;
    disk_buf.read_line(&mut input).map_err(|e| e.to_string())?;

    // Tell Rust explicitly to parse it into a u64
    let block_size: u64 = input.trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
    input.clear();
    let mut buf = vec![0u8; block_size as usize];
    let mut res : Vec<Vec<String>> = Vec::new();
    let mut i = ind;
    while res.len() < number_of_rows as usize {
        disk_out.write_all(format!("get block {} {}\n" , i , 1).as_bytes()).map_err(|e| e.to_string())?;
        disk_buf.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let mut k = 0;
        while k < buf.len() as usize && res.len() < number_of_rows as usize {
            let mut v = Vec::new();
            for j in &table.column_specs {
                let mut b = Vec::new();
                match j.data_type {
                    common::DataType::String => {
                        while buf[k] != 0 {
                            b.push(buf[k]);
                            k += 1;
                        }
                        k += 1;
                        v.push(String::from_utf8_lossy(&b).to_string());
                    },
                    common::DataType::Int32 => {
                        v.push(u32::from_le_bytes(buf[k..k + 4].try_into().unwrap()).to_string());
                        k += 4;
                    },
                    common::DataType::Int64 => {
                        v.push(u64::from_le_bytes(buf[k..k + 8].try_into().unwrap()).to_string());
                        k += 8; 
                    },
                    common::DataType::Float32 => {
                        v.push(f32::from_le_bytes(buf[k..k + 4].try_into().unwrap()).to_string());
                        k += 4;
                    },
                    common::DataType::Float64 => {
                        v.push(f64::from_le_bytes(buf[k..k + 8].try_into().unwrap()).to_string());
                        k += 8;
                    },
                }
            }
            res.push(v);
        }
        i += 1;
    }
    Ok(res)
}