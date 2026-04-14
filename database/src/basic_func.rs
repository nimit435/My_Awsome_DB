use anyhow::{Context, Result};
use clap::Parser;
use common::query::{ComparisionValue, Query};
use db_config::DbContext;
use serde_json::to_string;
use std::io::{BufRead, BufReader, Read, Write};
use std::num::ParseIntError;
use crate::tree_modification;
use common::DataType;
use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};


pub fn read_block(block_id : usize , ctx : &Vec<(String , DataType)> , disk_out : &mut impl Write , disk_buf : &mut impl BufRead) -> Result<Vec<Vec<String>>,String> {
    let mut input = String::new();
    disk_out.write_all(String::from("get block-size\n").as_bytes()).map_err(|e| e.to_string())?;
    disk_out.flush().map_err(|e|e.to_string());
    disk_buf.read_line(&mut input).map_err(|e| e.to_string())?;
    // Tell Rust explicitly to parse it into a u64
    let block_size: u64 = input.trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
    input.clear();
    let mut buf = vec![0u8; block_size as usize];
    disk_out.write_all(format!{"get block {} 1\n" , block_id}.as_bytes()).map_err(|e|e.to_string());
    disk_out.flush().map_err(|e|e.to_string());
    disk_buf.read_exact(&mut buf).map_err(|e|e.to_string());
    let mut i = 0;
    let mut res = Vec::new();
    while i < buf.len() {
        if i + 4 > buf.len() || buf[i..i+4] == [0, 0, 0, 0] {
            break;
        }
        let mut v = Vec::new();
        for j in 0..ctx.len() {
            match ctx[j].1 {
                DataType::Int32 => {
                    let mut b = vec![0u8;4];
                    for k in i..i + 4 {
                        b[k - i] = buf[k];
                    }
                    v.push((i32::from_le_bytes(b[..].try_into().unwrap())).to_string());
                    i += 4;
                },
                DataType::Float32 => {
                    let mut b = vec![0u8;4];
                    for k in i..i + 4 {
                        b[k - i] = buf[k];
                    }
                    v.push((f32::from_le_bytes(b[..].try_into().unwrap())).to_string());
                    i += 4;
                },
                DataType::Int64 => {
                    let mut b = vec![0u8;8];
                    for k in i..i + 8 {
                        b[k - i] = buf[k];
                    }
                    v.push((i64::from_le_bytes(b[..].try_into().unwrap())).to_string());
                    i += 8;
                },
                DataType::Float64 => {
                    let mut b = vec![0u8;8];
                    for k in i..i + 8 {
                        b[k - i] = buf[k];
                    }
                    v.push((f64::from_le_bytes(b[..].try_into().unwrap())).to_string());
                    i += 8;
                },
                DataType::String => {
                    let mut b = Vec::new();
                    while true {
                        b.push(buf[i]);
                        if buf[i] == 0 {
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    v.push(String::from_utf8_lossy(&b).to_string());
                },
            }
        }
        res.push(v);
    }
    Ok(res)
}

pub fn write_block(strt_ind :&mut usize , res : &Vec<Vec<String>> , ctx : &Vec<(String , DataType)>, disk_out : &mut impl Write, disk_buf : &mut impl BufRead) -> Result<() , String> {
    disk_out.write_all(format!{"get block-size\n"}.as_bytes()).map_err(|e| e.to_string());
    disk_out.flush();
    let mut input = String::new();
    disk_buf.read_line(&mut input);
    let block_size : usize = input.trim().parse().expect("Parsing error");
    let mut curr_size : usize = 0;
    for i in res {
        let mut byte_row = Vec::new();
        for j in 0..i.len() {
            match ctx[j].1 {
                DataType::Int32 => {
                    let mut b = i[j].trim().parse::<i32>().expect("Error in parsing").to_le_bytes();
                    for k in b {
                        byte_row.push(k);
                    }
                },
                DataType::Int64 => {
                    let mut b = i[j].trim().parse::<i64>().expect("Error in parsing").to_le_bytes();
                    for k in b {
                        byte_row.push(k);
                    }
                },
                DataType::Float32 => {
                    let mut b = i[j].trim().parse::<f32>().expect("Error in parsing").to_le_bytes();
                    for k in b {
                        byte_row.push(k);
                    }
                },
                DataType::Float64 => {
                    let mut b = i[j].trim().parse::<f64>().expect("Error in parsing").to_le_bytes();
                    for k in b {
                        byte_row.push(k);
                    }
                },
                DataType::String => {
                    let mut b = i[j].as_bytes();
                    for k in b {
                        byte_row.push(*k);
                    }
                },
            }
        }
        if curr_size + byte_row.len() <= block_size  {
            disk_out.write_all(format!{"put block {} 1\n" , strt_ind  }.as_bytes()).map_err(|e| e.to_string());
            disk_out.flush();
            disk_out.write_all(&byte_row).map_err(|e| e.to_string());
        }
        else {
            curr_size = byte_row.len();
            *strt_ind += 1;
            disk_out.write_all(format!{"put block {} 1\n" , strt_ind  }.as_bytes()).map_err(|e| e.to_string());
            disk_out.flush();
            disk_out.write_all(&byte_row).map_err(|e| e.to_string());
        }
    }
    Ok(())
}
