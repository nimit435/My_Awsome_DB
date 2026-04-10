use std::io::{BufRead, BufReader, Read, Write};
use std::num::ParseIntError;

use common::DataType;
use common::query::ComparisionOperator;
use crate::basic_func;
pub fn block_cross(write_ind : usize , column_id1 : usize , column_id2 : usize , read_ind1 : usize , read_ind2 : usize, ctx : &Vec<DataType> , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , number_of_blocks : u64) -> Result<(usize , usize) , String> {
    disk_out.write_all(format!{"get block-size\n"}.as_bytes).map_err(|e| e.to_string())?;
    disk_out.clear();
    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string())?;
    let mut B = b.trim().parse().expect("Problem in in finding block-size");
    B = ((64*1024*1024) / B as usize);
    b.clear();
    
}   

pub fn project(write_ind : usize , column_id : usize , read_ind : usize , ctx : &Vec<DataType> , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , number_of_blocks : u64) -> Result<(usize , usize) , String> {
    let mut block_fill = 0;
    let mut input = String::new();
    disk_out.write_all(format!{"get block-size"}.as_bytes()).map_err(|e| e.to_string());
    disk_out.flush().map_err(|e|e.to_string());
    disk_buf.read_line(&mut input).map_err(|e|e.to_string());
    let block_size = input.trim().parse().expect("Not a real intger for block_size.");
    let mut strt_ind = write_ind;
    let new_context = {ctx[column_id]};
    for i in 0..number_of_blocks {
        let mut vec = basic_func::read_block(read_ind + i as usize, ctx , &mut *disk_out , &mut *disk_buf).unwrap();
        let mut v : Vec<u8> = Vec::new();
        for j in 0..vec.len() {
            match ctx[column_id] {
                DataType::Int32 => {
                    let data_bytes = vec[j][column_id].trim().parse::<i32>().expect("failed to parse").to_le_bytes();
                    for k in 0..data_bytes.len() {
                        v.push(data_bytes[k]);
                    }
                },
                DataType::Float32 => {
                    let data_bytes = vec[j][column_id].trim().parse::<f32>().expect("failed to parse").to_le_bytes();
                    for k in 0..data_bytes.len() {
                        v.push(data_bytes[k]);
                    }
                },
                DataType::Int64 => {
                    let data_bytes = vec[j][column_id].trim().parse::<i64>().expect("failed to parse").to_le_bytes();
                    for k in 0..data_bytes.len() {
                        v.push(data_bytes[k]);
                    }
                },
                DataType::Float64 => {
                    let data_bytes = vec[j][column_id].trim().parse::<f64>().expect("failed to parse").to_le_bytes();
                    for k in 0..data_bytes.len() {
                        v.push(data_bytes[k]);
                    }
                },
                DataType::String => {
                    let data_bytes = vec[j][column_id].as_bytes();
                    for k in 0..data_bytes.len() {
                        v.push(data_bytes[k]);
                    }
                },
            }
        }
        write_block(&mut strt_ind , &v , &new_context , &mut *disk_out , &mut *disk_buf);
    }
    Ok((write_ind , strt_ind - write_ind + 1))
}