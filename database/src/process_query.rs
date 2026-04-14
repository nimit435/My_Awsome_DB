use core::num;
use std::io::{BufRead, BufReader, Read, Write};
use std::num::ParseIntError;

use common::DataType;
use common::query::{ComparisionOperator, Query};
use crate::basic_func::{read_block , write_block};
use common::query;
use db_config::DbContext;
use std::collections::BTreeMap;
use common::query::Predicate;
use common::query::{ComparisionValue, SortSpec};
use std::cmp::{min, max};
pub fn master(write_ind : &mut usize , query : &query::QueryOp ,  disk_buf : &mut impl BufRead , disk_out : &mut impl Write , ctx : &DbContext) -> Result<(usize , usize , Vec<(String ,DataType)>) , String> {
    let mut input = String::new();
    match query {
        query::QueryOp::Scan(data) => {
            disk_out.write_all(format!{"get file start-block {}",data.table_id}.as_bytes()).map_err(|e|e.to_string());
            disk_out.flush();
            disk_buf.read_line(&mut input).map_err(|e|e.to_string());
            let strt_block = input.trim().parse().expect("Wrong file.");
            input.clear();
            disk_out.write_all(format!{"get file num-blocks {}" , strt_block}.as_bytes()).map_err(|e|e.to_string());
            disk_out.flush();
            disk_buf.read_line(&mut input).map_err(|e|e.to_string());
            let num_of_blocks : usize = input.trim().parse().expect("Error parsing int");
            let mut inpt = 0;
            let mut ind = 0;
            for i in ctx.get_table_specs() {
                if i.file_id == data.table_id {
                    inpt = ind;
                    break;
                }
                ind += 1;
            }
            let mut new_ctx = Vec::new();
            for i in &ctx.get_table_specs()[ind].column_specs {
                new_ctx.push((i.column_name.clone() , i.data_type.clone()));
            }
            Ok((strt_block , num_of_blocks as usize , new_ctx))
        },
        query::QueryOp::Filter(data) => {
            let ( strt_ind , num_blocks , c) = master(&mut *write_ind , &*(data.underlying), &mut *disk_buf , &mut *disk_out , ctx).unwrap();
            let (new_strt , size) = filter_fn(strt_ind, num_blocks ,  &data.predicates ,  &mut *disk_buf , &mut *disk_out , &c).unwrap();
            Ok((new_strt , size , c))
        },
        query::QueryOp::Cross(data) => {
            let ( strt_ind1 , num_blocks1 , c1) = master(&mut *write_ind , &*(data.left), &mut *disk_buf , &mut *disk_out , ctx).unwrap();
            let mut w_ind = strt_ind1 + num_blocks1 + 100;
            let ( strt_ind2 , num_blocks2 , c2) = master(&mut w_ind , &*(data.right), &mut *disk_buf , &mut *disk_out , ctx).unwrap();
            let (f_strt , f_size , f_c) = block_cross(std::cmp::max(strt_ind1 + num_blocks1 + 100 , strt_ind2 + num_blocks2 + 100) ,strt_ind1 , strt_ind2 , &c1 , &c2 , &mut *disk_buf , &mut *disk_out , num_blocks1 as u64, num_blocks2 as u64 ).unwrap();
            Ok((f_strt , f_size , f_c))
        },
        query::QueryOp::Project(data) => {
            let ( strt_ind , num_blocks , c) = master(&mut *write_ind , &*(data.underlying), &mut *disk_buf , &mut *disk_out , ctx).unwrap();
            let mut t_map = BTreeMap::new();
            for i in &data.column_name_map {
                t_map.insert(&i.0, &i.1);
            }
            let mut cid = Vec::new();
            for i in 0..c.len() {
                match t_map.get(&c[i].0) {
                    Some(data) => {
                        cid.push(i);
                    },
                    None => {

                    },
                }
            }
            let (new_strt , size , cnew) = project(strt_ind + num_blocks + 100 , &cid , strt_ind , &c , &mut *disk_buf , &mut *disk_out , num_blocks as u64).unwrap();
            Ok((new_strt , size , cnew))
        },
        query::QueryOp::Sort(data) => {
            let ( strt_ind , num_blocks , c) = master(&mut *write_ind , &*(data.underlying), &mut *disk_buf , &mut *disk_out , ctx).unwrap();
            let (new_strt , num_blocks) = sort_fn(strt_ind , num_blocks , &data.sort_specs , &mut *disk_buf , &mut *disk_out , &c).unwrap();
            Ok((new_strt , num_blocks , c))
        }
    }
}
pub fn filter_fn(start_ind: usize, block_nums: usize,  predicates: &Vec<Predicate>,  disk_buf: &mut impl BufRead, disk_out: &mut impl Write, ctx: &Vec<(String, DataType)>) -> Result<(usize, usize), String>{
    // start_ind-> where the block starts in the disk on which the preprocessed table is stored.
    // end_ind-> where the block ends in the disk on which the preprocessed table is stored.
    disk_out.write_all(format!{"get block-size\n"}.as_bytes()).map_err(|e| e.to_string()).unwrap();
    disk_out.flush().map_err(|e| e.to_string()).unwrap();

    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string()).unwrap();
    let block_size: usize = b.trim().parse().expect("Problem in in finding block-size");
    b.clear();

    let mut new_start_ind = start_ind+block_nums+1;
    let st_ind = new_start_ind;

    for i in start_ind..start_ind + block_nums {
        let mut buf = vec![0u8; block_size];
        disk_out.write_all(format!{"get block {} 1\n" , i}.as_bytes()).map_err(|e| e.to_string()).unwrap();
        disk_out.flush().map_err(|e| e.to_string()).unwrap();
        disk_buf.read_exact(&mut buf).map_err(|e| e.to_string()).unwrap();
        // Now we have the block in the buf, we can apply the predicates on it and write the output to the monitor.
        let rows = read_block(i, ctx, &mut *disk_out, &mut *disk_buf).unwrap();
        for row in rows {
            let mut flag = true;
            for predicate in predicates {
                let col_ind = ctx.iter().position(|x| x.0 == predicate.column_name).unwrap();
                let val = &row[col_ind];
                match &predicate.value {
                    ComparisionValue::Column(col_name) => {
                        let col_ind2 = ctx.iter().position(|x| x.0 == *col_name).unwrap();
                        let val2 = &row[col_ind2];
                        match &predicate.operator{
                            ComparisionOperator::EQ => {
                                if val != val2 {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val == val2 {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val <= val2 {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val < val2 {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val >= val2 {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val > val2 {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                    },
                    ComparisionValue::I32(i) => {
                        let val_i32: i32 = val.parse().expect("Error in parsing value to i32");
                        match &predicate.operator{
                            ComparisionOperator::EQ => {
                                if val_i32 != *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val_i32 == *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val_i32 <= *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val_i32 < *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val_i32 >= *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val_i32 > *i {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                    },
                    ComparisionValue::I64(i) => {
                        let val_i64: i64 = val.parse().expect("Error in parsing value to i64");
                        match &predicate.operator{
                            ComparisionOperator::EQ => {
                                if val_i64 != *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val_i64 == *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val_i64 <= *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val_i64 < *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val_i64 >= *i {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val_i64 > *i {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                    },
                    
                    ComparisionValue::F32(f) => {
                        let val_f32: f32 = val.parse().expect("Error in parsing value to f32");
                        match &predicate.operator {
                            ComparisionOperator::EQ => {
                                if val_f32 != *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val_f32 == *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val_f32 <= *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val_f32 < *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val_f32 >= *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val_f32 > *f {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                    },
                    ComparisionValue::F64(f) => {
                        let val_f64: f64 = val.parse().expect("Error in parsing value to f64");
                        match &predicate.operator {
                            ComparisionOperator::EQ => {
                                if val_f64 != *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val_f64 == *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val_f64 <= *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val_f64 < *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val_f64 >= *f {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val_f64 > *f {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                        
                    },
                    ComparisionValue::String(s) => {
                        match &predicate.operator {
                            ComparisionOperator::EQ => {
                                if val != s {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::NE => {
                                if val == s {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GT => {
                                if val <= s {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::GTE => {
                                if val < s {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LT => {
                                if val >= s {
                                    flag = false;
                                    break;
                                }
                            },
                            ComparisionOperator::LTE => {
                                if val > s {
                                    flag = false;
                                    break;
                                }
                            },
                        }
                    },
                }
            }
            if flag {
                let mut rows = Vec::new();
                rows.push(row);
                write_block(&mut new_start_ind, &rows, ctx, disk_out, disk_buf).unwrap();
                // write this row to the temporary scratch space on the disk after new_start_ind

            }
        }
    }
    Ok((st_ind, new_start_ind- st_ind+1))
}

pub fn create_sorted_runs(start_ind: usize, block_nums:usize, sort_specs: &Vec<SortSpec>, disk_buf: &mut impl BufRead, disk_out: &mut impl Write, ctx: &Vec<(String, DataType)>)-> Result<Vec<(usize, usize)>, String>{
    disk_out.write_all(format!{"get block-size\n"}.as_bytes()).map_err(|e| e.to_string()).unwrap();
    disk_out.flush().map_err(|e| e.to_string()).unwrap();

    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string()).unwrap();
    let block_size: usize = b.trim().parse().expect("Problem in in finding block-size");
    b.clear();
    

    let mut sort_keys  = Vec::new();
    for spec in sort_specs{
        let col_ind = ctx.iter().position(|x| x.0 == *spec.column_name).unwrap();
        let datat = &ctx[col_ind].1;
        let asc = spec.ascending;
        sort_keys.push((col_ind, datat, asc));
    }

    let memsize = 32*1024*1024;
    let mut res_vec = Vec::new();
    let mut i = start_ind;
    let mut new_start_ind = start_ind + block_nums + 1;
    while i < start_ind + block_nums {
        new_start_ind += 1;
        let stt = new_start_ind;
        let st = i;
        let mut rows: Vec<Vec<String>> = Vec::new();
        while (i-st)*block_size<memsize && i < start_ind + block_nums {
            let rows_in_block = read_block(i, ctx, &mut *disk_out, &mut *disk_buf).unwrap();
            for row in rows_in_block{
                rows.push(row);
            }
            i+=1;
        }

        // sort the rows based on the sort_column and ascending

        rows.sort_by(|a,b|{
            for &(col_ind, dtype, asc) in &sort_keys {
                let ord = match dtype {
                    DataType :: Int32 => {
                        a[col_ind].parse::<i32>().unwrap()
                            .cmp(&b[col_ind].parse::<i32>().unwrap())
                    }
                    DataType :: Int64 => {
                        a[col_ind].parse::<i64>().unwrap()
                            .cmp(&b[col_ind].parse::<i64>().unwrap())
                    }
                    DataType :: Float32 => {
                        a[col_ind].parse::<f32>().unwrap()
                            .partial_cmp(&b[col_ind].parse::<f32>().unwrap())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                    DataType :: Float64 => {
                        a[col_ind].parse::<f64>().unwrap()
                            .partial_cmp(&b[col_ind].parse::<f64>().unwrap())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    }
                    DataType :: String => {
                        a[col_ind].cmp(&b[col_ind])
                    }
                };
                let ord = if asc { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
                
            }
            std::cmp::Ordering::Equal
        });

        write_block(&mut new_start_ind, &rows, ctx, disk_out, disk_buf).unwrap();

        res_vec.push((stt, new_start_ind-stt +1));
    }
    Ok(res_vec)

}

pub fn one_pass(sorted_run_info: &Vec<(usize, usize)>, sort_spec: &Vec<SortSpec>, disk_buf: &mut impl BufRead, disk_out: &mut impl Write, ctx: &Vec<(String, DataType)>) -> Result<Vec<(usize, usize)>, String>{
    let number_of_runs = sorted_run_info.len();
    let memsize = 32*1024*1024;
    disk_out.write_all(format!{"get block-size\n"}.as_bytes()).map_err(|e| e.to_string()).unwrap();
    disk_out.flush().map_err(|e| e.to_string()).unwrap();

    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string()).unwrap();
    let block_size: usize = b.trim().parse().expect("Problem in in finding block-size");
    b.clear();
    
    let mut res_vec = Vec::new();
    let lst = sorted_run_info.last().unwrap();
    let mut new_start_ind = lst.0 + lst.1 + 1;

    let mut sort_keys  = Vec::new();
    for spec in sort_spec{
        let col_ind = ctx.iter().position(|x| x.0 == *spec.column_name).unwrap();
        let datat = &ctx[col_ind].1;
        let asc = spec.ascending;
        sort_keys.push((col_ind, datat, asc));
    }

    let num_of_blocks_in_mem = memsize/block_size;
    let merge = max(1, num_of_blocks_in_mem-1);

    let compare = |a: &Vec<String>, b: &Vec<String>| -> std::cmp::Ordering {
        for &(col_ind, dtype, asc) in &sort_keys {
            let ord = match dtype {
                DataType :: Int32 => {
                    a[col_ind].parse::<i32>().unwrap()
                        .cmp(&b[col_ind].parse::<i32>().unwrap())
                }
                DataType :: Int64 => {
                    a[col_ind].parse::<i64>().unwrap()
                        .cmp(&b[col_ind].parse::<i64>().unwrap())
                }
                DataType :: Float32 => {
                    a[col_ind].parse::<f32>().unwrap()
                        .partial_cmp(&b[col_ind].parse::<f32>().unwrap())
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                DataType :: Float64 => {
                    a[col_ind].parse::<f64>().unwrap()
                        .partial_cmp(&b[col_ind].parse::<f64>().unwrap())
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                DataType :: String => {
                    a[col_ind].cmp(&b[col_ind])
                }
            };
            let ord = if asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
            
        }
        std::cmp::Ordering::Equal
    };
    let mut run_id: usize = 0;
    while run_id < number_of_runs{
        let end_batch = min(run_id + merge, number_of_runs);
        let batch = &sorted_run_info[run_id..end_batch];
        let mut buffers: Vec<Vec<Vec<String>>> = Vec::new();
        let mut row_pt: Vec<usize> = Vec::new();
        let mut block_pt: Vec<usize> = Vec::new();
        let mut end_pt: Vec<usize> = Vec::new();

        for (st, num_blocks) in batch{
            let rows = read_block(*st, ctx, &mut *disk_out, &mut *disk_buf ).unwrap();
            buffers.push(rows);
            row_pt.push(0);
            block_pt.push(*st +1);
            end_pt.push(*st + *num_blocks);
        }
        let stt = new_start_ind;
        // k way merge;
        loop{
            let mut best: Option<usize> = None;
            for i in 0..buffers.len(){
                if row_pt[i] >= buffers[i].len(){
                    continue;
                }
                
                best = Some(match best{
                    None => i,
                    Some(j) => if compare(&buffers[i][row_pt[i]], &buffers[j][row_pt[j]]) == std::cmp::Ordering::Less {
                        i
                    }
                    else {
                        j
                    },
                })    
            }
            let i = match best {
                None => break,
                Some(i) => i,
            };
            let row = &buffers[i][row_pt[i]];
            let mut rows = Vec::new();
            rows.push(row.clone());
            write_block(&mut new_start_ind, &rows, ctx, &mut *disk_out, &mut *disk_buf).unwrap();
            row_pt[i] += 1;
            
            if row_pt[i]>= buffers[i].len(){
                if block_pt[i] < end_pt[i]{
                    buffers[i] = read_block(block_pt[i], ctx, &mut *disk_out, &mut *disk_buf).unwrap();
                    row_pt[i] = 0;
                    block_pt[i] += 1;
                }
            }
        }
        res_vec.push((stt, new_start_ind - stt + 1));
        run_id = end_batch;
    }

    Ok(res_vec)
}
    
pub fn sort_fn(start_ind: usize, block_nums: usize, sort_specs: &Vec<SortSpec>, disk_buf: &mut impl BufRead, disk_out: &mut impl Write, ctx: &Vec<(String, DataType)>) -> Result<(usize, usize), String>{
    let mut runs = create_sorted_runs(start_ind, block_nums, sort_specs, &mut *disk_buf, &mut *disk_out, ctx).unwrap();
    while runs.len() >1 {
        runs = one_pass(&runs, sort_specs, &mut *disk_buf, &mut *disk_out, ctx).unwrap();
    }
    Ok(runs[0])
}
pub fn block_cross(write_ind : usize , block_id1 : usize , block_id2 : usize , ctx1 : &Vec<(String ,DataType)> , ctx2 : &Vec<(String , DataType)> , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , number_of_blocks1 : u64 , number_of_blocks2 : u64) -> Result<(usize , usize , Vec<(String ,DataType)>) , String> {
    disk_out.write_all(format!{"get block-size\n"}.as_bytes()).map_err(|e| e.to_string())?;
    disk_out.flush();
    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string())?;
    let mut B = b.trim().parse().expect("Problem in in finding block-size");
    B = ((64*1024*1024) / B as usize);
    b.clear();
    let mut new_context = Vec::new();
    for i in 0..ctx1.len() {
        new_context.push(ctx1[i].clone());
    }
    for i in 0..ctx2.len() {
        new_context.push(ctx2[i].clone());
    }
    let mut strt_ind = write_ind;
    if number_of_blocks1 > number_of_blocks2 {
        for i in (0..number_of_blocks1).step_by(B - 2) {
            for k in 0..(B - 2) {
                let mut res = Vec::new();
                let v1 = read_block(block_id1 + k + i as usize * (B-2), ctx1 ,&mut *disk_out,&mut *disk_buf).unwrap();
                for j in 0..number_of_blocks2 {
                    let v2 = read_block(block_id2 + j as usize  ,ctx2  ,&mut *disk_out, &mut *disk_buf).unwrap();
                    let mut v = Vec::new();
                    for k in 0..v1.len() {
                        for c in 0..v2.len() {
                            for t in 0..v1[k].len() {
                                v.push(v1[k][t].clone());
                            }
                            for t in 0..v2[c].len() {
                                v.push(v2[c][t].clone());
                            }
                            res.push(v.clone());
                        }
                    }
                }
                write_block(&mut strt_ind , &res , &new_context , &mut *disk_out , &mut *disk_buf);
            }
        }
        Ok((write_ind , number_of_blocks1 as usize *number_of_blocks2 as usize , new_context))
    }
    else {
        for i in (0..number_of_blocks2).step_by(B - 2) {
            for k in 0..(B - 2) {
                let mut res = Vec::new();
                let v1 = read_block(block_id2 + k + i as usize * (B-2), ctx2 ,&mut *disk_out,&mut *disk_buf).unwrap();
                for j in 0..number_of_blocks1 {
                    let v2 = read_block(block_id1 + j as usize  ,ctx1  ,&mut *disk_out, &mut *disk_buf).unwrap();
                    let mut v = Vec::new();
                    for k in 0..v2.len() {
                        for c in 0..v1.len() {
                            for t in 0..v1[k].len() {
                                v.push(v1[k][t].clone());
                            }
                            for t in 0..v2[c].len() {
                                v.push(v2[c][t].clone());
                            }
                            res.push(v.clone());
                        }
                    }
                }
                write_block( &mut strt_ind, &res , &new_context , &mut *disk_out , &mut *disk_buf);
            }
        }
        Ok((write_ind , strt_ind - write_ind + 1 , new_context))
    }
}   

pub fn project(write_ind : usize , column_ids : &Vec<usize> , read_ind : usize , ctx : &Vec<(String , DataType)> , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , number_of_blocks : u64) -> Result<(usize , usize , Vec<(String , DataType)>) , String> {
    let mut block_fill = 0;
    let mut input = String::new();
    disk_out.write_all(format!{"get block-size"}.as_bytes()).map_err(|e| e.to_string());
    disk_out.flush().map_err(|e|e.to_string());
    disk_buf.read_line(&mut input).map_err(|e|e.to_string());
    let block_size :usize = input.trim().parse().expect("Not a real integer for block_size.");
    let mut strt_ind = write_ind;
    let mut new_context = Vec::new();
    for i in 0..column_ids.len() {
        new_context.push(ctx[column_ids[i]].clone());
    }
    for i in 0..number_of_blocks {
        let mut vec = read_block(read_ind + i as usize, ctx , &mut *disk_out , &mut *disk_buf).unwrap();
        let mut v : Vec<Vec<String>> = Vec::new();
        for j in 0..vec.len() {
            let mut vi = Vec::new();
            for k in 0..new_context.len() {
                vi.push(vec[j][k].clone());
            }
            v.push(vi);
        }
        write_block(&mut strt_ind , &v , &new_context , &mut *disk_out , &mut *disk_buf);
    }
    Ok((write_ind , strt_ind - write_ind + 1 , ctx.clone()))
}