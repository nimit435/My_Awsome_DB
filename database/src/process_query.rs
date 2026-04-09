use std::io::{BufRead, BufReader, Read, Write};
use std::num::ParseIntError;
pub fn block_cross(start_ind : usize , file_id1 : &String , file_id2 : &String , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , ctx : &DbContext) -> Result<Vec<Vec<String>>,String> {
    disk_out.write_all(format!{"get block-size\n"}.as_bytes).map_err(|e| e.to_string())?;
    disk_out.clear();
    let mut b = String::new();
    disk_buf.read_line(&mut b).map_err(|e| e.to_string())?;
    let mut B = b.trim().parse().expect("Problem in in finding block-size");
    B = ((64*1024*1024) / B as usize);
    b.clear();
    let mut i = 0;
    disk_out.write_all(format!{"get start-block {}\n" , file_id1}.as_bytes).map_err(|e| e.to_string())?;
    disk_buf.read_line(&mut b).map_err(|e| e.to_string())?;
    let strt_1 : usize = b.trim().parse().expect("Error : invalid filed id.");
    b.clear();
    disk_out.write_all(format!{"get start-block {}\n" , file_id2}.as_bytes).map_err(|e| e.to_string())?;
    disk_buf.read_line(&mut b).map_err(|e| e.to_string())?;
    let strt_2 : usize = b.trim().parse().expect("Error : invalid filed id.");
    disk_out.write_all(format!{})
}   