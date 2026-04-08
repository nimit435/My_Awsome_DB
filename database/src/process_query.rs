pub fn block_join(file_id1 : &String , file_id2 : &String , disk_buf : &mut impl BufRead , disk_out : &mut impl Write , ctx : &DbContext) -> Result<Vec<Vec<String>>,String> {
    disk_out.write_all(format!("get block-size").as_bytes).map
}