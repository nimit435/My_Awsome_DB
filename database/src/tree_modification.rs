/*
This code is there to take a QueryOp tree and convert
it into modified tree which will be better to process
it and reduce io operations and time complexity
*/
use common::query::{FilterData, QueryOp, ScanData, ComparisionOperator, ComparisionValue, Predicate, Query};
use db_config::{DbContext, table::TableSpec, table::ColumnSpec};
use common::DataType;
use serde_json;

fn final_modify_split(tree: &mut QueryOp, db_context: &DbContext) {
    match tree {
        QueryOp::Filter(data) => {
            if data.predicates.len() > 1 {
                let (head, _tail) = split_filter(tree);
                *tree = head;
            }

            if let QueryOp::Filter(data) = tree {
                final_modify_split(&mut *data.underlying, db_context);
            }
        }
        QueryOp::Project(data) => final_modify_split(&mut *data.underlying, db_context),
        QueryOp::Sort(data) => final_modify_split(&mut *data.underlying, db_context),
        QueryOp::Cross(data) => {
            final_modify_split(&mut *data.left, db_context);
            final_modify_split(&mut *data.right, db_context);
        }
        QueryOp::Scan(_) => {}
    }
}

fn has_column(db_context: &DbContext, table_id: &str, column_name: &str) -> bool {
    for table_spec in db_context.get_table_specs() {
        if table_spec.name == table_id {
            for column_spec in &table_spec.column_specs {
                if column_spec.column_name == column_name {
                    return true;
                }
            }
        }
    }
    false
}

fn move_filter(tree: &mut QueryOp, db_context: &DbContext) {
    match tree {
        QueryOp::Filter(data) => {
            if let QueryOp::Filter(_) = &*data.underlying {
                let temp_query = (*data.underlying).clone();
                if let QueryOp::Filter(temp) = temp_query {
                    *data.underlying = (*temp.underlying).clone();

                    let mut v = Vec::new();
                    dfs(tree, &mut v, db_context, temp.predicates[0].column_name.clone());
                    for a_ptr in v {
                        let a = unsafe { &mut *a_ptr };
                        if let QueryOp::Filter(a_data) = a {
                            let mut t = temp.clone();
                            t.underlying = a_data.underlying.clone();
                            *a_data.underlying = QueryOp::Filter(t);
                        }
                    }
                    return;
                }
            }
            move_filter(&mut *data.underlying, db_context);
        }
        QueryOp::Project(data) => move_filter(&mut *data.underlying, db_context),
        QueryOp::Sort(data) => move_filter(&mut *data.underlying, db_context),
        QueryOp::Cross(data) => {
            move_filter(&mut *data.left, db_context);
            move_filter(&mut *data.right, db_context);
        }
        QueryOp::Scan(_) => {}
    }
}

fn dfs(tree: &mut QueryOp, vec: &mut Vec<*mut QueryOp>, db_context: &DbContext, c_name: String) {
    match tree {
        QueryOp::Scan(_) => {},
        QueryOp::Filter(data) => {
            if let QueryOp::Scan(scan_data) = &*data.underlying {
                if has_column(db_context, &scan_data.table_id, &c_name) {
                    vec.push(tree as *mut QueryOp);
                    return;
                }
            }
            dfs(&mut *data.underlying, vec, db_context, c_name.clone());
        },
        QueryOp::Project(data) => {
            if let QueryOp::Scan(scan_data) = &*data.underlying {
                if has_column(db_context, &scan_data.table_id, &c_name) {
                    vec.push(tree as *mut QueryOp);
                    return;
                }
            }
            dfs(&mut *data.underlying, vec, db_context, c_name.clone());
        },
        QueryOp::Sort(data) => {
            if let QueryOp::Scan(scan_data) = &*data.underlying {
                if has_column(db_context, &scan_data.table_id, &c_name) {
                    vec.push(tree as *mut QueryOp);
                    return;
                }
            }
            dfs(&mut *data.underlying, vec, db_context, c_name.clone());
        },
        QueryOp::Cross(data) => {
            if let QueryOp::Scan(scan_data) = &*data.left {
                if has_column(db_context, &scan_data.table_id, &c_name) {
                    vec.push(tree as *mut QueryOp);
                    return;
                }
            }
            if let QueryOp::Scan(scan_data) = &*data.right {
                if has_column(db_context, &scan_data.table_id, &c_name) {
                    vec.push(tree as *mut QueryOp);
                    return;
                }
            }
            dfs(&mut *data.left, vec, db_context, c_name.clone());
            dfs(&mut *data.right, vec, db_context, c_name.clone());
        },
    }
}
fn split_filter(node: &QueryOp) -> (QueryOp, QueryOp) {
    if let QueryOp::Filter(data) = node {
        if data.predicates.len() > 1 {
            let original_underlying = (*data.underlying).clone();
            let last_predicate = data.predicates.iter().rev().next().unwrap().clone();
            
            let tail = QueryOp::Filter(FilterData {
                predicates: vec![last_predicate],
                underlying: Box::new(original_underlying.clone()),
            });
            
            let mut curr = tail.clone();
            for predicate in data.predicates.iter().rev().skip(1) {
                curr = QueryOp::Filter(FilterData {
                    predicates: vec![predicate.clone()],
                    underlying: Box::new(curr),
                });
            }
            
            return (curr, tail); 
        }
    }
    (node.clone(), node.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_modification_from_json() {
        // Sample JSON representing a query with a filter having multiple predicates
        let json = r#"
        {
            "root": {
                "Filter": {
                    "predicates": [
                        {
                            "column_name": "age",
                            "operator": "GT",
                            "value": {"I32": 18}
                        },
                        {
                            "column_name": "age",
                            "operator": "LT",
                            "value": {"I32": 65}
                        }
                    ],
                    "underlying": {
                        "Scan": {
                            "table_id": "users"
                        }
                    }
                }
            }
        }
        "#;

        // Deserialize the query
        let mut query: Query = serde_json::from_str(json).expect("Failed to deserialize query");
        println!("Original query: {:#?}", query);

        // Create a sample DbContext
        let column_spec = ColumnSpec {
            column_name: "age".to_string(),
            data_type: DataType::Int32,
            stats: None,
        };
        let table_spec = TableSpec {
            name: "users".to_string(),
            file_id: "users.db".to_string(),
            column_specs: vec![column_spec],
        };
        let db_context = DbContext::from(vec![table_spec]).unwrap();

        // Apply final_modify_split to split filters
        final_modify_split(&mut query.root, &db_context);
        println!("After split_filter: {:#?}", query);

        // Apply move_filter to push down filters
        move_filter(&mut query.root, &db_context);
        println!("After move_filter: {:#?}", query);
    }
}

