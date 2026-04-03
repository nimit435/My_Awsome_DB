use common::{QueryOp, ScanData, FilterData};
use db_config::DbContext;
use std::collections::HashSet;

fn move_select(tree: &mut QueryOp, db_context: &DbContext) {
    if let QueryOp::Filter(filter_data) = tree {
        if let QueryOp::Scan(scan_data) = &*filter_data.underlying {
            if can_push_filter_to_scan(scan_data, filter_data, db_context) {
                return;
            }
        }
    }

    match tree {
        QueryOp::Filter(filter_data) => {
            move_select(&mut *filter_data.underlying, db_context);
        },
        QueryOp::Project(project_data) => {
            move_select(&mut *project_data.underlying, db_context);
        },
        QueryOp::Sort(sort_data) => {
            move_select(&mut *sort_data.underlying, db_context);
        },
        QueryOp::Cross(cross_data) => {
            move_select(&mut *cross_data.left, db_context);
            move_select(&mut *cross_data.right, db_context);
        },
        QueryOp::Scan(_) => {}
    }
}

fn can_push_filter_to_scan(scan_data: &ScanData, filter_data: &FilterData, db_context: &DbContext) -> bool {
    if let Some(table_spec) = db_context.get_table_by_name(&scan_data.table_id) {
        let table_columns: HashSet<&String> = table_spec
            .column_specs
            .iter()
            .map(|col| &col.column_name)
            .collect();

        return filter_data.predicates.iter().all(|pred| {
            table_columns.contains(&pred.column_name)
        });
    }
    false
}

fn split_filter(node: &mut QueryOp) -> QueryOp {
    if let QueryOp::Filter(filter_data) = node {
        if filter_data.predicates.len() > 1 {
            let mut current_op = QueryOp::Scan(ScanData { table_id: String::new() });

            for predicate in &filter_data.predicates {
                current_op = QueryOp::Filter(FilterData {
                    predicates: vec![predicate.clone()],
                    underlying: Box::new(current_op),
                });
            }

            return current_op;
        }
    }
    node.clone()
}

