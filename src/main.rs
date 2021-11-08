use petgraph::Graph;
use petgraph::algo::dijkstra;
use petgraph::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use serde::{ Deserialize, Serialize };

use dhat::{Dhat, DhatAlloc};

#[global_allocator]
static ALLOCATOR: DhatAlloc = DhatAlloc;

#[derive(Debug, Deserialize)]
struct Record {
    buyer: i32,
    seller: i32,
    count: i32,
    value: i32,
}

#[derive(Debug, Serialize)]
struct WriteRecord {
    buyer: i32,
    seller: i32,
}

fn run() -> Result<(), Box<dyn Error>> {
    let _dhat = Dhat::start_heap_profiling();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path("net.csv").unwrap();

    let mut graph= Graph::new_undirected();
    {
        let mut node_map : HashMap<i32, NodeIndex<u32>> = HashMap::default();
        for result in rdr.deserialize() {
            let record: Record = result?;
            let buyer = record.buyer;
            let seller = record.seller;

            if !node_map.contains_key(&buyer) {
                let node = graph.add_node(buyer);
                node_map.insert(buyer,  node);
            }

            if !node_map.contains_key(&seller) {
                let node = graph.add_node(seller);
                node_map.insert(seller,  node);
            }

            graph.add_edge(node_map[&buyer], node_map[&seller], 1);
        }
    }

    for node_index in graph.node_indices() {
        let distance = dijkstra(&graph, node_index, None, |e| *e.weight());
        let distance= distance 
            .iter()
            .filter(|&(_, v)| *v <= 2)
            .map(|(n, _)| graph[*n])
            .collect::<Vec<_>>();
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_path(format!("output/{}.csv", graph[node_index])).unwrap();
        
        for dist in distance {
            wtr.serialize(WriteRecord { buyer: graph[node_index], seller: dist })?;
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    run()?;
    Ok(())
}
