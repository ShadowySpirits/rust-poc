use clap::Parser;
use hdrhistogram::Histogram;
use mmap_rs::{MmapFlags, MmapOptions};
use parquet::file::reader::Length;
use rand::random;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::OpenOptions;
use std::process::exit;
use std::time::{Duration, SystemTime};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Unit is KB
    #[arg(short, long, default_value_t = 16)]
    bs: usize,

    #[arg(short, long, default_value_t = 1)]
    io_depth: usize,

    /// Unit is MB
    #[arg(short, long, default_value_t = 1024)]
    file_size: usize,

    #[arg(short, long, required = true)]
    path: String,

    /// Unit is second
    #[arg(short, long, default_value_t = 10)]
    duration: u64,
}

fn main() {
    let args = Args::parse();

    println!("page size: {}", MmapOptions::page_size());
    if args.bs * 1024 % MmapOptions::page_size() != 0 {
        eprintln!("Argument 'bs' must be a multiple of the page size.");
        exit(1);
    }

    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(args.path)
        .unwrap();
    file.set_len((args.file_size * 1024 * 1024) as u64).unwrap();

    let mut mapping = unsafe {
        MmapOptions::new(args.file_size * 1024 * 1024)
            .unwrap()
            .with_flags(MmapFlags::SHARED)
            .with_file(&file, 0)
            .map_mut()
            .unwrap()
    };

    assert_ne!(mapping.as_ptr(), std::ptr::null());

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.io_depth)
        .build_global()
        .unwrap();

    let mut bytes = 0u64;
    let mut io = 0u64;
    let mut hist = Histogram::<u64>::new(2)
        .unwrap()
        .into_sync();

    let start = SystemTime::now();
    let size: usize = args.bs * 1024 * args.io_depth;
    let mut offset: usize = 0;
    loop {
        if start.elapsed().unwrap().as_secs() > args.duration {
            break;
        }

        if offset >= file.len() as usize {
            offset %= file.len() as usize
        }

        // Write data to mmap buffer.
        let byte = random::<u8>();
        for i in offset..offset + size {
            mapping[i] = byte;
        }

        let mut flush_request_vec = Vec::with_capacity(args.io_depth);

        for _ in 0..args.io_depth {
            flush_request_vec.push(offset..offset + args.bs * 1024);
            offset += args.bs * 1024;
        }

        // Flush dirty pages to disk.
        flush_request_vec.into_par_iter().for_each_init(
            || hist.recorder(),
            |hist, range| {
                let now = SystemTime::now();
                mapping.flush(range.clone()).unwrap();
                hist.record(now.elapsed().unwrap().as_nanos() as u64)
                    .unwrap();
            },
        );

        bytes += size as u64;
        io += args.io_depth as u64;
    }

    let elapsed = start.elapsed().unwrap().as_secs();
    hist.refresh();
    println!(
        "Test done: cost {}s, throughput: {}MB/s, iops: {}, latency avg: {:?}, latency pt99: {:?}, latency pt999: {:?}, max: {:?}",
        elapsed,
        bytes / 1024 / 1024 / elapsed,
        io / elapsed,
        Duration::from_nanos(hist.mean() as u64),
        Duration::from_nanos(hist.value_at_quantile(0.99)),
        Duration::from_nanos(hist.value_at_quantile(0.999)),
        Duration::from_nanos(hist.max())
    );
}
