// benzhutiandbenqingqin😀😀😀😀glovetogetherandmakeasmallpigandsmallchikenfamilyhavemuchloveandsunnyandwarm

use std::{
    collections::VecDeque,
    env,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    process::exit,
    thread,
};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;
const MAX_STRING: &'static str = "9999999999999";

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: rsort <INPUT>");
        exit(1);
    }

    let src_path = &args[1];
    let chunk_size_limit: usize = 50 * KB;
    let input = File::open(src_path)?;
    let buffered = BufReader::new(input);
    let mut temp_size: usize = 0;
    let mut temp_lines = Vec::new();
    let mut chunk_index = 0;
    let mut handles = Vec::new();
    let mut chunk_paths = Vec::new();
    for line in buffered.lines().map(|l| l.unwrap()) {
        let line_len = line.len();
        temp_lines.push(line);
        temp_size += line_len;
        if temp_size > chunk_size_limit {
            // handle it: sort, then write to disk
            let path = format!("/tmp/{:05}.chunk", chunk_index);
            chunk_paths.push(path.clone());
            let handle = thread::spawn(move || {
                write_chunk(&path, temp_lines);
            });
            handles.push(handle);

            temp_lines = Vec::new();
            temp_size = 0;
            chunk_index += 1;
        }
    }

    // write remaining lines
    println!("ramaining len: {}", temp_lines.len());
    if temp_lines.len() > 0 {
        let path = format!("/tmp/{:05}.chunk", chunk_index);
        let handle = thread::spawn(move || {
            write_chunk(&path, temp_lines);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    // merge
    // 1. collect readers
    let mut readers: Vec<BufReader<File>> = Vec::new();
    for chunk_path in chunk_paths.iter() {
        let input = File::open(chunk_path)?;
        let buffered = BufReader::new(input);
        readers.push(buffered);
    }

    // 2. init bucket
    let mut buckets: Vec<VecDeque<String>> = Vec::new();
    let bucket_size = 100;
    for reader in readers.iter_mut() {
        let mut b = VecDeque::with_capacity(bucket_size);
        for line in reader
            .lines()
            .take(bucket_size)
            .into_iter()
            .map(|l| l.unwrap())
        {
            b.push_back(line);
        }
        buckets.push(b);
    }

    // 3. init headers
    let mut headers: Vec<String> = Vec::new();
    for b in buckets.iter_mut() {
        headers.push(b.pop_front().unwrap());
    }

    // 4. merge
    let dst_path = format!("{}.sorted", src_path);
    let mut dst = File::create(&dst_path).unwrap();
    let bucket_count = buckets.len();
    let mut min_slot: usize = 0;
    let mut empty_bucket_count = 0;
    let mut min: &str;
    loop {
        min = MAX_STRING;

        // found min str
        for i in 0..bucket_count {
            if headers[i].as_str() < min {
                min = &headers[i];
                min_slot = i;
            }
        }

        // write to dst
        println!("write {} from bucket {}", min, min_slot);
        dst.write_all(min.as_bytes()).unwrap();
        dst.write(b"\n").unwrap();
        min = "";

        match buckets[min_slot].pop_front() {
            Some(s) => {
                headers[min_slot] = s;
            }
            None => {
                println!("bucket {} is empty, read from file", min_slot);
                let mut iter = readers.get_mut(min_slot).unwrap().lines().peekable();

                // read from chunk file
                for line in iter.by_ref().take(bucket_size).map(|l| l.unwrap()) {
                    // push to bucket
                    buckets[min_slot].push_back(line);
                    println!("???");
                    exit(1);
                }

                if buckets[min_slot].len() == 0 {
                    println!("bucket {} is empty", min_slot);
                    headers[min_slot] = MAX_STRING.to_string();
                    empty_bucket_count += 1;

                    if empty_bucket_count == bucket_count {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn write_chunk(chunk_path: &str, mut lines: Vec<String>) {
    lines.sort();
    let mut chunk = File::create(&chunk_path).unwrap();
    println!("write to {}, lines: {}", chunk_path, lines.len());
    for mut l in lines {
        l.push('\n');
        chunk.write_all(l.as_bytes()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::{fs::File, io::Write};

    #[test]
    fn generate() {
        let count = 10000;
        let mut rng = rand::thread_rng();
        let path = "/tmp/source";
        let mut file = File::create(path).unwrap();
        let mut s: String;
        for _i in 0..count {
            s = format!("{:012}\n", rng.gen::<u32>());
            file.write_all(s.as_bytes()).unwrap();
        }
        println!("write source success, count: {}", count)
    }
}