use std::{
    env,
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    process::exit,
    thread,
};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;

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
        if temp_size + line_len < chunk_size_limit {
            temp_lines.push(line);
            temp_size += line_len
        } else {
            // handle it: sort, then write to disk
            let path = format!("/tmp/{:05}.chunk", chunk_index);
            chunk_paths.push(path.clone());
            let handle = thread::spawn(move || {
                // write_chunk(&path, temp_lines);
            });
            handles.push(handle);

            temp_lines = Vec::new();
            temp_size = 0;
            chunk_index += 1;
        }
    }

    for h in handles {
        h.join().unwrap();
    }
    println!("split chunks success");

    // merge
    // 1. collect readers
    let mut readers: Vec<BufReader<File>> = Vec::new();
    for chunk_path in chunk_paths.iter() {
        let input = File::open(chunk_path)?;
        let buffered = BufReader::new(input);
        readers.push(buffered);
    }

    // 2. init bucket
    let mut buckets: Vec<Vec<String>> = Vec::new();
    let bucket_size = 100;
    for reader in readers.iter_mut() {
        let mut b = Vec::new();
        for line in reader
            .lines()
            .take(bucket_size)
            .into_iter()
            .map(|l| l.unwrap())
        {
            b.push(line);
        }
        buckets.push(b);
    }

    // 3. init headers
    let mut headers: Vec<String> = Vec::new();
    for b in buckets.iter_mut() {
        headers.push(b.pop().unwrap());
    }

    // 4. merge
    let dst_path = format!("{}.sorted", src_path);
    let mut dst = File::create(&dst_path).unwrap();
    let bucket_count = buckets.len();
    let mut min: &str = "999999999999999";
    let mut min_slot: usize = 0;
    let mut empty_chunk_count = 0;
    loop {
        // found min str
        for i in 0..bucket_count {
            if headers[i].as_str() < min {
                min = &headers[i];
                min_slot = i;
            }
        }

        // println!("headers: {:#?}", headers);
        // println!("min: {}", min);
        // println!("min slot: {}", min_slot);

        // write to dst
        dst.write_all(min.as_bytes()).unwrap();
        dst.write(b"\n").unwrap();
        // println!("write: {}", min);
        min = "";
        // break;

        match buckets[min_slot].pop() {
            Some(s) => {
                headers[min_slot] = s;
            }
            None => {
                // read from chunk file
                for line in readers.get_mut(min_slot).unwrap().lines().take(bucket_size) {
                    match line {
                        Ok(s) => {
                            // push to bucket
                            buckets[min_slot].push(s);
                        }
                        Err(_) => {
                            headers[min_slot] = "9".to_string();
                            empty_chunk_count += 1;
                        }
                    }
                }

                if empty_chunk_count == bucket_count {
                    break;
                }
            }
        }
    }

    Ok(())
}

fn write_chunk(chunk_path: &str, mut lines: Vec<String>) {
    lines.sort();
    let mut chunk = File::create(&chunk_path).unwrap();
    for mut l in lines {
        l.push('\n');
        chunk.write_all(l.as_bytes()).unwrap();
    }
    println!("write to {}", chunk_path);
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::{fs::File, io::Write};

    #[test]
    fn generate() {
        let count = 100 * 10000;
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
