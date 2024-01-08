use std::io::{BufWriter, Write};
use std::path::Path;
use std::result::Result;
use std::time::Instant;
use std::{fs::File, thread::JoinHandle};

use anyhow::{Context, Error};
use fxhash::FxHashMap;
use memmap2::MmapOptions;

type Symbol = String;

fn main() -> anyhow::Result<()> {
    let chunks = chunk_it("../measurements.txt", 12).context("unable to chunk the file")?;
    eprintln!("processing {} chunks...", chunks.len());

    let start = Instant::now();
    let sensors = process_chunks(chunks)?;
    eprintln!("processing time {:?}", start.elapsed());

    let start = Instant::now();
    let sensors = merge_results(sensors);
    eprintln!("merge took {:?}", start.elapsed());

    let start = Instant::now();
    write_results(sensors, "results.txt")?;
    eprintln!("writing result took {:?}", start.elapsed());

    Ok(())
}

// TODO make sure we are not in an escaped LF when trying to find('\n')
// cause right now that's a bug
pub fn chunk_it<P: AsRef<Path>>(path: P, nb_chunks: usize) -> Result<Vec<Chunk>, Error> {
    let file = File::open(path).context("unable to open file")?;
    let mmap = unsafe {
        MmapOptions::new()
            .map_copy_read_only(&file)
            .context("unable to mmap the file")?
    };
    let data = unsafe { std::str::from_utf8_unchecked(&mmap) };
    
    let eof = data.len();

    let mut chunks = Vec::with_capacity(nb_chunks);
    let chunk_size = eof / nb_chunks;

    let mut offset = 0;
    for _ in 0..nb_chunks {
        let mut end = offset + chunk_size;
        // if after eof then, eof
        if end >= eof {
            end = eof;
        }
        if offset == end {
            break;
        }
        // else, try to find the closest LF char
        match data[end..].find('\n') {
            Some(lf) => {
                end += lf + 1;
                chunks.push(Chunk { start: offset, end });
                offset = end;
            }
            None => {
                chunks.push(Chunk {
                    start: offset,
                    end: eof,
                });
                break;
            }
        }
    }

    Ok(chunks)
}

fn process_chunks(chunks: Vec<Chunk>) -> anyhow::Result<Vec<FxHashMap<Symbol, Sensor>>> {
    let file = File::open("../measurements.txt").expect("potato");
    let handles = chunks
        .into_iter()
        .map(|chunk| {
            let file = file.try_clone().expect("unable to clone file fd");
            std::thread::spawn(move || {
                let start = Instant::now();
                let tid = std::thread::current().id();
                let mmap = unsafe {
                    MmapOptions::new()
                        .map(&file)
                        .expect("unable to mmap the file")
                };
                mmap.advise(memmap2::Advice::Sequential)
                    .expect("mmap advise failed");
                let sensors = process_chunk(&mmap[chunk.start..chunk.end]);
                eprintln!("{tid:?} took {:?}", start.elapsed(),);
                sensors
            })
        })
        .collect::<Vec<JoinHandle<_>>>();

    let sensors = handles
        .into_iter()
        .map(|handle| handle.join().expect("unable to join thread"))
        .collect::<Vec<_>>();

    Ok(sensors)
}

fn process_chunk(data: &[u8]) -> FxHashMap<Symbol, Sensor> {
    let total = data.len();

    let mut sensors: FxHashMap<Symbol, Sensor> =
        fxhash::FxHashMap::with_capacity_and_hasher(450, Default::default());
    let mut name = String::with_capacity(25);
    let mut prev = 0;
    let mut curr = 0;
    loop {
        if curr == total {
            break;
        }

        match data[curr] {
            b';' => {
                let text = unsafe { std::str::from_utf8_unchecked(&data[prev..curr]) };
                // eprintln!("name=\"{text}\"");
                name.clear();
                name.push_str(text);
                curr += 1;
                // moving on
                prev = curr;
            }
            b'\n' => {
                let text = unsafe { std::str::from_utf8_unchecked(&data[prev..curr]) };
                let temp = text.parse::<f32>().unwrap();

                // line completed, record it
                sensors
                    .entry(name.clone())
                    .and_modify(|s| s.add_temp(temp))
                    .or_insert_with(|| Sensor::new(temp));

                // and still increment
                curr += 1;
                // moving on
                prev = curr;
            }
            _ => {
                curr += 1;
            }
        }
    }

    sensors
}

fn write_results(sensors: Vec<(Symbol, Sensor)>, path: &str) -> anyhow::Result<()> {
    let result = File::create(path).context("unable to create results.txt")?;
    let mut writer = BufWriter::new(result);
    writer.write_all(b"{")?;
    let last_index = sensors.len() - 1;
    for (index, (name, Sensor { min, sum, cnt, max })) in sensors.into_iter().enumerate() {
        writer
            .write_fmt(format_args!(
                "{name}={min:.1}/{:.1}/{max:.1}",
                sum / cnt as f32
            ))
            .context("unable to write")?;
        if index < last_index {
            writer.write_all(b", ")?;
        }
    }
    writer.write_all(b"}")?;

    Ok(())
}

fn merge_results(chunk_results: Vec<FxHashMap<Symbol, Sensor>>) -> Vec<(Symbol, Sensor)> {
    let mut all_sensors: FxHashMap<Symbol, Sensor> = fxhash::FxHashMap::default();
    for sensors in chunk_results {
        for (name, s) in sensors {
            all_sensors
                .entry(name)
                .and_modify(|sensor| sensor.merge(&s))
                .or_insert(s);
        }
    }
    let mut sensors: Vec<_> = all_sensors.into_iter().collect();
    sensors.sort_by(|(a, _), (b, _)| a.cmp(b));
    sensors
}

/// A chunk contains lines without overlapping
#[derive(Clone, Copy, Debug)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
}

struct Sensor {
    min: f32,
    sum: f32,
    cnt: usize,
    max: f32,
}

impl Sensor {
    fn new(temp: f32) -> Self {
        Self {
            cnt: 1,
            min: temp,
            max: temp,
            sum: temp,
        }
    }

    fn add_temp(&mut self, temp: f32) {
        if temp < self.min {
            self.min = temp;
        }
        if temp > self.max {
            self.max = temp;
        }
        self.sum += temp;
        self.cnt += 1;
    }

    fn merge(&mut self, sensor: &Sensor) {
        if self.min > sensor.min {
            self.min = sensor.min;
        }
        if self.max < sensor.max {
            self.max = sensor.max;
        }
        self.sum += sensor.sum;
        self.cnt += sensor.cnt;
    }
}

impl Default for Sensor {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            sum: 0.0,
            cnt: 0,
            max: f32::MIN,
        }
    }
}

impl serde::Serialize for Sensor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Sensor", 4)?;
        s.serialize_field("min", &self.min)?;
        s.serialize_field("avg", &(self.sum / self.cnt as f32))?;
        s.serialize_field("max", &self.max)?;
        s.serialize_field("count", &self.cnt)?;
        s.end()
    }
}
