use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread::ScopedJoinHandle;
use std::time::Instant;

use anyhow::{anyhow, Context, Error, Result};
use memmap2::MmapOptions;

type Symbol = String;
type HashMap = ahash::AHashMap<Symbol, Sensor>;

fn main() -> Result<()> {
    let nb_threads = num_cpus::get();
    let file = File::open("../measurements.txt").context("unable to open file")?;
    let mmap = unsafe {
        MmapOptions::new()
            .map(&file)
            .context("unable to mmap the file")?
    };
    // mmap.advise(memmap2::Advice::Sequential)
    //     .context("unable to set mmap advice sequential")?;

    let chunks = chunk_it(&mmap, nb_threads).context("unable to chunk the file")?;
    eprintln!("processing {} chunks...", chunks.len());

    let start = Instant::now();
    let sensors = process_chunks(&chunks)?;
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
pub fn chunk_it(buf: &[u8], nb_chunks: usize) -> Result<Vec<Chunk<'_>>, Error> {
    let data = unsafe { std::str::from_utf8_unchecked(buf) };

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
                chunks.push(Chunk {
                    data: &buf[offset..end],
                    start: offset,
                    end,
                });
                offset = end;
            }
            None => {
                chunks.push(Chunk {
                    data: &buf[offset..end],
                    start: offset,
                    end: eof,
                });
                break;
            }
        }
    }

    Ok(chunks)
}

fn process_chunks(chunks: &[Chunk<'_>]) -> Result<Vec<HashMap>> {
    let sensors = std::thread::scope(move |ctx| {
        let handles = chunks
            .iter()
            .map(|chunk| {
                ctx.spawn(move || {
                    let start = Instant::now();
                    let tid = std::thread::current().id();
                    let sensors = process_chunk(chunk);
                    eprintln!("{tid:?} took {:?}", start.elapsed());
                    sensors
                })
            })
            .collect::<Vec<ScopedJoinHandle<_>>>();

        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|err| anyhow!("unable to join the thread ({err:?})"))?
            })
            .collect::<Result<Vec<_>>>()
    })?;

    Ok(sensors)
}

fn process_chunk(chunk: &Chunk<'_>) -> Result<HashMap> {
    let total = chunk.data.len();

    let mut sensors = HashMap::with_capacity(450);
    let mut name = String::with_capacity(25);
    let mut prev = 0;
    let mut curr = 0;
    loop {
        if curr == total {
            break;
        }

        match chunk.data[curr] {
            b';' => {
                let text = unsafe { std::str::from_utf8_unchecked(&chunk.data[prev..curr]) };
                // eprintln!("name=\"{text}\"");
                name.clear();
                name.push_str(text);
                curr += 1;
                // moving on
                prev = curr;
            }
            b'\n' => {
                let text = unsafe { std::str::from_utf8_unchecked(&chunk.data[prev..curr]) };
                let temp = text.parse::<f32>().context("unable to parse float")?;

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

    Ok(sensors)
}

fn write_results(sensors: Vec<(Symbol, Sensor)>, path: &str) -> Result<()> {
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

fn merge_results(chunk_results: Vec<HashMap>) -> Vec<(Symbol, Sensor)> {
    let mut all_sensors = HashMap::default();
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
pub struct Chunk<'a> {
    pub data: &'a [u8],
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
