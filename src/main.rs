use std::fs::File;
use std::io::{BufWriter, Write};
use std::result::Result;
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::Context;
use chunkit::chunk_it;
use fxhash::FxHashMap;
use memmap2::MmapOptions;

fn main() -> anyhow::Result<()> {
    let chunks = chunk_it("../measurements.txt", 12).context("unable to chunk the file")?;
    eprintln!("processing {} chunks:\n{chunks:?}", chunks.len());

    let handles = chunks
        .into_iter()
        .map(|chunk| {
            std::thread::spawn(move || {
                let tid = std::thread::current().id();
                let start = Instant::now();
                eprintln!("{tid:?} spawned");
                let file = File::open("../measurements.txt").expect("unable to open file");
                let buf = unsafe {
                    MmapOptions::new()
                        .map_copy_read_only(&file)
                        .expect("unable to mmap the file")
                };
                let sensors = process_chunk(&buf[chunk.start..chunk.end]);
                eprintln!("{tid:?} done in {:?}", start.elapsed());
                sensors
            })
        })
        .collect::<Vec<JoinHandle<_>>>();

    let mut sensors = Vec::new();
    for handle in handles {
        let result = handle.join().expect("unable to join thread");
        sensors.push(result);
    }

    let start = Instant::now();
    let sensors = merge_results(sensors);
    eprintln!("merge took {:?}", start.elapsed());

    let start = Instant::now();
    let result = File::create("results.txt").context("unable to create results.txt")?;
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
    eprintln!("writing result took {:?}", start.elapsed());

    Ok(())
}

fn merge_results(chunk_results: Vec<FxHashMap<Box<str>, Sensor>>) -> Vec<(Box<str>, Sensor)> {
    let mut all_sensors: FxHashMap<Box<str>, Sensor> = fxhash::FxHashMap::default();
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

fn process_chunk(data: &[u8]) -> FxHashMap<Box<str>, Sensor> {
    let eof = data.len();

    let mut sensors: FxHashMap<Box<str>, Sensor> = fxhash::FxHashMap::default();
    let mut name = String::new();
    let mut prev = 0;
    let mut curr = 0;
    loop {
        if curr == eof {
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
                    .entry(name.clone().into_boxed_str())
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
