use std::{
    fs::File,
    io::{BufWriter, Write},
    result::Result,
    sync::mpsc::channel,
};

use anyhow::Context;
use chunkit::Chunkit;
use fxhash::FxHashMap;
use rayon::prelude::*;

fn main() -> anyhow::Result<()> {
    let chunks = Chunkit::open("../measurements.txt").context("unable to chunk file")?;

    let (sender, receiver) = channel();

    chunks.par_iter().for_each_with(sender, |sender, chunk| {
        let data = chunks.data(*chunk);
        let mut sensors: FxHashMap<&str, Sensor> = fxhash::FxHashMap::default();

        data.lines()
            .map(|line| line.split_once(';').unwrap())
            .map(|(name, temp)| (name, temp.parse::<f32>().unwrap()))
            .for_each(|(name, temp)| {
                sensors
                    .entry(name)
                    .and_modify(|sensor| sensor.add_temp(temp))
                    .or_insert_with(|| Sensor {
                        cnt: 1,
                        min: temp,
                        max: temp,
                        sum: temp,
                    });
            });

        sender.send(sensors).expect("unable to send");
    });

    let mut all_sensors: FxHashMap<&str, Sensor> = fxhash::FxHashMap::default();
    for sensors in receiver.into_iter() {
        for (name, s) in sensors {
            all_sensors
                .entry(name)
                .and_modify(|sensor| sensor.merge(&s))
                .or_insert(s);
        }
    }

    let mut sensors: Vec<_> = all_sensors.into_iter().collect();
    let last_index = sensors.len() - 1;
    sensors.sort_by(|(a, _), (b, _)| a.cmp(b));

    let result = File::create("results.csv").context("unable to create result.csv")?;
    let mut writer = BufWriter::new(result);
    writer.write_all(b"{")?;
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

struct NamedSensor<'a> {
    name: &'a str,
    min: f32,
    max: f32,
    avg: f32,
}

struct Sensor {
    min: f32,
    sum: f32,
    cnt: usize,
    max: f32,
}

impl Sensor {
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
