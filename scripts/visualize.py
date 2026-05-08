#!/usr/bin/env python3
"""
Simple Matplotlib visualizer for Chicago Air Quality outputs.
Usage:
  python scripts/visualize.py [--copy-from-container] [--show] [--outdir=plots] [--sensor=SENSOR_ID]

If --copy-from-container is given, the script will run:
  docker cp hadoop-master:/dashboard/spark_out ./dashboard_spark_out
  docker cp hadoop-master:/dashboard/rolling_out ./dashboard_rolling_out
and then read CSVs from those local folders.

Outputs:
  - plots/per_sensor_top15.png
  - plots/daily_pm25.png
  - plots/anomalies.png
  - plots/rolling_<sensor>.png
"""
import argparse
import glob
import os
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def docker_copy(src, dst):
    print(f"Copying from container: {src} -> {dst}")
    try:
        subprocess.check_call(["docker", "cp", src, dst])
    except subprocess.CalledProcessError as e:
        print("docker cp failed:", e)
        sys.exit(1)


def find_part_file(dirpath):
    p = Path(dirpath)
    if not p.exists():
        return None
    # prefer part- files, then any csv
    parts = list(p.glob('part-*.csv'))
    if parts:
        return parts[0]
    csvs = list(p.glob('*.csv'))
    return csvs[0] if csvs else None


def read_csv(path):
    if path is None:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"Failed reading {path}: {e}")
        return pd.DataFrame()


def plot_per_sensor(df, outpath):
    if df.empty:
        print('No per-sensor data')
        return
    df['pm25_avg'] = pd.to_numeric(df['pm25_avg'], errors='coerce')
    top = df.sort_values('pm25_avg', ascending=False).head(15)
    plt.figure(figsize=(12,6))
    plt.bar(top['datasourceid'].astype(str), top['pm25_avg'], color='tab:orange')
    plt.xticks(rotation=45, ha='right')
    plt.title('Top 15 Sensors by PM2.5 Avg')
    plt.ylabel('PM2.5 Avg')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print('Wrote', outpath)


def plot_daily(df, outpath):
    if df.empty:
        print('No daily data')
        return
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['pm25_avg'] = pd.to_numeric(df['pm25_avg'], errors='coerce')
    df = df.sort_values('date')
    plt.figure(figsize=(12,5))
    plt.plot(df['date'], df['pm25_avg'], marker='o', linestyle='-')
    plt.xticks(rotation=45)
    plt.title('Daily PM2.5 Average')
    plt.ylabel('PM2.5 Avg')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print('Wrote', outpath)


def plot_anomalies(df, outpath):
    if df.empty:
        print('No anomalies')
        return
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    df['pm25'] = pd.to_numeric(df['pm25'], errors='coerce')
    df = df.dropna(subset=['time'])
    plt.figure(figsize=(12,5))
    plt.scatter(df['time'], df['pm25'], c='red', alpha=0.7)
    plt.xticks(rotation=45)
    plt.title('Anomalies (PM2.5)')
    plt.ylabel('PM2.5')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print('Wrote', outpath)


def plot_rolling(df, sensor, outpath):
    if df.empty:
        print('No rolling data')
        return
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    pmcol = None
    for c in ['rolling_pm25_24h', 'rolling_pm25', 'pm25']:
        if c in df.columns:
            pmcol = c
            break
    if pmcol is None:
        print('No rolling PM column found')
        return
    df_sensor = df[df['datasourceid'] == sensor]
    if df_sensor.empty:
        print('No rolling data for sensor', sensor)
        return
    df_sensor = df_sensor.sort_values('time')
    df_sensor[pmcol] = pd.to_numeric(df_sensor[pmcol], errors='coerce')
    plt.figure(figsize=(12,5))
    plt.plot(df_sensor['time'], df_sensor[pmcol], marker='o')
    plt.xticks(rotation=45)
    plt.title(f'Rolling PM2.5 ({sensor})')
    plt.ylabel('PM2.5')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()
    print('Wrote', outpath)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--copy-from-container', action='store_true')
    parser.add_argument('--outdir', default='plots')
    parser.add_argument('--show', action='store_true')
    parser.add_argument('--sensor', default=None, help='Sensor id for rolling plot')
    parser.add_argument('--local-base', default='dashboard_spark_out')
    args = parser.parse_args()

    if args.copy_from_container:
        # copy directories locally
        docker_copy('hadoop-master:/dashboard/spark_out', 'dashboard_spark_out')
        docker_copy('hadoop-master:/dashboard/rolling_out', 'dashboard_rolling_out')

    per_dir = Path(args.local_base) / 'per_sensor'
    daily_dir = Path(args.local_base) / 'daily'
    anoms_dir = Path(args.local_base) / 'anomalies'
    rolling_dir = Path('dashboard_rolling_out')

    per_file = find_part_file(per_dir)
    daily_file = find_part_file(daily_dir)
    anoms_file = find_part_file(anoms_dir)
    rolling_file = find_part_file(rolling_dir)

    per = read_csv(per_file)
    daily = read_csv(daily_file)
    anoms = read_csv(anoms_file)
    rolling = read_csv(rolling_file)

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    plot_per_sensor(per, outdir / 'per_sensor_top15.png')
    plot_daily(daily, outdir / 'daily_pm25.png')
    plot_anomalies(anoms, outdir / 'anomalies.png')

    if args.sensor:
        sensor = args.sensor
    else:
        sensor = None
        if not rolling.empty and 'datasourceid' in rolling.columns:
            sensor = rolling['datasourceid'].iloc[0]
    if sensor:
        plot_rolling(rolling, sensor, outdir / f'rolling_{sensor}.png')

    if args.show:
        for f in sorted(outdir.glob('*.png')):
            img = plt.imread(f)
            plt.figure(figsize=(10,6))
            plt.imshow(img)
            plt.axis('off')
        plt.show()


if __name__ == '__main__':
    main()
