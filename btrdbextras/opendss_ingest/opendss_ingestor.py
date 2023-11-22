import argparse
from datetime import datetime

import btrdb
import numpy as np
import opendssdirect as dss
import simulation_utils as sims
from btrdb.utils.timez import datetime_to_ns, ns_delta, to_nanoseconds


def simulate_network(load, load_names):
    V, I = sims.simulate_network(load, load_names)
    return V, I


def initialize_simulation(fs, start_ns, end_ns):
    # The location of the .dss file specifying the model.
    model_loc = "./Models/13Bus/IEEE13Nodeckt.dss"
    # Activate the model in OpenDSS
    dss.run_command("Redirect " + model_loc)

    T = int((end_ns - start_ns) * fs / 1e9)
    load, load_names = sims.get_loads()
    nloads = len(load_names)
    return T, load, load_names, nloads


def generate_scaling(mu, sig, size):
    return np.random.normal(loc=mu, scale=sig, size=size)


def run_simulation(start_ns, end_ns, collection_prefix, fs=30, conn=None):
    # Initialize simulation parameters
    T, load, load_names, nloads = initialize_simulation(fs, start_ns, end_ns)
    timestamps = np.arange(start_ns, end_ns, 1e9 // fs, dtype="int")
    scale = generate_scaling(1.1, 0.1, [nloads, T])
    new_load = scale * load[:, np.newaxis]
    collections, names, tags, annotations = sims.get_stream_info(
        base_col=collection_prefix
    )
    streams_dict = sims.create_streams(
        collection_prefix, collections, names, tags, annotations, conn
    )

    # Run simulation
    V, I = simulate_network(new_load, load_names)
    sims.add_all_data(timestamps, V, streams_dict, collection_prefix)
    sims.add_all_data(timestamps, I, streams_dict, collection_prefix)


def main(collection_prefix, start_time, end_time, fs, profile):
    conn = btrdb.connect(profile=profile)
    run_simulation(start_time, end_time, collection_prefix, fs, conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate network.")
    parser.add_argument(
        "-s",
        "--start_ns",
        default=datetime_to_ns(datetime.utcnow()),
        type=int,
        help=("Start time in nanoseconds. " "(default: %(default))"),
    )
    parser.add_argument(
        "-e",
        "--end_ns",
        type=int,
        help="End " "time " "in nanoseconds relative to Jan 1, 2023.",
    )
    parser.add_argument(
        "--frequency", default=30, type=int, help="Sapler frequency in Hz"
    )
    parser.add_argument(
        "--duration_days",
        default=1,
        type=int,
        help="Duration in hours relative to start.",
    )
    parser.add_argument(
        "--collection_prefix", default="simulated/ieee13", help="Collection prefix"
    )
    parser.add_argument(
        "--profile", default="ni4ai", help="BTRDB profile name (default: %(default))"
    )

    args = parser.parse_args()
    if args.end_ns is None:
        args.end_ns = args.start_ns + ns_delta(days=args.duration_days)
    main(
        args.collection_prefix, args.start_ns, args.end_ns, args.frequency, args.profile
    )
