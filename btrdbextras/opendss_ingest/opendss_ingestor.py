import argparse
import os
import time
from datetime import datetime
from typing import List, Tuple

import numpy as np
import opendssdirect as dss
import pyarrow as pa
from btrdb import connect as btrdb_connect
from btrdb.stream import Stream,StreamSet, INSERT_BATCH_SIZE
from btrdb.utils.timez import datetime_to_ns, ns_delta

import btrdbextras.opendss_ingest.simulation_utils as sims

MODEL_REL_PATH = os.path.dirname(__file__)


def initialize_simulation(model_loc:str) -> Tuple[np.ndarray, List[str]]:
    """
    Initializes the simulation by activating the model in OpenDSS and
    retrieving the simulated loads.

    Parameters
    ----------
    model_loc : str
        The file path of the model to be activated in OpenDSS.

    Returns
    -------
    load : np.ndarray
        An array of loads retrieved from the model.
    load_names : list of str
        An array of load names corresponding to the loads in the model.

    """
    # Activate the model in OpenDSS
    dss.run_command("Redirect " + model_loc)
    load, load_names = sims.get_loads()
    return load, load_names


def generate_scaling(mu, sig, size):
    """
    Generates a random scaling factor based on a normal distribution with mean `mu`
    and standard deviation `sig`. The number of scaling factors generated is determined by `size`.

    Parameters
    ----------
    mu : float
        The mean of the normal distribution.
    sig : float
        The standard deviation of the normal distribution.
    size : int
        The number of scaling factors to generate.

    Returns
    -------
    ndarray
        An array of scaling factors generated from the normal distribution.

    """
    # TODO: add more types of scaling as additive noise/signal?
    return np.random.normal(loc=mu, scale=sig, size=size)


def simulate_event(value, continue_event:bool=False):

    event_type = np.random.choice(np.arange(10)) # 10% probability of evnet?

    if event_type == 0 and continue_event: # where the values for V and I are
        # 0.0
        value = 0.0
    elif event_type == 1: # with no data at all
        value = None
    elif event_type == 2: # value is out of bounds
        value = np.random.choice([np.random.uniform(low=-10, high=-1),
                                  np.random.uniform(low=1e15, high=1e20)])
    else:
        pass
    return value

def run_simulation(start_ns, end_ns, collection_prefix,
                   fs=30, conn=None,
                   model_location=None):
    """
    Runs a simulation from `start_ns` to `end_ns` with the given `collection_prefix`.
    The simulation is initialized using a model obtained from `model_location`.
    If `model_location` is not provided, it defaults to 'Models/13Bus/IEEE13Nodeckt.dss'.
    The simulation uses frequency `fs` and a database connection `conn`.

    Parameters
    ----------
    start_ns : int
        The start time of the simulation in nanoseconds.
    end_ns : int
        The end time of the simulation in nanoseconds.
    collection_prefix : str
        The prefix for the name of the data collection for the simulation.
    fs : int, optional
        The frequency of the simulation, defaults to 30.
    conn : obj, optional
        The database connection used for the simulation, defaults to None.
    model_location : str, optional
        The file location of the model to be used for the simulation.
        Defaults to 'Models/13Bus/IEEE13Nodeckt.dss' if not specified.

    """
    model_location = (model_location
                      if model_location is not None
                      else os.path.join(MODEL_REL_PATH,
                                        'Models/13Bus/IEEE13Nodeckt.dss'))
    # Initialize simulation parameters
    load, load_names = initialize_simulation(model_location)
    timestamps = np.arange(start_ns, end_ns, 1e9 // fs, dtype="int")
    scale = generate_scaling(1.1, 0.1, [len(load_names), timestamps.size])
    new_load = scale * load[:, np.newaxis]
    collections, names, tags, annotations = sims.get_stream_info(
        base_col=collection_prefix
    )
    streams_dict = sims.create_streams(
        collection_prefix, collections, names, tags, annotations, conn
    )
    streamset = StreamSet(list(streams_dict.values()))
    _stream_info = lambda s: "/".join(
        [s.collection.replace(
            collection_prefix + '/', ''
        ), s.name]
    )
    _values = lambda s, cont_event: simulate_event(
        (V.get(_stream_info(s)) or I.get(_stream_info(s)))[0],
        cont_event)
    prev_timestamp = None
    # For example, event lasts for at most 100 timestamps
    continue_event_ind_left = np.random.randint(1, 100)
    # Run simulation
    for i in range(new_load.shape[1]):
        V, I = sims.simulate_network(new_load[:, [i]], load_names)
        if continue_event_ind_left == 0:
            continue_event_ind_left = np.random.randint(1, 100)
        else:
            continue_event_ind_left -= 1
        now = datetime_to_ns(datetime.utcnow())
        _datamap_gen = conn._executor.map(
            lambda s: (s._uuid,
                       dict(time=now,
                                   value=_values(s,
                                                 continue_event_ind_left > 0)
                       )),
            streamset._streams
            )
        data_map = {}
        for k,v in _datamap_gen:
            if v['value'] is not None:
                data_map[k] = pa.Table.from_pylist([v])
        _streamset = StreamSet([Stream(conn, uuid=k) for k in
                                data_map.keys()])
        _streamset.arrow_insert(data_map)
        if i % INSERT_BATCH_SIZE == 0:
            [stream.flush() for stream in streamset]
            print("Streamset flushed.")
        time.sleep(round(1/fs,ndigits=5))
        # print(prev_timestamp, now) if i % 10 == 0 else None
        prev_timestamp = now

def main(collection_prefix, start_time, end_time, fs, profile):
    conn = btrdb_connect(profile=profile)
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
        "--frequency", default=30, type=int,
        help="Sampling frequency in Hz"
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
        print("end_ns is not given, using default value for duration")
        args.end_ns = args.start_ns + ns_delta(days=args.duration_days)

    main(
        args.collection_prefix, args.start_ns, args.end_ns, args.frequency, args.profile
    )
