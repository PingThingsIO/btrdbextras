import uuid
from typing import Dict, List, Optional, Tuple

import numpy as np
import opendssdirect as dss
from btrdb import BTrDB
from tqdm.auto import tqdm

PHASE_LETTERS = ["A", "B", "C"]


def v2dict(bus_names: List[str]) -> Dict[str, float]:
    """
    Returns the voltage data on each phase of the buses in bus_names.

    Parameters
    ----------
    bus_names : List[str]
        Buses for which to return voltage data.

    Returns
    -------
    Dict[str, float]
        V : dictionary of real values. The keys are the stream collection/name for the data.
        The collection / stream name encodes the bus, phase, and quantity
        which are formatted by the method get_voltage_stream_colname.
    """
    # Instantiate the dict of results
    V = {}

    # Iterate through the buses
    for bus in bus_names:
        # Set the current bus to be "active"
        dss.Circuit.SetActiveBus(bus)

        # Get the phases at this bus
        phases = dss.Bus.Nodes()
        nphases = len(phases)

        # Get all voltages at this bus
        # This is a real array of size nphases * 2 -
        # each pair is the re & imag part of the voltage
        busvolt = dss.Bus.Voltages()

        # Get the voltage at each phase
        for pidx in range(nphases):
            # We don't want to save any phase 0 data
            if phases[pidx] == 0:
                continue

            voltage = busvolt[pidx * 2] + 1j * busvolt[pidx * 2 + 1]

            # Save the magnitude data
            col, name = get_voltage_stream_colname(
                bus, PHASE_LETTERS[phases[pidx] - 1], True
            )
            V[col + "/" + name] = np.abs(voltage)

            # Save the angle data
            col, name = get_voltage_stream_colname(
                bus, PHASE_LETTERS[phases[pidx] - 1], False
            )
            V[col + "/" + name] = np.angle(voltage, deg=True)

    return V


def i2dict(con_names: List[str]) -> Dict[str, float]:
    """
    Returns the complex currents on each phase at each end of each connector.
    The order of results matches the input names.

    Parameters
    ----------
    con_names : List[str]
        Connectors for which to return current data

    Returns
    -------
    Dict[str, float]
        I - dictionary of real values. The keys are the stream collection/name for the data.
        The collection / stream name encodes the connector, end, phase, and quantity
        which are formatted by the method get_lineflow_stream_colname.
    """

    ncons = len(con_names)
    # Get the ends of each connector
    con_ends = get_conn_ends(con_names)

    # Instantiate the dict of results
    I = {}

    for cidx in range(ncons):
        # Set the current connector to be "active"
        dss.Circuit.SetActiveElement(con_names[cidx])

        # Get the phases on the connector
        # this is the phases of each terminal at each end
        phases = dss.CktElement.NodeOrder()
        nphases = int(len(phases) / 2)

        # Get the currents on each phase at each end of the connector
        # This is a real array of size nphases * 2 * 2 -
        # each pair is the re & imag part of the current
        coni = dss.CktElement.Currents()

        for end in range(len(con_ends[cidx])):
            for pidx in range(nphases):
                # We don't want to save any phase 0 information which corresponds to the grounding connection
                if phases[pidx] == 0:
                    continue
                # Construct the complex current.
                current = (
                    coni[2 * (end * nphases + pidx)]
                    + 1j * coni[2 * (end * nphases + pidx) + 1]
                )
                # Save the magnitude data
                col, name = get_lineflow_stream_colname(
                    con_names[cidx],
                    con_ends[cidx][end],
                    PHASE_LETTERS[phases[pidx] - 1],
                    True,
                )
                I[col + "/" + name] = np.abs(current)
                # Save the angle data
                col, name = get_lineflow_stream_colname(
                    con_names[cidx],
                    con_ends[cidx][end],
                    PHASE_LETTERS[phases[pidx] - 1],
                    False,
                )
                I[col + "/" + name] = np.angle(current, deg=True)
    return I


def simulate_network(
    loads: np.ndarray,
    load_names: List[str],
    con_types: Optional[List[str]] = ["Line", "Transformer"],
) -> Dict[str, np.ndarray]:
    """
    Simulates the network for all the values of load in the input loads.

    Parameters
    ----------
    loads : np.ndarray
        n x T matrix of floats. This is the load values to set and simulate.
    load_names : List[str]
        The names of the loads whose values are to be set to those in loads.
    con_types : Optional[List[str]]
        The connector types for which to return current data.

    Returns
    -------
    Dict[str, np.ndarray]
        V : Dictionary of real length T array. Values are voltage magnitude
        & angle time series generated by simulation.
        The keys are the stream collection/name for the data. The collection
        / stream name encodes the bus,
        phase, and quantity which is formatted by the method get_voltage_stream_colname.

        I : Dictionary of real length T array. Values are current magnitude
        & angle time series generated by simulation.
        The keys are the stream collection/name for the data. The collection
        / stream name encodes the
        connector, end, phase, and quantity which is formatted by the method
        get_lineflow_stream_colname.
    """
    [n, T] = np.shape(loads)

    # Get the buses and connectors
    bus_names = get_buses()
    con_names = get_connectors(con_types)
    con_ends = get_conn_ends(con_names)

    V = {}
    I = {}

    # Run the first simulation to get the keys for the output dictionary
    # Set the new load values
    set_loads(loads[:, 0], load_names)
    # Solve the power flow
    dss.Solution.Solve()
    # Get the data
    vdata = v2dict(bus_names)
    idata = i2dict(con_names)
    # Save the voltage data
    for key, val in vdata.items():
        V[key] = np.nan * np.ones(T)
        V[key][0] = val
    # Save the current data
    for key, val in idata.items():
        I[key] = np.nan * np.ones(T)
        I[key][0] = val

    # Iterate through rest of the times
    for t in tqdm(range(1, T), desc="Running simulation", leave=False):
        set_loads(loads[:, t], load_names)
        dss.Solution.Solve()
        vdata = v2dict(bus_names)
        idata = i2dict(con_names)

        for key, val in vdata.items():
            V[key][t] = val
        for key, val in idata.items():
            I[key][t] = val

    return V, I


###############################################################################
# Methods related to streams that we will create & push data to.
###############################################################################


def get_stream_info(
    base_col="simulated",
) -> Tuple[List[str], List[str], List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Returns collection names, tags, and annotations
    for all the streams we want to create to hold
    voltage and current data across the network.

    Parameters
    ----------
    base_col : str
        The base collection level under which we want all the simulated streams to be organized.

    Returns
    -------
    collections: List[str]
    names : List[str]
    tags : List[Dict[str, str]]
    annotations : List[Dict[str, str]]
    """
    phases = PHASE_LETTERS

    # The lists to store all results
    collections = []
    names = []
    tags = []
    annotations = []

    ## Get information for streams of bus voltages
    # Get the names of all buses
    bus_names = dss.Circuit.AllBusNames()
    # Iterate over all buses and determine streams for each
    for bus in bus_names:
        # Set the bus to "active"
        dss.Circuit.SetActiveBus(bus)
        # Get the basekV of this bus
        basekV = dss.Bus.kVBase()
        # Get phases at this bus
        busphases = dss.Bus.Nodes()
        for p in busphases:
            # We don't want to save any phase 0 information
            if p == 0:
                continue
            # Magnitude stream
            cM, nM, tM, aM = get_voltage_stream_info(bus, phases[p - 1], True, basekV)
            # Angle stream
            cA, nA, tA, aA = get_voltage_stream_info(bus, phases[p - 1], False, basekV)
            # Save results
            collections.append(base_col + "/" + cM)
            collections.append(base_col + "/" + cA)
            names.append(nM)
            names.append(nA)
            tags.append(tM)
            tags.append(tA)
            annotations.append(aM)
            annotations.append(aA)

    ## Get information for the streams of connection currents
    # Get the names of all connectors
    con_names = get_connectors()
    for con in con_names:
        # Set the current connector to be "active"
        dss.Circuit.SetActiveElement(con)

        # Get the buses that this connector connects
        # (the split removes terminals indicating the phases at each end
        # so three-phase busX.1.2.3 becomes busX)
        to = dss.CktElement.BusNames()[0].split(".")[0]
        frm = dss.CktElement.BusNames()[1].split(".")[0]
        # Check that to and frm are different (these can be the same for capacitors)
        if to == frm:
            ends = [to]
        else:
            ends = [to, frm]

        # Get the phases on the line
        # this is the phases of each terminal at each end
        conphases = dss.CktElement.NodeOrder()
        nphases = int(len(conphases) / 2)

        for end in ends:
            for p in conphases[0:nphases]:
                # We don't want to save any phase 0 information
                if p == 0:
                    continue

                # Magnitude stream
                cM, nM, tM, aM = get_lineflow_stream_info(con, end, phases[p - 1], True)
                # Angle stream
                cA, nA, tA, aA = get_lineflow_stream_info(
                    con, end, phases[p - 1], False
                )
                # Save results
                collections.append(base_col + "/" + cM)
                collections.append(base_col + "/" + cA)
                names.append(nM)
                names.append(nA)
                tags.append(tM)
                tags.append(tA)
                annotations.append(aM)
                annotations.append(aA)

    return collections, names, tags, annotations


def get_existing_streams(col_prefix, conn):
    """Get the existing streams under the base collection col_prefix"""
    streams = conn.streams_in_collection(col_prefix)
    # Build the dictionary of the streams
    streams_dict = {}
    for stream in streams:
        streams_dict[stream.collection + "/" + stream.name] = stream
    print("Found", len(streams_dict.keys()), "streams under", col_prefix)
    return streams_dict


def create_streams(
    col_prefix: str,
    collections: List,
    names,
    tags,
    annotations,
    conn: BTrDB,
    verbose: bool = False,
):
    """
    Given a set of collections, names, tags, and annotations for intended streams, check if
    they exist. If not, create them.

    Returns
    -------
    existing : dict
        A dictionary capturing all the intended streams. Keys are the collection/name of the stream,
        values are stream objects.
    """

    existing = get_existing_streams(col_prefix, conn)

    # Iterate through the desired streams and check if they exist already. If not
    # create them.
    nstreams = len(collections)
    nexisting = 0
    ncreated = 0
    for i in range(nstreams):
        stream_info = collections[i] + "/" + names[i]
        if stream_info in existing:
            if verbose:
                print(stream_info, "already exists.")
            nexisting += 1
            pass
        else:
            stream_id = uuid.uuid4()

            stream = conn.create(
                uuid=stream_id,
                collection=collections[i],
                tags=tags[i],
                annotations=annotations[i],
            )

            existing[stream_info] = stream
            if verbose:
                print("Created", stream_info, ", uuid:", stream_id)
            ncreated += 1
    print("Found", nexisting, "streams. Created", ncreated, "streams.")
    return existing


def get_lineflow_stream_info(line_name, line_end, phase, ismag):
    if ismag:
        unit = "amps"
    else:
        unit = "degrees"
    collection, name = get_lineflow_stream_colname(line_name, line_end, phase, ismag)

    tags = {"name": name, "unit": unit}
    annotations = {"phase": phase}

    return collection, name, tags, annotations


def get_lineflow_stream_colname(line_name, line_end, phase, ismag):
    collection = line_name + "/" + line_end
    if ismag:
        lastltr = "M"
    else:
        lastltr = "A"

    name = "I" + phase + lastltr
    return collection, name


def get_voltage_stream_colname(bus_name, phase, ismag):
    collection = bus_name
    if ismag:
        lastltr = "M"
    else:
        lastltr = "A"

    name = "V" + phase + lastltr
    return collection, name


def get_voltage_stream_info(bus_name, phase, ismag, basekV):
    if ismag:
        unit = "volts"
    else:
        unit = "degrees"
    collection, name = get_voltage_stream_colname(bus_name, phase, ismag)
    tags = {"name": name, "unit": unit}
    annotations = {"phase": phase, "basekV": str(basekV)}

    return collection, name, tags, annotations


def add_all_data(times, data_dict, streams_dict, base_col):
    """
    Add data to each stream.

    Parameters
    ----------
    times : list of ints
        The timestamps for the data to be added (one set of times for all data)

    data_dict : dict of arrays
        The dictionary containing data to be added. Keys are the collection/name of
        the stream to which data is to be added. Values are arrays of floats to add.

    streams_dict : dict of stream objects
        keys are the collection/name of each stream. values are the stream objects.

    base_col : string
        base collection prefix under which all streams can be found.
    """
    # Create progress bar
    nstreams = len(data_dict.keys())
    pbar = tqdm(total=nstreams, desc="Pushing data to streams", leave=False)

    for key in data_dict:
        stream_info = base_col + "/" + key
        if stream_info in streams_dict:
            add_to_stream(streams_dict[stream_info], times, data_dict[key])
        else:
            print("WARNING", stream_info, "not found")
        pbar.update(1)
    pbar.close()


def add_to_stream(stream, times, values):
    """
    Given times and values, put them in the required tuple format and
    add them to the stream.
    """
    payload = []

    if len(times) != len(values):
        print("WARNING: times & values not same size")
    for i in range(len(times)):
        payload.append((times[i], values[i]))

    stream.insert(payload, merge="replace")


###############################################################################
# Convenient wrappers to get model information
###############################################################################


def get_buses():
    """A convenient wrapper to retrieve all buses in the system."""
    return dss.Circuit.AllBusNames()


def get_connectors(qualified=["Line", "Transformer"]):
    """This method returns all connection elements of the "qualified" types"""
    connectors = []
    pds = dss.PDElements.AllNames()
    for pd in pds:
        # Need to split the name to get the element type
        if pd.split(".")[0] in qualified:
            connectors.append(pd)
    return connectors


def get_conn_ends(con_names):
    """The list of lists with names of connectors ends"""
    con_ends = []
    for con in con_names:
        # Set the current connector to be "active"
        dss.Circuit.SetActiveElement(con)
        # Get the buses that this connector connects
        # (the split removes terminals indicating the phases at each end
        # so three phase busX.1.2.3 becomes busX)
        to = dss.CktElement.BusNames()[0].split(".")[0]
        frm = dss.CktElement.BusNames()[1].split(".")[0]
        # Check for to and frm being identical - can happen with capacitors
        if to == frm:
            ends = [to]
        else:
            ends = [to, frm]
        con_ends.append(ends)
    return con_ends


def get_loads():
    """Get all the loads in the network"""
    load_names = dss.Loads.AllNames()
    nloads = len(load_names)

    load = np.zeros([nloads])
    # Get the initial loads
    for i in range(nloads):
        dss.Loads.Name(load_names[i])
        load[i] = dss.Loads.kW()

    return load, load_names


def set_loads(load, load_names):
    """Set the value of load load_names[i] to load[i]"""
    nloads = len(load_names)
    for i in range(nloads):
        # Set this load to be active
        dss.Loads.Name(load_names[i])
        # Set the load
        dss.Loads.kW(load[i])


def get_nbuses():
    """Get the number of buses in the network"""
    return len(dss.Circuit.AllBusNames())


def get_nlines():
    """Get the number of lines in the network"""
    return len(dss.Lines.AllNames())


def get_nconnectors(contypes=["Line", "Transformer"]):
    """Get the number of connectors in the network"""
    return len(get_connectors(qualified=contypes))


def get_nloads():
    """Get the number of loads in the network"""
    return len(dss.Loads.AllNames())
