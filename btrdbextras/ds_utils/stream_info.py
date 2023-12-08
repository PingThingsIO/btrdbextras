"""
Module for general utility functions for PingThings DataSci Team.
"""
##########################################################################
# Imports
##########################################################################
import warnings
from typing import List, Union

import numpy as np
import pandas as pd

from btrdb.stream import Stream
from btrdb.utils.timez import ns_delta

__all__ = [
    "describe_streams",
    "find_samplerate",
    "StreamType",
    "ANGLE",
    "CONDUCTANCE",
    "CURRENT",
    "DQ",
    "FREQUENCY",
    "POWER",
    "RESISTANCE",
    "VOLTAGE",
    ]

##########################################################################
# Constants
##########################################################################
KNOWN_HZ = np.array([1 / (15 * 60), 15, 30, 60, 120])

# Unit types
ANGLE = "angle"
CONDUCTANCE = "conductance"
CURRENT = "current"
DQ = "data quality"
FREQUENCY = "frequency"
POWER = "power"
RESISTANCE = "resistance"
VOLTAGE = "voltage"

# StreamTypeMap categorize streams based on provided units
StreamTypeMap = {
    "": (None, 1),
    "A": (CURRENT, 1),
    "Amp": (CURRENT, 1),
    "Amps": (CURRENT, 1),
    "amps": (CURRENT, 1),
    "arcmin": (ANGLE, 1 / 60),
    "CurrentAcInternalShunt": (CURRENT, 1),
    "DC/V": (VOLTAGE, 1),
    "Deg": (ANGLE, 1),
    "deg": (ANGLE, 1),
    "Degree": (ANGLE, 1),
    "Degrees": (ANGLE, 1),
    "degrees": (ANGLE, 1),
    "FLAG": (DQ, 1),
    "FREQ": (FREQUENCY, 1),
    "Freq": (FREQUENCY, 1),
    "Hz": (FREQUENCY, 1),
    "IPHA": (ANGLE, 1),
    "IPHM": (CURRENT, 1),
    "KA": (CURRENT, 1e3),
    "kA": (CURRENT, 1e3),
    "kAmps": (CURRENT, 1e3),
    "kilovolts": (VOLTAGE, 1e3),
    "kOhms": (RESISTANCE, 1e3),
    "KV": (VOLTAGE, 1e3),
    "kV": (VOLTAGE, 1e3),
    "kVolt": (VOLTAGE, 1e3),
    "kVolts": (VOLTAGE, 1e3),
    "millivolts": (VOLTAGE, 1e-3),
    "MVAR": (POWER, 1e6),
    "MVar": (POWER, 1e6),
    "MVARS": (POWER, 1e6),
    "MVr": (VOLTAGE, 1e6),
    "MW": (POWER, 1e6),
    "Ohms": (RESISTANCE, 1),
    "radian": (ANGLE, 1),
    "Radian": (ANGLE, 1),
    "radians": (ANGLE, 1),
    "Radians": (ANGLE, 1),
    "rpm": (FREQUENCY, 1),
    "Siemens": (CONDUCTANCE, 1),
    "STAT": (DQ, 1),
    "STAcAmp": (CURRENT, 1),
    "STActivePower": (POWER, 1),
    "STAcVolt": (VOLTAGE, 1),
    "STApparentPower": (POWER, 1),
    "STDcAmp": (CURRENT, 1),
    "STDcVolt": (VOLTAGE, 1),
    "STReactivePower": (POWER, 1),
    "V": (VOLTAGE, 1),
    "v": (VOLTAGE, 1),
    "VAR": (POWER, 1),
    "VA": (POWER, 1),
    "Vars": (POWER, 1),
    "Volt": (VOLTAGE, 1),
    "VoltageAc": (VOLTAGE, 1),
    "Volts": (VOLTAGE, 1),
    "volts": (VOLTAGE, 1),
    "VPHA": (ANGLE, 1),
    "VPHM": (VOLTAGE, 1),
    "Vr": (VOLTAGE, 1),
    "W": (POWER, 1),
    "Watts": (POWER, 1),
    # "%":                      (TODO, 1), # TODO: fill in missing value from Dominion
    # "ALOG":                   (TODO, 1), # TODO: fill in missing value from Dominion
    # "Analog":                 (TODO, 1), # TODO: fill in missing value from Dominion
    # "DIGI":                   (TODO, 1), # TODO: fill in missing value from Dominion
    # "Digital":                (TODO, 1), # TODO: fill in missing value from Dominion
    # "digital":                (TODO, 1), # TODO: fill in missing value from Dominion
    # "Phasor":                 (TODO, 1), # TODO: fill in missing value from Dominion
    # "PU":                     (TODO, 1), # TODO: fill in missing value from Dominion
    # "QUAL":                   (TODO, 1), # TODO: fill in missing value from Dominion
    }


##########################################################################
# General utilities
##########################################################################
def describe_streams(
    streams: List[Stream], display_annotations=False, filter_annotations=None
    ):
    """
    This function displays streams info such as collection, UUID, tags and annotations in Dataframe format.

    Parameters
    ----------
    streams : list of Stream objects or Streamset.
        Streams to display streams info.
    display_annotations: bool, optional.
        Whether to display annotations. Default is False.
    filter_annotations : [str], optional.
        Only display the specified annotations along with the tags, collection and UUID.

    Returns
    -------
    pandas.DataFrame
        DataFrame displaying streams info.

    Examples
    ----------
    >>> streams = conn.streams_in_collection('sunshine/PMU3')
    >>> describe_streams(streams)
    """

    table = []
    for idx, stream in enumerate(streams):
        stream_info_dict = {"collection": stream.collection, "UUID": str(stream.uuid)}
        stream_info_dict.update(stream.tags())

        if display_annotations:
            annotations = stream.annotations()[0]
            stream_info_dict.update(annotations)

        table.append(stream_info_dict)

    table_df = pd.DataFrame(table)

    if filter_annotations is not None:
        if not isinstance(filter_annotations, list) or not all(
            isinstance(annotation, str) for annotation in filter_annotations
            ):
            raise TypeError("filter_annotations has to be a list of str.")

        # find filter_annotations that are not in "annotations", this might break if python version is below 3.7
        # since dictionaries are not ordered for python version lower than 3.7
        not_found_annotations = [
            annotation
            for annotation in filter_annotations
            if annotation not in table_df.columns[6:]
            ]
        if len(not_found_annotations) > 0:
            raise ValueError(f"{not_found_annotations} not found in annotations.")

        table_df = table_df[
            ["collection", "UUID", "name", "unit", "distiller", "ingress"]
            + filter_annotations
            ]

    return table_df


def find_samplerate(stream, pw=50, update=False, version=0):
    """
    Find the sample rate of the stream.

    Parameters
    ----------
    stream : Stream
        BTrDB Stream object
    pw : int
        pointwidth (default is 50) approximately 13 days, creating ~28 time points in a
        year to calculate sampling rate from.
    update: bool
        (default - False) whether to update the stream annotation with the sample rate
    version: int
        specify which version to use

    Returns
    -------
    Fs : int
        Calculated sampling rate of the stream.
    """

    def _find_sampling(stream, s, e, v):
        d = [_[0].time for _ in stream.values(s, e, v)]
        if len(d) in {0, 1}:
            return
        temp = pd.Series(d, name="time")

        # find the median time difference from timestamps convert nanoseconds to seconds
        fs = round((temp.diff().median() / 1e6), 2) / 1.0e3
        if fs == 0:
            return
        else:
            return 1 / fs

    start = stream.earliest()[0].time
    end = stream.latest()[0].time
    n_range = ns_delta(minutes=60)
    # find sample rates from 60 mins of every 2nd time window of the aligned_windows (if
    # there's more than 10 windows)
    windows = stream.aligned_windows(
        start, end, pw, version=version
        )  # returns list of tuples
    if len(windows) > 10:
        windows = windows[1:-2:2]
        warnings.warn(
            f" {stream.uuid.hex}: calculating sample rate from every other window of"
            f"pointwidht = {pw})"
            )
    Fs = [
        _find_sampling(stream, _[0].time, _[0].time + n_range, version) for _ in windows
        ]
    Fs = np.mean([_ for _ in Fs if _])
    if np.isnan(Fs):
        warnings.warn(f" {stream.uuid.hex} has no data to calculate sampling rate from")
    else:
        Fs = float(KNOWN_HZ[np.abs(KNOWN_HZ - Fs).argmin()])
        if update:
            stream.update(annotations={"sample_rate": Fs})
        return Fs


# StreamType from Unit
class StreamType:
    """
    The StreamType class is used to determine a stream's tagged unit to their corresponding general
    unit types, such as Voltage, Current, etc and scale factors to base unit of measurements.

    Examples
    --------

    Create a stream type object for voltage with a scale factor of 1 and print the unit type and
    scale factor

    >>> voltage_streamtype = StreamType.from_unit("VPHM")
    >>> print(voltage_streamtype.unit_type, voltage_streamtype.scalefactor)
    (Voltage, 1)

    Create a stream type object for current with a scale factor of 1000

    >>> current_streamtype = StreamType.from_unit("kA")
    >>> print(current_streamtype.unit_type, current_streamtype.scalefactor)
    CURRENT, 1000

    Can check each stream's unit type to be used in per-unit calculations

    >>> from btrdbextras.ds_utils.stream_info import VOLTAGE, CURRENT
    >>> tagged_unit = stream.unit() # Stream's unit is 'VPHM'
    >>> if StreamType.from_unit(tagged_unit) == VOLTAGE:
    >>>     # calculation for per-unit with basekV value
    >>>     pass
    >>> else:
    >>>     # calculation for per-unit with basePower value
    >>>     pass

    """

    def __init__(self, unit_type: str, scalefactor: Union[int, float]):
        self.unit_type = unit_type
        self.scalefactor = scalefactor

    def __repr__(self):
        return f"<{self.unit_type.title()}: scale-factor={self.scalefactor:.0f}>"

    @classmethod
    def from_unit(cls, stream_unit: str):
        return cls(*StreamTypeMap[stream_unit])