"""
Module for general anomaly detection functions using recursive Tree search with Arrow endpoints.

NOTE: Using pyarrow.Table.to_pylist() and iterating the list is ~9.36x more performant than using
pyarrow.Table.to_pandas() and iterating with itertuples(): Need more testing.

Pyarrow converts Table via Dataframe Interchange Protocol so the `time` is pandas.TimeStamp objects
from `to_pylist()`.

#TODO: traverse the tree in increements of 6?
"""
import warnings
from typing import Tuple, Union

import pandas as pd
from btrdb.stream import Stream
from btrdb.utils.general import pointwidth

__all__ = [
    "search_generator_to_dataframe",
    "search_timestamps_above_threshold",
    "search_timestamps_below_threshold",
    "search_timestamps_outside_bounds",
    "search_timestamps_within_bounds",
    "search_timestamps_at_value",
]


####################################################################################################
# Helper functions
####################################################################################################
def search_generator_to_dataframe(search_generator):
    """
    Parameters
    ----------
    search_generator : generator
        The search_generator object containing the search results.

    Returns
    -------
    df : DataFrame
        The DataFrame containing the search results. The columns of the DataFrame depend on the
        shape of the search_generator object. If it has only one column, the column name will be
        "Time". If it has two columns, the column names will be "StartTime" and "EndTime".
    """
    df = pd.DataFrame(search_generator)
    if df.shape[1] == 1:
        df.columns = ["Time"]
    elif df.shape[1] == 2:
        df.columns = ["StartTime", "EndTime"]
    return df


def combine_consecutive_windows(
    dataframe,
    event_merge_threshold_ns: int = 0,
    agg: str = "max",
    data_columns: str = "Severity",
    stream=None,
):
    """
    Combine consecutive windows (end time is the same as the consecutive start time) into a single timerange.
    Such as [(2021-03-15 18:34:54.518091776,	2021-03-15 18:35:03.108026368),
    (2021-03-15 18:35:03.108026368,	2021-03-15 18:35:11.697960960)]
    -> [(2021-03-15 18:34:54.518091776,	2021-03-15 18:35:11.697960960)].

    If a stream object is given, check for the gaps between the events and combine the windows.

    NOTE: DataFrame must have ["StartTime", "EndTime"], error will be thrown if not in the columns.

    Parameters
    ----------
    dataframe : DataFrame
        A Pandas' DataFrame object.
    event_merge_threshold_ns : int
        If 2 events are within the specified time range in nanoseconds (end of previous event to start of consecutive),
        they are combined as a single event.
    agg : str
        Aggregation function to apply to the 'Severity'.
    data_columns : str or list of str
        Column names to apply aggregation to.
    stream : Stream, optional
        Stream to check for gaps between the events and combine the event windows.
    Returns
    -------
    DataFrame
        A Pandas' DataFrame object
    """

    def _agg_data_columns(data, mask, starttimes, end_times):
        """
        Apply aggregation to the given `data_columns`.
        """
        severity = (
            data[data_columns].where(mask).copy()  # use the starttime as reference
        )
        for startidx, endidx in zip(*[starttimes.index, end_times.index]):
            severity[startidx] = data.loc[startidx:endidx, data_columns].agg(agg)
        return severity.dropna()

    # DataFrame must have ["StartTime", "EndTime"], error will be thrown if not in columns
    merge_endtimes = dataframe.EndTime[
        (dataframe.StartTime.shift(-1) - dataframe.EndTime > event_merge_threshold_ns)
        | dataframe.StartTime.shift(-1).isna()
    ]
    merge_starttimes = dataframe.StartTime[
        (dataframe.StartTime - dataframe.EndTime.shift(1) > event_merge_threshold_ns)
        | dataframe.EndTime.shift(1).isna()
    ]
    if len(merge_endtimes) != len(merge_starttimes):
        warnings.warn("Unequal sizes for the merging start and end times.")
    elif len(merge_endtimes) == 0 or len(merge_starttimes) == 0:
        # nothing to merge
        return dataframe
    severity = _agg_data_columns(
        dataframe,
        dataframe.StartTime - dataframe.EndTime.shift(1) > event_merge_threshold_ns,
        merge_starttimes,
        merge_endtimes,
    )

    merged_df = pd.concat(
        [
            merge_starttimes.reset_index(drop=True),
            merge_endtimes.reset_index(drop=True),
            severity.reset_index(drop=True),
        ],
        axis=1,
    )

    if stream is not None and merged_df.shape[0] > 1:
        # combine the windows between the events if gaps (if the count == 0)
        # (done after merging to reduce the number of events)
        count = pd.concat(
            [
                merged_df.EndTime,
                merged_df.StartTime.shift(-1),
            ],
            axis=1,
        ).apply(
            lambda x: stream.count(int(x.EndTime), int(x.StartTime), precise=True)
            if not x.isna().any()
            else None,
            axis=1,
        )

        merged_start = merged_df.StartTime[~count.shift(1).eq(0) | count.isna()]
        merged_end = merged_df.EndTime[~count.eq(0) | count.isna()]
        severity = _agg_data_columns(
            merged_df, ~count.shift(1).eq(0) | count.isna(), merged_start, merged_end
        )

        merged_df = pd.concat(
            [
                merged_start.reset_index(drop=True),
                merged_end.reset_index(drop=True),
                severity.reset_index(drop=True),
            ],
            axis=1,
        )
    return merged_df


def calculate_bounds_severity(
    value: float, lower_bound: float, upper_bound: float
) -> float:
    """Calculate severity based on how far away from the bounds."""
    if value < lower_bound:
        severity = abs(lower_bound - value)
    else:  # this is the other condition where `point['value'] > upper_bound`
        severity = abs(upper_bound - value)
    return severity


####################################################################################################
# Search functions
####################################################################################################
def search_timestamps_above_threshold(
    stream: Stream,
    start: int,
    end: int,
    threshold: Union[int, float],
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are above the specified `threshold`
    using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        Stream to search

    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    threshold: float
        Find timestamps above threshold
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to search with RawPoints. Default is 36
        (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuples of start and end timestamps of the
        StatPoints windows that is above threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if above threshold for more than specified max depth, default
        ~1.15 minutes) timestamps of event.

    Example
    -------
    >>> result = search_timestamps_above_threshold(stream, threshold, start, end, initial_pw, final_pw, return_rawpoint_timestamps, version)
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
    """
    assert isinstance(
        initial_pw, (int, pointwidth)
    ), "Please provide `initial_pw` as an integer or pointwidth object"
    if isinstance(initial_pw, int):
        initial_pw = pointwidth(
            initial_pw
        )  # convert initial_pw integer to pointwidth object
    windows = stream.arrow_aligned_windows(
        start, end, int(initial_pw), version
    ).to_pylist()
    for window in windows:
        # Get the time range of the current window
        wstart = window["time"]
        wend = wstart + pd.Timedelta(initial_pw.nanoseconds, unit="ns")

        # if the minimum aggregate is above threshold, return full window
        if not return_rawpoint_timestamps and window["min"] > threshold:
            yield wstart, wend
        elif window["max"] > threshold:
            # If we are at a window length of a max_depth, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []
                yield wstart, wend
            elif return_rawpoint_timestamps and initial_pw <= final_pw:
                points, _ = stream.arrow_values(wstart, wend, version).to_pylist()
            # Otherwise, traverse the stat point children of this node
            else:
                points = search_timestamps_above_threshold(
                    stream,
                    wstart.value,
                    wend.value,
                    threshold,
                    initial_pw=initial_pw - 2,
                    final_pw=final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )
            for point in points:
                if isinstance(point, dict):
                    if point["value"] > threshold:
                        yield (point["time"],)
                else:
                    yield point


def search_timestamps_below_threshold(
    stream: Stream,
    start: int,
    end: int,
    threshold: Union[int, float],
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are below the specified `threshold`
    using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        Stream to search
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    threshold: float
        Find timestamps below threshold
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to search with RawPoints. Default is 36
        (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuples of start and end timestamps of the
        StatPoints windows that is above threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if below threshold for more than specified max
        depth, default ~1.15 minutes) timestamps of event.

    Example
    -------
    >>> result = search_timestamps_below_threshold(stream, start, end, threshold,)
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
    """
    assert isinstance(
        initial_pw, (int, pointwidth)
    ), "Please provide `initial_pw` as an integer or pointwidth object"
    if isinstance(initial_pw, int):
        initial_pw = pointwidth(
            initial_pw
        )  # convert initial_pw integer to pointwidth object
    windows = stream.arrow_aligned_windows(
        start, end, int(initial_pw), version
    ).to_pylist()
    for window in windows:
        # Get the time range of the current window
        wstart = window["time"]
        wend = wstart + pd.Timedelta(initial_pw.nanoseconds, unit="ns")

        # if the minimum aggregate is above threshold, return full window
        if not return_rawpoint_timestamps and window["max"] < threshold:
            yield wstart, wend
        elif window["min"] < threshold:
            # If we are at a window length of a max_depth, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []
                yield wstart, wend
            elif return_rawpoint_timestamps and initial_pw <= final_pw:
                points = stream.arrow_values(wstart, wend, version).to_pylist()
            # Otherwise, traverse the stat point children of this node
            else:
                points = search_timestamps_below_threshold(
                    stream,
                    wstart.value,
                    wend.value,
                    threshold,
                    initial_pw=initial_pw - 2,
                    final_pw=final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )
            for point in points:
                if isinstance(point, dict):
                    if point["value"] < threshold:
                        yield (point["time"],)
                else:
                    yield point


def search_timestamps_outside_bounds(
    stream: Stream,
    start: int,
    end: int,
    bounds: Tuple[float, float],
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are outside specified bounds values (lower
    and upper bounds) using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        BTrDB Stream to search.
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    bounds: tuple(float, float), length: 2
        Find events with values outside the specified (lower, upper) bounds threshold.
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to
        search with RawPoints. Default is 36 (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuple of start and end
        timestamps of the StatPoint windows that is outside the specified bounds threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if above threshold for more than specified max
        depth default is ~1.15 minutes) timestamps of event.

    Example
    -------
    For measurement stream with values (280-290):
    >>> result = search_timestamps_outside_bounds(stream, start, end, bounds=(280.0, 290.0))
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
    []
    """
    assert len(bounds) == 2, "Requires a minimum and maximum for the `bounds`"
    lower_bound, upper_bound = bounds
    assert isinstance(
        initial_pw, (int, pointwidth)
    ), "Please provide `initial_pw` as an integer or pointwidth object"
    if isinstance(initial_pw, int):
        initial_pw = pointwidth(
            initial_pw
        )  # convert initial_pw integer to pointwidth object
    windows = stream.arrow_aligned_windows(
        start, end, int(initial_pw), version
    ).to_pylist()
    for window in windows:
        # Get the time range of the current window
        wstart = window["time"]
        wend = wstart + pd.Timedelta(initial_pw.nanoseconds, unit="ns")
        # if the aggregates are outside the bounds, return full window
        if not return_rawpoint_timestamps and (
            window["min"] > upper_bound or window["max"] < lower_bound
        ):
            yield wstart, wend
        elif window["min"] < lower_bound or window["max"] > upper_bound:
            # If we are at a window length of a final_pw, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []

                yield wstart, wend
            elif initial_pw <= final_pw and return_rawpoint_timestamps:
                points = stream.arrow_values(wstart, wend, version).to_pylist()
            # Otherwise, traverse the stat point children of this node
            else:
                points = search_timestamps_outside_bounds(
                    stream,
                    wstart.value,
                    wend.value,
                    bounds,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if point["value"] < lower_bound or point["value"] > upper_bound:
                        yield (point["time"],)
                else:
                    yield point


def search_timestamps_within_bounds(
    stream: Stream,
    start: int,
    end: int,
    bounds: Tuple[float, float],
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find timestamps in stream between `start` and `end` that are within specified bounds values
    (lower and upper bounds) using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        BTrDB Stream to search.
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    bounds: tuple(float, float), length: 2
        Find events with values within the specified (lower, upper) bounds threshold.
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to
        search with RawPoints. Default is 36 (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuple of start and end
        timestamps of the StatPoint windows that is within the specified bounds threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if within bounds for more than specified max
        depth default is ~1.15 minutes) timestamps of event.

    Example
    -------
    For measurement stream with values (280-290):

    >>> result = search_timestamps_outside_bounds(stream, start, end, bounds=(0, 1))
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
    []

    """
    # TODO: add more examples cases
    assert len(bounds) == 2, "Requires a minimum and maximum for the `bounds`"
    lower_bound, upper_bound = bounds
    assert isinstance(
        initial_pw, (int, pointwidth)
    ), "Please provide `initial_pw` as an integer or pointwidth object"
    if isinstance(initial_pw, int):
        initial_pw = pointwidth(
            initial_pw
        )  # convert initial_pw integer to pointwidth object
    windows = stream.arrow_aligned_windows(
        start, end, int(initial_pw), version
    ).to_pylist()
    for window in windows:
        # Get the time range of the current window
        wstart = window["time"]
        wend = wstart + pd.Timedelta(initial_pw.nanoseconds, unit="ns")

        # if the aggregates are with the bounds, return full window
        if not return_rawpoint_timestamps and (
            lower_bound <= window["min"] and window["max"] <= upper_bound
        ):
            yield wstart, wend
        elif not (window["max"] < lower_bound or window["min"] > upper_bound):
            # If we are at a window length of a final_pw, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []
                yield wstart, wend
            elif initial_pw <= final_pw and return_rawpoint_timestamps:
                points = stream.arrow_values(wstart, wend, version).to_pylist()
            # Otherwise, traverse the stat point children of this node
            else:
                points = search_timestamps_within_bounds(
                    stream,
                    wstart.value,
                    wend.value,
                    bounds,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if lower_bound <= point["value"] <= upper_bound:
                        yield (point["time"],)
                else:
                    yield point


def search_timestamps_at_value(
    stream: Stream,
    start: int,
    end: int,
    value: Union[int, float],
    tol: float = 1e-6,
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are equal to specified values (with
    tolerance) using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        BTrDB Stream to search.
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    value : int, float
        The values to search in the tree.
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to
        search with RawPoints. Default is 36 (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuple of start and end
        timestamps of the StatPoint windows that is outside the specified bounds threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if above threshold for more than specified max
        depth default is ~1.15 minutes) timestamps of event.

    Example
    -------
    >>> result = search_timestamps_at_value(stream, start, end, value)
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
    """
    # TODO: add more examples cases
    bounds = (value - tol, value + tol)
    yield from search_timestamps_within_bounds(
        stream,
        start,
        end,
        bounds,
        initial_pw - 2,
        final_pw,
        return_rawpoint_timestamps=return_rawpoint_timestamps,
        version=version,
    )


def search_timestamps_at_agg_value(
    stream: Stream,
    start: int,
    end: int,
    value: Union[int, float],
    agg: str,
    tol=1e-3,
    initial_pw: Union[int, pointwidth] = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are equal to the specified `threshold`
    using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        Stream to search
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
    value: tuple of (agg,threshold)
        Find values with given aggregate and threshold value.
    initial_pw: int or pointwidth, default: 49
        Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to search with RawPoints. Default is 36
        (approximately 1.15 minutes).
    return_rawpoint_timestamps: bool, default: False
        Return RawPoint timestamps if `True`, else returns time-range tuples of start and end timestamps of the
        StatPoints windows that is equal to threshold.
    version: int, default: 0
        Version of the stream to search from.

    Yields
    ------
    tuple
        Timestamp (nanoseconds) of start (and end if equal to threshold for more than specified max depth, default
        ~1.15 minutes) timestamps of event.
    """
    all_measure_aggs = ["min", "mean", "max"]
    assert isinstance(
        initial_pw, (int, pointwidth)
    ), "Please provide `initial_pw` as an integer or pointwidth object"
    if isinstance(initial_pw, int):
        initial_pw = pointwidth(
            initial_pw
        )  # convert initial_pw integer to pointwidth object
    windows = stream.arrow_aligned_windows(
        start, end, int(initial_pw), version
    ).to_pylist()
    for window in windows:
        wstart = window["time"]
        wend = wstart + pd.Timedelta(initial_pw.nanoseconds, unit="ns")
        if not return_rawpoint_timestamps and all(
            window[_agg] == value for _agg in all_measure_aggs
        ):
            # only returns the window if the window's mean is close to value and stddev are smaller than 2x tolerance
            yield (wstart, wend)
        elif (value - tol) <= window[agg] <= (value + tol):
            # If we are at a window length of a max_depth, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []
                yield (wstart, wend)
            elif return_rawpoint_timestamps and initial_pw <= final_pw:
                points = stream.arrow_values(wstart, wend, version).to_pylist()
            else:
                points = search_timestamps_at_agg_value(
                    stream,
                    wstart.value,
                    wend.value,
                    value,
                    agg,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if value - tol <= point["value"] <= value + tol:
                        yield (point["time"],)
                else:
                    yield point


# def search_change_timestamps(
#     stream,
#     start,
#     end,
#     delta,
#     initial_pw=49,
#     final_pw=36,
#     version=0,
#     ):
#     """
#     Finding all the sags(values below threshold and above bad_value_threshold) using a depth first search tree traversal
#     algorithm in the specified stream. The search of the sags will end when the pointwidth ends at final_pw.
#
#     Parameters
#     ----------
#     stream : btrdb.Stream
#         Stream object.
#     start : int or float
#         Time to start finding sags.
#     end : int or float
#         Time to stop finding sags.
#     delta : int or float
#          Find absolute percentage change values to mean above this threshold.
#     bad_value_threshold : int or float, default=0
#          Find values below threshold and above bad_value_threshold.
#     initial_pw : int, default=49
#         Initial query pointwidth of tree traversal, Default is 49 (approximately 7 days).
#     final_pw: int, default: 36
#         Final pointwidth depth to use tree traversal with StatPoints and to search with RawPoints. Default is 36
#         (approximately 1.15 minutes).
#     version : int, default=0
#         Stream version.
#
#     Yield
#     ----------
#     point : tuple
#       (window_start_time, window_end_time, min_value).
#     """
#     # Ensure pw is a pointwidth object
#     pw = pointwidth(initial_pw)
#     windows = stream.arrow_aligned_windows(
#         start, end, int(pw), version
#         ).to_pylist()
#
#     for window in windows:
#         wstart = window['time']
#         wend = wstart + pw.nanoseconds
#
#         min_value, mean_value, max_value = (window[agg] for agg in ['min', 'mean','max'])
#
#         # skip window if the change between max or min to mean value of is below specified delta
#         if abs(max_value - mean_value) < delta or abs(mean_value - min_value) < delta:
#             continue
#         #TODO: use z-score of min/max
#
#         # if max and min of a time window both below delta but above
#         elif True:
#             yield wstart, wend, min_value
#
#         # if the min of a time window is below threshold and mean is above threshold,
#         # traverse down the tree to search for sags
#         elif min_value <= threshold and mean_value > threshold:
#
#             # yield  the start, end time and min value of the time window at the final pw
#             if initial_pw == final_pw:
#                 if min_value > bad_value_threshold:
#                     yield wstart, wend, min_value
#             else:
#                 # traverse down the tree 6 levels at time (due to tree structure) if (initial_pw - final_pw) >= 6
#                 if (initial_pw - final_pw) >= 6:
#                     traverse_step = 6
#                 else:
#                     traverse_step = 1
#                 points = search_change_timestamps(
#                     stream,
#                     wstart.value,
#                     wend.value,
#                     threshold,
#                     bad_value_threshold,
#                     initial_pw=initial_pw - traverse_step,
#                     final_pw=final_pw,
#                     version=version,
#                     )
#
#                 for point in points:
#                     yield point


def search_sags_timestamps(
    stream,
    start,
    end,
    threshold,
    bad_value_threshold=0,
    initial_pw=44,
    final_pw=36,
    version=0,
):
    """
    Finding all the sags(values below threshold and above bad_value_threshold) using a depth first
    search tree traversal algorithm in the specified stream. The search of the sags will end when
    the pointwidth ends at final_pw.

    Parameters
    ----------
    stream : btrdb.Stream
        Stream object.
    start : int or float
        Time to start finding sags.
    end : int or float
        Time to stop finding sags.
    threshold : int or float
         Find values below threshold and above bad_value_threshold.
    bad_value_threshold : int or float, default=0
         Find values below threshold and above bad_value_threshold.
    initial_pw : int, default=44
        Initial query pointwidth of tree traversal, Default is 44 (approximately 4.89 hours).
    final_pw: int, default: 36
        Final pointwidth depth to use tree traversal with StatPoints and to search with RawPoints.
        Default is 36 (approximately 1.15 minutes).
    version : int, default=0
        Stream version.

    Yield
    ----------
    point : tuple
        Timestamps of the detected sag (points with values below the sag threshold) with start and
        end timestamps.
    """
    # Ensure pw is a pointwidth object
    pw = pointwidth(initial_pw)

    # Begin by collecting all stat points at the specified pointwidth
    # Note that zip creates a list of windows and versions and we ignore the versions
    statpoints, _ = zip(*stream.aligned_windows(start, end, pw, version))
    # Traversing from left to right from the windows
    for sp in statpoints:
        wstart = sp.time
        wend = sp.time + pw.nanoseconds

        min_value = sp.min
        max_value = sp.max
        mean_value = sp.mean

        # skip timewindow if the max or the mean value of a time window is below bad_value_threshold
        # this is to avoid mixing up true voltage sags caused by event with sags caused by DQ issue
        if max_value < bad_value_threshold or mean_value < bad_value_threshold:
            continue

        # if max and min of the a time window are both below threshold and above bad_value_threshold
        # while mean is above threshold, this means all the values in the time window is within the
        # boundary of acceptable sags threshold
        elif (
            bad_value_threshold < max_value < threshold
            and bad_value_threshold < min_value < threshold
            and mean_value > threshold
        ):
            yield wstart, wend, min_value

        # if the min of a time window is below threshold and mean is above threshold,
        # traverse down the tree to search for sags
        elif min_value <= threshold and mean_value > threshold:
            # yield the start, end time and min value of the time window at the final pw
            if initial_pw == final_pw:
                if min_value > bad_value_threshold:
                    yield wstart, wend, min_value
            else:
                # traverse down the tree 6 levels at time
                traverse_step = 6 if (initial_pw - final_pw) >= 6 else 1

                points = search_sags_timestamps(
                    stream,
                    wstart.value,
                    wend.value,
                    threshold,
                    bad_value_threshold,
                    initial_pw=initial_pw - traverse_step,
                    final_pw=final_pw,
                    version=version,
                )

                # Yield all points to the calling function
                for point in points:
                    yield point
