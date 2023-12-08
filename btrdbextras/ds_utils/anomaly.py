"""
Module for general anomaly detection functions using recursive Tree search.

NOTE: Using pyarrow.Table.to_pylist and iterating is ~9.36x more performant than using
pyarrow.Table.to_pandas().itertuples() and iterating.

#TODO: traverse the tree in increements of 6?
"""


from typing import Tuple, Union

import btrdb
from btrdb.stream import Stream
from btrdb.utils.general import pointwidth
from btrdb.utils.timez import ns_to_datetime

__all__ = [
    "search_timestamps_above_threshold",
    "search_timestamps_below_threshold",
    "search_timestamps_outside_bounds",
    "search_timestamps_at_agg_value",
]


def _calculate_bounds_severity(
    value: float, lower_bound: float, upper_bound: float
) -> float:
    """Calculate severity based on how far away from the bounds."""
    if value < lower_bound:
        severity = abs(lower_bound - value)
    else:  # this is the other condition where `point['value'] > upper_bound`
        severity = abs(upper_bound - value)
    return severity


def search_timestamps_above_threshold(
    stream: Stream,
    threshold: Union[int, float],
    start: int,
    end: int,
    initial_pw: int = 49,
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
    threshold: float
        Find values above threshold
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
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
        wstart = window["time"].value
        wend = wstart + initial_pw.nanoseconds

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
                    threshold,
                    wstart,
                    wend,
                    initial_pw=initial_pw - 2,
                    final_pw=final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )
            for point in points:
                if isinstance(point, dict):
                    if point["value"] > threshold:
                        yield (point["time"].value,)
                else:
                    yield point


def search_timestamps_below_threshold(
    stream: Stream,
    threshold: Union[int, float],
    start: int,
    end: int,
    initial_pw: int = 49,
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
    threshold: float
        Find values above threshold
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
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
        wstart = window["time"].value
        wend = wstart + initial_pw.nanoseconds

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
                    threshold,
                    wstart,
                    wend,
                    initial_pw=initial_pw - 2,
                    final_pw=final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )
            for point in points:
                if isinstance(point, dict):
                    if point["value"] < threshold:
                        yield (point["time"].value,)
                else:
                    yield point


def search_timestamps_outside_bounds(
    stream: Stream,
    start: int,
    end: int,
    bounds: Tuple[float, float],
    initial_pw: int = 49,
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
    return_severity : bool, default: True
        Whether to return the severity level of the event detected. Default is True.
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
        wstart = window["time"].value
        wend = wstart + initial_pw.nanoseconds
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
                    wstart,
                    wend,
                    bounds,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if point["value"] < lower_bound or point["value"] > upper_bound:
                        severity = _calculate_bounds_severity(
                            point["value"], lower_bound, upper_bound
                        )
                        yield (point["time"],)
                else:
                    yield point


def search_timestamps_within_bounds(
    stream: Stream,
    start: int,
    end: int,
    bounds: Tuple[float, float],
    initial_pw: int = 49,
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
    return_severity : bool, default: True
        Whether to return the severity level of the event detected. Default is True.
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
        wstart = window["time"].value
        wend = wstart + initial_pw.nanoseconds

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
                    wstart,
                    wend,
                    bounds,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if lower_bound <= point["value"] <= upper_bound:
                        yield (point["time"].value,)
                else:
                    yield point


def search_timestamps_at_value(
    stream: Stream,
    start: int,
    end: int,
    value: Union[int, float],
    tol: float = 1e-6,
    initial_pw: int = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are equal specified values (with some
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
    return_severity : bool, default: True
        Whether to return the severity level of the event detected. Default is True.
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
    value: Union[int, float],
    agg: str,
    start: int,
    end: int,
    tol=1e-3,
    initial_pw: int = 49,
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
    value: tuple of (agg,threshold)
        Find values with given aggregate and threshold value.
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
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
        wstart = window["time"].value
        wend = wstart + initial_pw.nanoseconds
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
                    value,
                    agg,
                    wstart,
                    wend,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if value - tol <= point["value"] <= value + tol:
                        yield (point["time"].value,)
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
#                     wstart,
#                     wend,
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
                    wstart,
                    wend,
                    threshold,
                    bad_value_threshold,
                    initial_pw=initial_pw - traverse_step,
                    final_pw=final_pw,
                    version=version,
                )

                # Yield all points to the calling function
                for point in points:
                    yield point
