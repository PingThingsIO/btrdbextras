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
    bounds: Tuple[float, float],
    start: int,
    end: int,
    initial_pw: int = 49,
    final_pw: int = 36,
    return_rawpoint_timestamps: bool = False,
    return_severity=True,
    version: int = 0,
) -> tuple:
    """
    Find points in stream between `start` and `end` that are outside specified bounds values (lower
    and upper bounds) using StatPoints recursively through BTrDB tree.

    Parameters
    ----------
    stream: Stream
        BTrDB Stream to search.
    bounds: tuple(float, float), length: 2
        Find events with values outside the specified (lower, upper) bounds threshold.
    start: int
        The start time in nanoseconds for the range to search from.
    end: int
        The end time in nanoseconds for the range to search from.
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
    >>> result = search_timestamps_outside_bounds(stream, threshold, start, end, initial_pw, final_pw, return_rawpoint_timestamps, version)
    >>> result_timestamps = [timestamp for timestamp in result]
    >>> print(result_timestamps)
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
            if return_severity:
                point_value = (
                    window["min"] if window["min"] > upper_bound else window["max"]
                )
                severity = _calculate_bounds_severity(
                    point_value, lower_bound, upper_bound
                )
                yield wstart, wend, severity
            else:
                yield wstart, wend
        elif window["min"] < lower_bound or window["max"] > upper_bound:
            # If we are at a window length of a final_pw, use values
            if initial_pw <= final_pw and not return_rawpoint_timestamps:
                points = []
                if return_severity:
                    point_value = (
                        window["min"] if window["min"] < lower_bound else window["max"]
                    )
                    severity = _calculate_bounds_severity(
                        point_value, lower_bound, upper_bound
                    )
                    yield wstart, wend, severity
                else:
                    yield wstart, wend
            elif initial_pw <= final_pw and return_rawpoint_timestamps:
                points = stream.arrow_values(wstart, wend, version).to_pylist()
            # Otherwise, traverse the stat point children of this node
            else:
                points = search_timestamps_outside_bounds(
                    stream,
                    bounds,
                    wstart,
                    wend,
                    initial_pw - 2,
                    final_pw,
                    return_rawpoint_timestamps=return_rawpoint_timestamps,
                    return_severity=return_severity,
                    version=version,
                )

            for point in points:
                if isinstance(point, dict):
                    if point["value"] < lower_bound or point["value"] > upper_bound:
                        severity = _calculate_bounds_severity(
                            point["value"], lower_bound, upper_bound
                        )
                        yield (point["time"], severity) if return_severity else (
                            point["time"],
                        )
                else:
                    yield point


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
    stack = [(start, end, initial_pw)]
    while stack:
        wstart, wend, pw = stack.pop()
        windows = stream.arrow_aligned_windows(
            wstart, wend, int(pw), version
        ).to_pylist()
        for window in windows:
            wstart = window["time"].value
            wend = wstart + pw.nanoseconds
            if not return_rawpoint_timestamps and all(
                [window[_agg] == value for _agg in all_measure_aggs]
            ):
                # only returns the window if the window's mean is close to value and stddev are smaller than 2x tolerance
                yield (wstart, wend)
            elif (value - tol) <= window[agg] <= (value + tol):
                # If we are at a window length of a max_depth, use values
                if pw <= final_pw and not return_rawpoint_timestamps:
                    points = []
                    yield (wstart, wend)
                elif return_rawpoint_timestamps and pw <= final_pw:
                    points = stream.arrow_values(wstart, wend, version).to_pylist()
                else:
                    stack.append((wstart, wend, pw - 2))
                    continue

                for point in points:
                    if isinstance(point, dict):
                        if value - tol <= point["value"] <= value + tol:
                            yield (point["time"].value,)
                    else:
                        yield point


def find_sags_dfs(
    stream, tau, start=btrdb.MINIMUM_TIME, end=btrdb.MAXIMUM_TIME, pw=48, version=0
):
    """
    Finding all the sags(values below tau) using a depth first search algorithm in the specified stream.

    Parameters
    ----------
    stream : btrdb.Stream
        Stream object.
    tau : int or float
        Sag threshold. Points with values below this threshold will be marked as a sag.
    start : int or float, default=btrdb.MINIMUM_TIME
        Time to start finding sags.
    end : int or float, default=btrdb.MAXIMUM_TIME
        Time to stop finding sags.
    pw : int, default=48
        Pointwidth.
    version : int, default=0
        Stream version.

    Yield
    ----------
    point : int
      Timestamp of the detected sag(points with values below the sag threshold).
    """
    # Ensure pw is a pointwidth object
    pw = pointwidth(pw)

    # Begin by collecting all stat points at the specified pointwidth
    # Note that zip creates a list of windows and versions and we ignore the versions
    statpoints, _ = zip(*stream.aligned_windows(start, end, pw, version))
    # Traversing from left to right from the windows
    for sp in statpoints:
        # Check to see if the value is in the window
        if sp.min <= tau:
            # Get the time range of the current window
            wstart = sp.time
            wend = sp.time + pw.nanoseconds

            if pw <= 30:
                # If we are at a window length of a second, use values
                points, _ = zip(*stream.values(wstart, wend, version))
            else:
                # Otherwise, traverse the stat point children of this node
                points = find_sags_dfs(stream, tau, wstart, wend, pw - 1, version)

            # Yield all points to the calling function
            for point in points:
                if point.value <= tau:
                    yield point


def sag_survey(sags, between_sags_in_sec=1, limit=100, verbose=False):
    """
    Consolidate individual sag points into distinct sags based on the time between them.

    Parameters
    ----------
    sags : Generator
        Timestamps of individual sag points (points with values below the sag threshold).
    between_sags_in_sec : int or float, default=1
        Time gap threshold. Adjacent sags less than this value apart are combined into a single sag.
    limit : int, default=100
        Limit on number of sags to return.
    verbose : bool, default=False
        Progress of sag survey.

    Return
    ----------
    starts : list of int
        Starting timestamps of the unique sags found.
    durations : list of int
        Time duration of each sag in nanoseconds.
    magnitudes : list of int
        Minimum value found among the combined sags.
    sags_found : list of int
      Number of unique sags found.

    Examples
    ----------
    >>> vsags = find_sags_dfs(voltage_stream, voltage_thresh, start=start, end=end)
    >>> vsags_start_time, vsags_duration, vsags_magnitude, vsags_found = sag_survey(vsags, between_sags_in_sec=1)
    """
    # Initialize sag information
    starts = []
    durations = []
    magnitudes = []
    sags_found = []

    # Get the first sag
    sag = _safe_next(sags)
    if sag == None:
        print("No voltage sags found.")
    else:
        if verbose:
            print("Voltage sag found!")
        start, mag = sag
        dur = 0

    count = 0
    sags_count = 1
    while sag:
        sag = _safe_next(sags)
        # If we are on the last sag
        if (sag == None) or (count > limit):
            starts.append(start)
            durations.append(dur)
            magnitudes.append(mag)
            sags_found.append(sags_count)
            sag = None
        else:
            sag_time, sag_value = sag
            # Check if this is a different sag
            # More than 1s or more after last sag point
            if sag_time - (start + dur) > between_sags_in_sec * 1e9:
                if verbose:
                    print("Voltage sag found!", count)
                # Save last sag
                starts.append(start)
                durations.append(dur)
                magnitudes.append(mag)
                sags_found.append(sags_count)
                # Increment sag count
                count += 1
                # Initialize next sag
                start = sag_time
                mag = sag_value
                dur = 0
                sags_count = 0
            # Otherwise update properties of this sag
            else:
                dur = sag_time - start
                mag = min(mag, sag_value)
            sags_count += 1

    return starts, durations, magnitudes, sags_found


# A convenience for iterating through a generator
def _safe_next(iterable):
    """
    Helper function for sag_survey() to ensure it does not run into StopIteration error when all values have been evaluated.
    """
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return first
