import re
import warnings
from tabulate import tabulate

import btrdb
from btrdb.stream import StreamSet, Stream
from btrdb.utils.general import pointwidth
from btrdb.utils.timez import ns_delta, to_nanoseconds

KNOWN_DISTILLER_TYPES = ["repeats", "duplicate-times", "zeros"]

class AmbiguousDistillateError(ValueError):
    """
    Raised when a distillate stream's name contains references to multiple known
    data quality distiller names
    """
    pass

class DQDistillate(Stream):
    """
    Subsets a Stream object and allows for identfication of data quality events

    Parameters
    ----------
    btrdb : BTrDB
        A reference to the BTrDB object connecting this stream back to the
		physical server.
    uuid : UUID
        The unique UUID identifier for this stream.
    """
    def __init__(self, btrdb, uu):
        # gives all same attrs/methods as Stream
        super().__init__(btrdb, uu)

        # NOTE: this involves determining distiller type based on the distillate 
        # stream name, so we will need to be careful how we name distillates
        types = re.findall(r"(?=("+'|'.join(KNOWN_DISTILLER_TYPES)+r"))", self.name)
        if len(types) == 0:
            raise ValueError(f"unknown distiller type. Must be one of [{', '.join(KNOWN_DISTILLER_TYPES)}]")
        if len(types) > 1:
            raise AmbiguousDistillateError(f"ambiguous distiller name. contains references to [{', '.join(types)}]")
        self.type = types[0]

    def contains_issue(self, start=None, end=None, depth=30):
        """
        Tells whether a distillate stream contains an event, which is denoted by 1 values

        Parameters
        ----------
        start: (optional) datetime, datetime64, float, str
            start time of period to search for events
        end: (optional) datetime, datetime64, float, str
            end time of period to search for events
        depth: (optional) int
            The precision of the window duration as a power of 2 in nanoseconds.
            e.g 30 would make the window duration accurate to roughly 1 second

        Returns
        -------
        bool
            Returns bool indicating whether or not the distillate stream contains an event
        """
        # NOTE: need to be careful about unpacking initially in case earliest or latest
        # returns None, which happens when there is no data
        if start is None:
            earliest = self.earliest()
            start = earliest[0].time if earliest else None

        if end is None:
            latest = self.latest()
            # adding 1 to end time because end is exclusive in windows()
            end = latest[0].time +1 if latest else None

        start, end = to_nanoseconds(start), to_nanoseconds(end)
        # There's no event if there's no data
        if start is None and end is None:
            return False
        width = end - start
        windows, _ = zip(*self.windows(start, end, width, depth))
        return any(w.max >= 1 for w in windows)

    def __repr__(self):
        return f"DQDistillate collection={self.collection}, name={self.name}, type={self.type}"

class DQStream(Stream):
    """
    Subsets Stream object. Contains a regular BTrDB stream along with its distillate Streams

    Parameters
    ----------
    stream: btrdb.stream.Stream
    """
    def __init__(self, stream):
        # gives all same attrs/methods as Stream
        super().__init__(stream._btrdb, stream.uuid)
        self._distillates = self._get_distillates()
		
    def _get_distillates(self):
        """
        Finds distillate Streams for each of the underlying source Streams

        Returns
        -------
        list[DQDistillate]
            list of distillate Streams
        """
        # NOTE: This involves looking up distillate streams by their source_uuid annotation, so we
        # need to make sure that all distillers give output streams this annotation
        distillates = []
        for stream in self._btrdb.streams_in_collection(annotations={"source_uuid": str(self.uuid)}):
            try:
                distillates.append(DQDistillate(stream._btrdb, stream.uuid))
            # we only want to raise this error if we aren't sure which distiller the stream is
            # referring to, it's fine if it's a non data quality distiller, that is expected
            except ValueError as e:
                if isinstance(e, AmbiguousDistillateError):
                    raise
                else:
                    continue
        if len(distillates) < 1:
            warnings.warn(f"Could not find any data quality distillates for stream {str(self.uuid)}")
        return distillates
		
    @property
    def distillates(self):
        """
        Returns list of distillate Streams
        """
        return self._distillates
        
    def list_distillates(self):
        """
        Outputs dict that shows which distillates the DQStream has available

        Returns
        -------
        dict
            Dict containing uuid, collection, and name of the DQStream, as well as bool values
            for each distiller that denote which distillates are available to the DQStream
        """
        info = {
            "uuid": str(self.uuid),
            "collection": self.collection,
            "name": self.name,
        }
        distillers = {
            distiller: True if self[distiller]
            else False for distiller in KNOWN_DISTILLER_TYPES
        }
        # combine basic info with distiller info
        return {**info, **distillers}

    def contains_any_issue(self, start=None, end=None, depth=30):
        """
        Indicates whether this group of streams contains any data quality events

        Parameters
        ----------
        start: (optional) datetime, datetime64, float, str
            start time of period to search for events
        end: (optional) datetime, datetime64, float, str
            end time of period to search for events
        depth: (optional) int
            The precision of the window duration as a power of 2 in nanoseconds.
            e.g 30 would make the window duration accurate to roughly 1 second
        
        Returns
        -------
        bool
            Returns bool indicating whether or not any of the underlying streams
            contain any event
        """
        if len(self._distillates) == 0:
            return None
        for distillate in self._distillates:
            if distillate.contains_issue(start=start, end=end, depth=depth):
                return True
        return False

    def contains_issue(self, distil_type, start=None, end=None, depth=30):
        """
        Indicates whether this group of streams contains a specific data quality event

        Parameters
        ----------
        distil_type: str
            The type of event to search for. Must be one of KNOWN_DISTILLER_TYPES
        start: (optional) datetime, datetime64, float, str
            start time of period to search for events
        end: (optional) datetime, datetime64, float, str
            end time of period to search for events
        depth: (optional) int
            The precision of the window duration as a power of 2 in nanoseconds.
            e.g 30 would make the window duration accurate to roughly 1 second
        
        Returns
        -------
        bool
            Returns bool indicating whether or not any of the underlying streams contain
            a certain event
        """
        try:
            distillate = self[distil_type]
        except KeyError:
            return None
        return distillate.contains_issue(start=start, end=end, depth=depth)
    
    def __getitem__(self, item):
        for distillate in self._distillates:
            if distillate.type == item:
                return distillate
        raise KeyError(f"DQDistillate with type '{item}' not found")

    def __repr__(self):
        return f"DQStream collection={self.collection}, name={self.name}"
		
class DQStreamSet(StreamSet):
    """
    Subsets a StreamSet object. Contains a list of Streams along with each of their
    distillate Streams

    Parameters
    ----------
    streams: list
        list[btrdb.stream.Stream]
    """
    def __init__(self, streams):		
        dq_streams = []
        for stream in streams:
            if not isinstance(stream, DQStream):
                stream = DQStream(stream)
            dq_streams.append(stream)
        # gets everything that a StreamSet has
        super().__init__(dq_streams)
        # TODO: this feels hacky, how should we address this? Nowhere else in StreamSet
        # do we access a BTrDB connection directly, everything is usually done
        # at the Stream level.
        self._conn = self._streams[0]._btrdb

    def describe(self, notebook=False, *additional_cols):
        """
        Outputs table describing metadata of distillate streams

        Parameters:
        notebook: bool
            Whether or not this function is run from a notebook. Ensures
            legible formatting
        *additional_cols: str
             additional columns to include in output table. Will result in empty values
             if they are not found in a stream's tags or annotations

        Returns:
        str
            A tabulated representation of each underlying stream's information
        """
        fmt = "html" if notebook else None
        # used to decide if user provided an arg that requires us to
        # query for a stream's annotations
        KNOWN_TAGS = ["name", "unit", "ingress", "distiller"]
        contains_annotations = False

        table = [["Collection", "Name", "Unit", "UUID", "Version", "Available Data Quality Info"]]

        # add args as table columns if user provides them
        if additional_cols:
            if not all(a in KNOWN_TAGS for a in additional_cols):
                contains_annotations=True
            table[0].extend(additional_cols)

        # query for metadata for all streams upfront
        # store metadata results in a dict where uuid is the key and metadata dict is values
        uuids = [str(stream.uuid) for stream in self._streams]
        uu_str = ",".join(f"'{uu}'" for uu in uuids)
        if not contains_annotations:
            query = f"SELECT uuid, name, unit, distiller, ingress FROM streams WHERE uuid IN ({uu_str})"
            meta = {res["uuid"]: {tag: res.get(tag) for tag in KNOWN_TAGS} for res in self._conn.query(query)}
        else:
            query = f"""
                     SELECT uuid, annotations, name, unit, distiller, ingress
                     FROM streams
                     WHERE uuid IN ({uu_str})
                 """
            meta = {
             res["uuid"]: {**res["annotations"], **{tag: res.get(tag) for tag in KNOWN_TAGS}}
             for res in self._conn.query(query)
         }

        # iterate through streams, lookup metadata by uuid
        for stream in self._streams:
            stream_meta = meta[str(stream.uuid)]
            dqinfo = ", ".join([distillate.type for distillate in stream.distillates])
            if not dqinfo:
                dqinfo = "N/A"

            temp = [
                stream.collection,
                stream.name,
                stream_meta["unit"],
                str(stream.uuid)[:8] + "...",
                stream.version(),
                dqinfo,
            ]
            # add additional data to row if additional cols provided
            if additional_cols:
                # if a metadata key is not found, it will just be blank in the table
                temp.extend([stream_meta.get(col) for col in additional_cols])
            table.append(temp)
        return tabulate(table, headers="firstrow", tablefmt=fmt)

    def list_distillates(self):
        """
        Outputs table showing which distillates each underlying Stream
        has available

        Returns
        -------
        list
            list of dicts containing uuid, collection, and name of the DQStream,
            as well as bool values for each distiller that denote which distillates
            are available to the DQStream
        """
        return [
            stream.list_distillates()
            for stream in self._streams
        ]

    def contains_any_issue(self, start=None, end=None, depth=30):
        """
        Indicates whether this group of streams contains any data quality events

        Parameters
        ----------
        start: (optional) datetime, datetime64, float, str
            start time of period to search for events
        end: (optional) datetime, datetime64, float, str
            end time of period to search for events
        depth: (optional) int
            The precision of the window duration as a power of 2 in nanoseconds.
            e.g 30 would make the window duration accurate to roughly 1 second
        
        Returns
        -------
        dict[str, bool]
            Returns dict indicating whether or not each of the underlying streams
            contain any event
        """
        return {
            str(stream.uuid): stream.contains_any_issue(start=start, end=end, depth=depth)
            for stream in self._streams
        }

    def contains_issue(self, distil_type, start=None, end=None, depth=30):
        """
        Indicates whether this group of streams contains a specific data quality event

        Parameters
        ----------
        distil_type: str
            The type of event to search for. Must be one of KNOWN_DISTILLER_TYPES
        start: (optional) datetime, datetime64, float, str
            start time of period to search for events
        end: (optional) datetime, datetime64, float, str
            end time of period to search for events
        depth: (optional) int
            The precision of the window duration as a power of 2 in nanoseconds.
            e.g 30 would make the window duration accurate to roughly 1 second
        
        Returns
        -------
        dict[str, bool]
            Returns dict indicating whether or each of the underlying streams contain
            a certain event
        """
        return {
            str(stream.uuid): stream.contains_issue(distil_type, start=start, end=end, depth=depth)
            for stream in self._streams
        }
    
    def __getitem__(self, index):
        """
        Returns the DQStream contained at a given index within the set

        Parameters
        ----------
        index: int
            The index of the desired stream.

        Returns
        -------
        DQStream
            The DQStream stored in this object at the given index
        """
        return self._streams[index]

    def __repr__(self):
        token = "stream" if len(self) == 1 else "streams"
        return f"<{self.__class__.__name__} ({len(self._streams)} {token})>"
