import re
import btrdb
from btrdb.stream import StreamSet, Stream
from btrdb.utils.general import pointwidth
from btrdb.utils.timez import ns_delta, to_nanoseconds
from tabulate import tabulate
import warnings

KNOWN_DISTILLER_TYPES = ["repeats", "duplicate-times", "zeros"]

class Distillate(Stream):
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
            raise Exception(f"unknown distiller type. Must be one of [{','.join(KNOWN_DISTILLER_TYPES)}]")
        if len(types) > 1:
            raise Exception(f"ambiguous distiller name. contains references to [{', '.join(types)}]")            
        self.type = types[0]

    def contains_event(self, start=None, end=None, depth=30):
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
        start = to_nanoseconds(start) if start else self.earliest()[0].time
        end = to_nanoseconds(end) if end else self.latest()[0].time + 1
        width = end - start
        windows, _ = zip(*self.windows(start, end, width, depth))
        return any(w.max >= 1 for w in windows)
    
    def __repr__(self):
        return f"Distillate collection={self.collection}, name={self.name}, type={self.type}"

class DQStream(Stream):
    """
    Subsets StreamSet object. Contains an original stream along with its
    distillate Streams
    """
    def __init__(self, stream):
        # gives all same attrs/methods as Stream
        super().__init__(stream._btrdb, stream.uuid)
        self._distillates = self._get_distillates()
		
    def _get_distillates(self):
        """
        Finds distillate streams for each of the underlying source streams

        Returns
        -------
        list[Distillates]
            List of distillate streams
        """
        distillates = [
            Distillate(stream._btrdb, stream.uuid)
            for stream in self._btrdb.streams_in_collection(annotations={"source_uuid": str(self.uuid)})
        ]
        if len(distillates) < 1:
            warnings.warn(f"Could not find distillates for stream {str(self.uuid)}")
        return distillates
		
    @property
    def distillates(self):
        """
        Returns list of distillate streams
        """
        return self._distillates
    
    def describe(self):
        """
        Outputs table describing metadata of distillate streams
        """
        raise NotImplementedError
    
    def contains_any_event(self, start=None, end=None, depth=30):
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
        for distillate in self._distillates:
            if distillate.contains_event(start=start, end=end, depth=depth):
                return True
        return False

    def contains_event(self, distil_type, start=None, end=None, depth=30):
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
        distillate = self[distil_type]
        return distillate.contains_event(start=start, end=end, depth=depth)
    
    def __getitem__(self, item):
        for distillate in self._distillates:
            if distillate.type == item:
                return distillate
        raise KeyError(f"Distillate with type '{item}' not found")
		
class DQStreamSet(StreamSet):
    """
    Subsets a StreamSet object

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
        # self._conn = streams[0]._btrdb
        # self._distillates = self._get_distillates()
    
    @property
    def distillates(self):
        """
        Returns list of distillate streams
        """
        return [
		    stream._distillates
			for stream in self._streams
		]        
    
    def describe(self, *additional_cols):
        """
        Outputs table describing metadata of distillate streams

        Parameters:
         *additional_cols: str
             additional columns to include in output table. Will result in empty values
             if they are not found in a stream's tags or annotations

        Returns:
        str: A tabulated representation of each underlying stream's information
        """
        # used to decide if user provided an arg that requires us to
        # query for a stream's annotations
        KNOWN_TAGS = ["name", "unit", "ingress", "distiller"]
        contains_annotations = False

        table = [["Collection", "Name", "Unit", "UUID", "Version", "Available Data Quality Info"]]

        # TODO: this feels hacky, how should we address this? Nowhere else in StreamSet
        # do we access a BTrDB connection directly, everything is usually done
        # at the Stream level.
        conn = self._streams[0]._btrdb

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
            meta = {res["uuid"]: {tag: res.get(tag) for tag in KNOWN_TAGS} for res in conn.query(query)}
        else:
            query = f"""
                     SELECT uuid, annotations, name, unit, distiller, ingress
                     FROM streams
                     WHERE uuid IN ({uu_str})
                 """
            meta = {
             res["uuid"]: {**res["annotations"], **{tag: res.get(tag) for tag in KNOWN_TAGS}}
             for res in conn.query(query)
         }


        # iterate through streams, lookup metadata by uuid
        for stream in self._streams:
            stream_meta = meta[str(stream.uuid)]
            dqinfo = "\n".join([distillate.type for distillate in stream.distillates])
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
        return tabulate(table, headers="firstrow")

    
    def contains_any_event(self, start=None, end=None, depth=30):
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
        dict
            Returns bool indicating whether or not any of the underlying streams
            contain any event
        """
        for stream in self._streams:
            if stream.contains_any_event(start=start, end=end, depth=depth):
                return True
        return False

    def contains_event(self, distil_type, start=None, end=None, depth=30):
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
        for stream in self._streams:
            try:
                stream.contains_event(distil_type, start=start, end=end, depth=depth)
            except KeyError:
                continue

        distillate = self[distil_type]
        return distillate.contains_event(start=start, end=end, depth=depth)
    
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

if __name__ == "__main__":
    db = btrdb.connect(profile="d2")
    stream2 = db.stream_from_uuid("077d6745-e3ae-5795-b22d-1eb067abb360")
    stream1 = db.stream_from_uuid("9464f51f-e05a-5db1-a965-3c339f748081")
    dq = DQStreamSet([stream1, stream2])
    print(dq.distillates)
    print(dq.describe())