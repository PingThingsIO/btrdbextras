import uuid
from unittest.mock import Mock, PropertyMock

import pandas as pd
import pytest
from ds_utils.ds_api.stream_info import ANGLE, describe_streams, StreamType, VOLTAGE
from pandas.testing import assert_frame_equal

from btrdb import BTrDB
from btrdb.stream import Stream, StreamSet


@pytest.fixture(scope="session")
def mock_stream1():
    uu = uuid.UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb25921")
    stream = Mock(Stream)
    type(stream)._btrdb = PropertyMock(return_value=Mock(BTrDB))
    type(stream).uuid = PropertyMock(return_value=uu)
    type(stream).collection = PropertyMock(return_value="animals/dog")
    type(stream).name = PropertyMock(return_value="bob")
    type(stream).unit = PropertyMock(return_value="puppy")

    stream.annotations = Mock(
        return_value=({"color": "red", "animals": "dog", "breed": "corgi"}, 11)
    )
    stream.tags = Mock(
        return_value={"name": "bob", "unit": "puppy", "distiller": "", "ingress": ""}
    )

    # stream.windows = Mock(return_value=[()])

    return stream


@pytest.fixture(scope="session")
def mock_stream2():
    uu = uuid.UUID("0d22a53b-e2ef-4e0a-ab89-b2d48fb25922")
    stream = Mock(Stream)
    type(stream)._btrdb = PropertyMock(return_value=Mock(BTrDB))
    type(stream).uuid = PropertyMock(return_value=uu)
    type(stream).collection = PropertyMock(return_value="animals/dog")
    type(stream).name = PropertyMock(return_value="peter")
    type(stream).unit = PropertyMock(return_value="puppy")

    stream.annotations = Mock(
        return_value=({"color": "green", "animals": "dog", "breed": "pug"}, 11)
    )
    stream.tags = Mock(
        return_value={"name": "peter", "unit": "puppy", "distiller": "", "ingress": ""}
    )

    stream.values = Mock(return_value=[()])

    return stream


class TestStreamInfo:
    def test_describe_streams_return_tags_only(self, mock_stream1, mock_stream2):
        """
        Assert describe_streams() returns all the tags along with UUID and collection.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        stream1_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
            "name": "bob",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
        }

        stream2_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
            "name": "peter",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
        }

        assert_frame_equal(
            describe_streams(streamset), pd.DataFrame([stream1_dict, stream2_dict])
        )

    def test_describe_streams_return_all_info(self, mock_stream1, mock_stream2):
        """
        Assert describe_streams() returns all the tags and annotations along with UUID and collection.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        stream1_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
            "name": "bob",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
            "color": "red",
            "animals": "dog",
            "breed": "corgi",
        }

        stream2_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
            "name": "peter",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
            "color": "green",
            "animals": "dog",
            "breed": "pug",
        }

        assert_frame_equal(
            describe_streams(streamset, display_annotations=True),
            pd.DataFrame([stream1_dict, stream2_dict]),
        )

    def test_describe_streams_return_filter_annotations(
        self, mock_stream1, mock_stream2
    ):
        """
        Assert describe_streams() returns all the tags and specified annotations along with UUID and collection.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        stream1_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
            "name": "bob",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
            "color": "red",
        }

        stream2_dict = {
            "collection": "animals/dog",
            "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
            "name": "peter",
            "unit": "puppy",
            "distiller": "",
            "ingress": "",
            "color": "green",
        }
        assert_frame_equal(
            describe_streams(
                streamset, display_annotations=True, filter_annotations=["color"]
            ),
            pd.DataFrame([stream1_dict, stream2_dict]),
        )

    def test_describe_streams_return_filter_annotations_nonexist(
        self, mock_stream1, mock_stream2
    ):
        """
        Assert describe_streams() returns all the tags and specified annotations along with UUID and collection.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        # stream1_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
        #     "name": "bob",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "red",
        # }
        #
        # stream2_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
        #     "name": "peter",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "green",
        # }

        with pytest.raises(
            ValueError, match=r"(?<=\[)(.*?)(?=] not found in annotations)"
        ):
            assert describe_streams(
                streamset, display_annotations=True, filter_annotations=["address"]
            )

    def test_describe_streams_return_filter_annotations_wrong_type_str(
        self, mock_stream1, mock_stream2
    ):
        """
        Assert describe_streams() throws an exception when the input type is wrong.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        # stream1_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
        #     "name": "bob",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "red",
        # }
        #
        # stream2_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
        #     "name": "peter",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "green",
        # }

        with pytest.raises(
            TypeError, match="filter_annotations has to be a list of str."
        ):
            assert describe_streams(
                streamset, display_annotations=True, filter_annotations="location"
            )

    def test_describe_streams_return_filter_annotations_wrong_type_int_list(
        self, mock_stream1, mock_stream2
    ):
        """
        Assert describe_streams() throws an exception when the input type is wrong.
        """
        streamset = StreamSet([mock_stream1, mock_stream2])
        # stream1_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25921",
        #     "name": "bob",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "red",
        # }
        #
        # stream2_dict = {
        #     "collection": "animals/dog",
        #     "UUID": "0d22a53b-e2ef-4e0a-ab89-b2d48fb25922",
        #     "name": "peter",
        #     "unit": "puppy",
        #     "distiller": "",
        #     "ingress": "",
        #     "color": "green",
        # }

        with pytest.raises(
            TypeError, match="filter_annotations has to be a list of str."
        ):
            assert describe_streams(
                streamset, display_annotations=True, filter_annotations=[1, 2, 3]
            )

    def test_streamtype_from_unit(self):
        truths = {"VPHM": StreamType(VOLTAGE, 1), "VPHA": StreamType(ANGLE, 1)}
        for unit in truths.keys():
            assert truths[unit].unit_type == StreamType.from_unit(unit).unit_type

    def test_find_samplerate(self, mock_stream1):
        assert False
