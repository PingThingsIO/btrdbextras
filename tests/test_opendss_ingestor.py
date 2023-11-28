import os

import btrdb
from btrdb.utils.timez import to_nanoseconds

from btrdbextras.opendss_ingest.opendss_ingestor import (
    MODEL_REL_PATH,initialize_simulation,run_simulation
)


class TestOpendssIngestor:
    def test_initialize_simulation(self):
        # Arrange
        mock_model_loc = os.path.join(
            MODEL_REL_PATH,
            "Models/13Bus/IEEE13Nodeckt.dss"
            )
        load, load_names = ([1155., 160., 120., 120., 170., 230., 170.,
                             485., 68., 290., 170., 128., 17., 66., 117.],
                            ['671', '634a', '634b', '634c', '645', '646', '692',
                             '675a', '675b', '675c', '611', '652', '670a', '670b',
                             '670c'])
        # Act
        results = initialize_simulation(mock_model_loc)
        assert results[0].tolist() == load
        assert results[-1] == load_names

    def test_simulate_network(self):
        # load, load_names = ([1155., 160., 120., 120., 170., 230., 170.,
        #                      485., 68., 290., 170., 128., 17., 66., 117.],
        #                     ['671', '634a', '634b', '634c', '645', '646',
        #                      '692',
        #                      '675a', '675b', '675c', '611', '652', '670a',
        #                      '670b',
        #                      '670c'])
        start_time = to_nanoseconds('2023-01-01 00:00:00')
        end_time = to_nanoseconds('2023-01-01 00:01:00')
        db = btrdb.connect(profile='ni4ai')
        run_simulation(start_time, end_time,
                       collection_prefix='simulated/ieee13', fs=30, conn=db)
        assert True
