import unittest

from pyspark.sql.functions import lit
from flow_writer.abstraction import pipeline_step
from flow_writer.abstraction.stage import Stage
from flow_writer.builders import inject_after, inject_before

from tests import spark as spark


class TestInjectNodeInStage(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_inject_in_stage_after(self):
        """
        'inject_in_stage_after' should insert an additional node after the specified location
        """
        data = [
            ("Allowed", 71, 192),
            ("Blocked", 30, 1274),
            ("DENIED", 70, 192),
            ("Failed", 19, 417),
            ("TCP_AUTH_HIT", None, 192),
            ("TCP_AUTH_MISS", 33, 1411),
            ("FAILED", 101, 718),
            ("ALLOWED", 19, 643)
        ]

        df = spark.createDataFrame(data, ["dvc_action", "url_length", "LengthBytes"])

        @pipeline_step()
        def step_scale_url_length(df, mul_factor=3):
            df = df.withColumn("url_length_scaled", df.url_length * mul_factor)
            return df

        @pipeline_step()
        def step_filter_small_packets(df, column, threshold):
            df = df.filter(df[column] >= threshold)
            return df

        @pipeline_step()
        def step_add_ones(df):
            df = df.withColumn("ones", lit(1))
            return df

        stage = Stage("preprocess",
          step_scale_url_length(mul_factor=3),
          step_filter_small_packets(column="LengthBytes", threshold=0)
        )

        stage_with_additional_step = inject_after(stage, step_add_ones, 'step_scale_url_length')

        self.assertListEqual(
            [r.ones for r in stage_with_additional_step.run(df).collect()],
            [1]*len(data)
        )

        self.assertListEqual(
            [n.name for n in stage_with_additional_step.steps],
            [
                'step_scale_url_length',
                'step_add_ones',
                'step_filter_small_packets'
            ]
        )

    def test_inject_in_stage_before(self):
        """
        'inject_in_stage_before' should insert an additional node before the specified location
        """
        data = [
            ("Allowed", 71, 192),
            ("Blocked", 30, 1274),
            ("DENIED", 70, 192),
            ("Failed", 19, 417),
            ("TCP_AUTH_HIT", None, 192),
            ("TCP_AUTH_MISS", 33, 1411),
            ("FAILED", 101, 718),
            ("ALLOWED", 19, 643)
        ]

        df = spark.createDataFrame(data, ["dvc_action", "url_length", "LengthBytes"])

        @pipeline_step()
        def step_scale_url_length(df, mul_factor=3):
            df = df.withColumn("url_length_scaled", df.url_length * mul_factor)
            return df

        @pipeline_step()
        def step_filter_small_packets(df, column, threshold):
            df = df.filter(df[column] >= threshold)
            return df

        @pipeline_step()
        def step_add_ones(df):
            df = df.withColumn("ones", lit(1))
            return df

        stage = Stage("preprocess",
              step_scale_url_length(mul_factor=3),
              step_filter_small_packets(column="LengthBytes", threshold=0)
        )

        stage_with_additional_step = inject_before(stage, step_add_ones, 'step_scale_url_length')

        self.assertListEqual(
            [r.ones for r in stage_with_additional_step.run(df).collect()],
            [1]*len(data)
        )

        self.assertListEqual(
            [n.name for n in stage_with_additional_step.steps],
            [
                'step_add_ones',
                'step_scale_url_length',
                'step_filter_small_packets'
            ]
        )

        self.assertListEqual(
            [(r.ones, r.url_length) for r in stage_with_additional_step.run_step(df, 'step_add_ones').collect()],
            list(zip([1]*len(data), [r[1] for r in data]))
        )

    def test_whole_information_is_propagated(self):
        """
        'inject_in_pipeline_before/after' should propagate all of the fields that were present in the base stage
        """
        data = [
            ("Allowed", 71, 192),
            ("Blocked", 30, 1274),
            ("DENIED", 70, 192),
            ("Failed", 19, 417),
            ("TCP_AUTH_HIT", None, 192),
            ("TCP_AUTH_MISS", 33, 1411),
            ("FAILED", 101, 718),
            ("ALLOWED", 19, 643)
        ]

        df = spark.createDataFrame(data, ["dvc_action", "url_length", "LengthBytes"])

        @pipeline_step()
        def step_scale_url_length(df, mul_factor=3):
            df = df.withColumn("url_length_scaled", df.url_length * mul_factor)
            return df

        @pipeline_step()
        def step_filter_small_packets(df, column, threshold):
            df = df.filter(df[column] >= threshold)
            return df

        @pipeline_step()
        def step_add_ones(df):
            df = df.withColumn("ones", lit(1))
            return df

        stage = Stage("preprocess",
                      step_scale_url_length(mul_factor=3),
                      step_filter_small_packets(column="LengthBytes", threshold=0)
                      )

        stage = stage.with_description('DOC')

        stage_ = inject_before(stage, step_add_ones, 'step_scale_url_length')

        self.assertEqual(stage.description, stage_.description)

