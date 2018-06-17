import unittest

from pyspark.sql.functions import lit
from flow_writer.abstraction import pipeline_step
from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage
from flow_writer.builders import inject_after, inject_before

from tests import spark as spark


class TestInjectNodeInPipeline(unittest.TestCase):

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

    def test_inject_in_pipeline_after(self):
        """
        'inject_in_pipeline_after' should insert an additional node after the specified location
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = spark.createDataFrame(data, ["name", "age_str", "employed"])

        @pipeline_step()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @pipeline_step()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @pipeline_step()
        def step_twoify(df):
            return df.withColumn("employed", lit(2))

        @pipeline_step()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
            stage_map_zeros,
            stage_map_ones_and_twos,
            stage_map_threes
        )

        @pipeline_step()
        def step_onehundred(df):
            return df.withColumn("employed", lit(100))

        # pipeline_ = inject_after(pipeline, step_onehundred, 'zeros/step_zerify')
        #
        # self.assertListEqual(
        #     [r.employed for r in pipeline_.run_stage('zeros', df).collect()],
        #     [100]*len(data)
        # )

        pipeline_ = inject_after(pipeline, step_onehundred, 'ones and twos/step_twoify')

        self.assertListEqual(
            [r.employed for r in pipeline_.run_n_steps(df, 4).collect()],
            [100]*len(data)
        )

    def test_inject_in_pipeline_before(self):
        """
        'inject_in_pipeline_before' should insert an additional node before the specified location
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = spark.createDataFrame(data, ["name", "age_str", "employed"])

        @pipeline_step()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @pipeline_step()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @pipeline_step()
        def step_twoify(df):
            return df.withColumn("employed", lit(2))

        @pipeline_step()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
                            stage_map_zeros,
                            stage_map_ones_and_twos,
                            stage_map_threes
                            )

        @pipeline_step()
        def step_onehundred(df):
            return df.withColumn("employed", lit(100))

        pipeline_ = inject_before(pipeline, step_onehundred, 'zeros/step_zerify')

        self.assertListEqual(
            [r.employed for r in pipeline_.run_stage('zeros', df).collect()],
            [0]*len(data)
        )

        self.assertListEqual(
            [r.employed for r in pipeline_.run_n_steps(df, 1).collect()],
            [100]*len(data)
        )

    def test_whole_information_is_propagated(self):
        """
        'inject_in_pipeline_before/after' should propagate all of the fields that were present in the base pipeline
        """
        data = [
            ("Bob", "25", 2),
            ("Sue", "30", 2),
            ("Joe", "17", 2),
            ("Leo", "30", 2),
            ("Jay", "33", 2),
            ("Ali", "26", 2),
        ]

        df = spark.createDataFrame(data, ["name", "age_str", "employed"])

        @pipeline_step()
        def step_zerify(df):
            return df.withColumn("employed", lit(0))

        @pipeline_step()
        def step_onify(df):
            return df.withColumn("employed", lit(1))

        @pipeline_step()
        def step_twoify(df):
            return df.withColumn("employed", lit(2))

        @pipeline_step()
        def step_threeify(df):
            return df.withColumn("employed", lit(3))

        stage_map_zeros = Stage("zeros", step_zerify)
        stage_map_ones_and_twos = Stage("ones and twos", step_onify, step_twoify)
        stage_map_threes = Stage("three", step_threeify)

        pipeline = Pipeline("workflow",
                            stage_map_zeros,
                            stage_map_ones_and_twos,
                            stage_map_threes
                            )

        pipeline = pipeline.with_description('DOC')

        @pipeline_step()
        def step_onehundred(df):
            return df.withColumn("employed", lit(100))

        pipeline_ = inject_before(pipeline, step_onehundred, 'zeros/step_zerify')

        self.assertEqual(pipeline.description, pipeline_.description)

