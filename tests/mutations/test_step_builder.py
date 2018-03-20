import unittest

from pyspark.sql.functions import length, log, lit

from flow_writer import node
from flow_writer import Pipeline
from flow_writer import Stage

from tests import spark as spark


class TestFlowSideEffects(unittest.TestCase):

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

    def test_step_builder(self):
        """
        We should be able to derive new pipelines by adding additional steps at certain locations
        """
        data = [
            ("Bob", "25", 0),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 0),
            ("Jay", "33", 0),
            ("Ali", "26", 0),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        @node()
        def step_one(df):
            return df.withColumn('score', df.score + 1)

        @node()
        def step_two(df):
            return df.withColumn('score', df.score + 1)

        @node()
        def step_three(df):
            return df.withColumn('score', df.score + 1)

        @node()
        def step_four(df):
            return df.withColumn('score', df.score + 1)

        stage_one = Stage("stage one",
            step_one,
            step_two
        )

        stage_two = Stage("stage two",
            step_three
        )

        stage_three = Stage("stage three",
            step_four
        )

        pipeline = Pipeline("Pipeline", stage_one, stage_two, stage_three)

        df_run = pipeline.run(df)
        self.assertEqual([r.score for r in df_run.collect()], [4]*len(data))

        @node()
        def step_five(df):
            return df.withColumn('score', df.score + 100)

        pipeline_ = pipeline.with_step(step_five).after('stage two/step_three')

        df_run = pipeline_.run_stage('stage two', df)
        self.assertEqual(df_run.first().score, 101)

        pipeline_ = pipeline.with_step(step_five).before('stage three/step_four')
        df_run = pipeline_.run_stage('stage two', df)
        self.assertEqual(df_run.first().score, 1)
        df_run = pipeline_.run_stage('stage three', df)
        self.assertEqual(df_run.first().score, 101)

        @node()
        def step_nullify(df):
            return df.withColumn('score', lit(0))

        pipeline_2 = pipeline_.with_step(step_nullify).after('stage one/step_two')
        df_run = pipeline_2.run_stage('stage one', df)
        self.assertEqual(df_run.first().score, 0)
        df_run = pipeline_2.run(df)
        self.assertEqual(df_run.first().score, 102)
