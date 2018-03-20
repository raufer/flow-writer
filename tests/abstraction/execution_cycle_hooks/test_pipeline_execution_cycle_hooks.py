import unittest
import pyspark.sql.types as T

from flow_writer import node
from flow_writer import Pipeline
from tests import spark as spark
from flow_writer import Stage


class TestPipelineExecCycle(unittest.TestCase):

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

    def test_after_each_step(self):
        """
        'pipeline.after_each_step(f)' should invoke some behaviour 'f' after each step
        That callable should be a function that receives a signal and does something with it
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage_one = Stage("first stage",
          step_just_adults(threshold=18),
          step_stringify,
        )

        stage_two = Stage("second stage", step_rename)
        pipeline = Pipeline("pipeline", stage_one, stage_two)

        global mutable_state
        mutable_state = 0

        def f(df, step):
            global mutable_state
            mutable_state += 1
            return mutable_state

        pipeline = pipeline.after_each_step(f)
        _ = pipeline.run(df)
        self.assertEqual(mutable_state, pipeline.get_num_steps())

        gen = pipeline.run_iter_stages(df)
        _ = next(gen)
        self.assertEqual(mutable_state, 5)
        _ = next(gen)
        self.assertEqual(mutable_state, 6)

    def test_after_each_step_inject_obj(self):
        """
        'pipeline.after_each_step(f)' should make sure that the step object is also injected if requested by 'f'
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage_one = Stage("first stage",
          step_just_adults(threshold=18),
          step_stringify,
        )

        stage_two = Stage("second stage", step_rename)
        pipeline = Pipeline("pipeline", stage_one, stage_two)

        global mutable_state
        mutable_state = []

        def f(df, step):
            global mutable_state
            mutable_state.append(step.name)

        pipeline = pipeline.after_each_step(f)

        _ = pipeline.run(df)

        self.assertListEqual(
            mutable_state,
            [
                'step_just_adults',
                'step_stringify',
                'step_rename'
            ]
        )

    def test_before_each_step(self):
        """
        'pipeline.before_each_step(f)' should invoke some behaviour 'f' immediately before calling each step
        That callable should be a function that receives a signal and does something with it
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage_one = Stage("first stage",
          step_just_adults(threshold=18),
          step_stringify,
        )

        stage_two = Stage("second stage", step_rename)

        pipeline = Pipeline("pipeline", stage_one, stage_two)

        global mutable_state
        mutable_state = []

        def f(df, step):
            global mutable_state
            mutable_state.append(df.count())

        pipeline_before = pipeline.before_each_step(f)
        gen = pipeline_before.run_iter_stages(df)
        _ = next(gen)
        self.assertListEqual(mutable_state, [4, 3])

        pipeline_after = pipeline.after_each_step(f)
        gen = pipeline_after.run_iter_stages(df)
        _ = next(gen)
        self.assertListEqual(mutable_state, [4, 3, 3, 3])

    def test_after_each_stage(self):
        """
        'pipeline.after_each_stage(f)' should run a function 'f' immediately after each completion of a stage
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage_one = Stage("first stage",
                          step_just_adults(threshold=18),
                          step_stringify,
                          )

        stage_two = Stage("second stage", step_rename)
        pipeline = Pipeline("pipeline", stage_one, stage_two)

        global mutable_state
        mutable_state = []

        def f(df, stage):
            global mutable_state
            mutable_state.append(stage.name)

        pipeline = pipeline.after_each_stage(f)

        _ = pipeline.run(df)

        self.assertListEqual(
            mutable_state,
            [
                'first stage',
                'second stage'
            ]
        )

    def test_before_each_stage(self):
        """
        'pipeline.before_each_stage(f)' should run a function 'f' immediately before the execution of each stage
        """
        data = [
            ("Bob", 25, 1),
            ("Sue", 30, 0),
            ("Joe", 17, 0),
            ("Leo", 30, 1)
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_stringify(df):
            return df.withColumn("age_str", df.age.cast(T.StringType()))

        @node()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        stage_one = Stage("first stage",
                          step_just_adults(threshold=18),
                          step_stringify,
                          )

        stage_two = Stage("second stage", step_rename)
        pipeline = Pipeline("pipeline", stage_one, stage_two)

        global mutable_state
        mutable_state = []

        def f(df, stage):
            global mutable_state
            mutable_state.append(df.count())

        pipeline_before = pipeline.before_each_stage(f)
        gen = pipeline_before.run_iter_stages(df)
        _ = next(gen)
        self.assertListEqual(mutable_state, [4])

        # mutable_state = []
        # pipeline_after = pipeline.after_each_stage(f)
        # gen = pipeline_after.run_iter(df)
        # _ = next(gen)
        # self.assertListEqual(mutable_state, [3])
