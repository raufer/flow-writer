import unittest
import os
import shutil

from pyspark.sql.functions import length, log
from pyspark.sql.types import IntegerType

from flow_writer.abstraction import pipeline_step
from flow_writer.abstraction.pipeline import Pipeline
from flow_writer.abstraction.stage import Stage

from tests import spark as spark


@pipeline_step()
def write_df(df, path, format, params=None):
    if not params:
        params = {
            'header': 'true',
            'inferSchema': 'true'
        }

    df.write.format(format).save(path, **params)
    return df


@pipeline_step()
def read_df(spark, path, format, params=None):
    if not params:
        params = {
            'header': 'true',
            'inferSchema': 'true'
        }

    df = spark.read.format(format).load(path, **params)
    return df


class TestFlowSideEffects(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.temp_path = 'tests/.temp/'

    @classmethod
    def teardown_class(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

    def test_add_side_effects_to_stage_after(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the stage level, with the 'after' operator
        This separates concerns and leaves the core of a given workflow, ie the transformations, with reduced noise

        We should be able to perform the side effect after a specific step, by name, or by position
        """
        data = [
            ("Bob", "25", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "33", 10),
            ("Ali", "26", 25),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        @pipeline_step()
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @pipeline_step()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        transform = Stage("transform",
                          step_cast_to_int(column="age"),
                          step_transform(method='linear')(scale_factor=10),
                          step_just_adults(threshold=18)
                          )

        df_transform = transform.run(df)

        self.assertListEqual([r.score for r in df_transform.collect()], [s[2] * 10 for s in data if int(s[1]) > 18])

        stage_with_side_effect = transform.with_side_effect(
            write_df(path=self.temp_path + 'NAME', format='csv')).after(
            'step_transform')

        _ = stage_with_side_effect.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'NAME', 'csv')

        self.assertListEqual(
            sorted([r.score for r in df_check.collect()]),
            sorted([s[2] * 10 for s in data])
        )

        stage_with_another_se = transform.with_side_effect(
            write_df(path=self.temp_path + 'ANOTHER_NAME', format='csv')).after('step_just_adults')
        _ = stage_with_another_se.run(df)

        df_check = read_df(spark, self.temp_path + 'ANOTHER_NAME', 'csv')
        self.assertListEqual(
            sorted([r.score for r in df_check.collect()]),
            sorted([s[2] * 10 for s in data if int(s[1]) > 18])
        )

    def test_add_side_effects_to_stage_before(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the stage level, with the 'before' operator
        This separates concerns and leaves the core of a given workflow, ie the transformations, with reduced noise

        We should be able to perform the side effect after a specific step, by name, or by position
        """
        data = [
            ("Bob", "25", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "33", 10),
            ("Ali", "26", 25),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        @pipeline_step()
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @pipeline_step()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        transform = Stage("transform",
                          step_cast_to_int(column="age"),
                          step_transform(method='linear')(scale_factor=10),
                          step_just_adults(threshold=18)
                          )

        df_transform = transform.run(df)

        self.assertListEqual([r.score for r in df_transform.collect()], [s[2] * 10 for s in data if int(s[1]) > 18])

        stage_with_side_effect = transform.with_side_effect(
            write_df(path=self.temp_path + 'NAME', format='csv')).before('step_transform')

        _ = stage_with_side_effect.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'NAME', 'csv')

        self.assertListEqual(
            sorted([r.score for r in df_check.collect()]),
            sorted([s[2] for s in data])
        )

        stage_with_another_se = transform.with_side_effect(
            write_df(path=self.temp_path + 'ANOTHER_NAME', format='csv')).before('step_just_adults')
        _ = stage_with_another_se.run(df)

        df_check = read_df(spark, self.temp_path + 'ANOTHER_NAME', 'csv')
        self.assertListEqual(
            sorted([r.score for r in df_check.collect()]),
            sorted([s[2] * 10 for s in data])
        )

    def test_add_side_effects_pipeline_level(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the pipeline level also
        The caller can control the place where the side effect occurs
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_name = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'NAME', format='csv')).after('transform/step_just_employed')

        _ = pipeline_se_name.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'NAME', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )

        pipeline_with_another_se = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'ANOTHER_NAME', format='csv')).after('postprocess/step_rename')
        _ = pipeline_with_another_se.run(df)

        df_check = read_df(spark, self.temp_path + 'ANOTHER_NAME', 'csv')
        self.assertListEqual(df_check.schema.names, ["name", "age", "is_employed"])

    def test_add_side_effects_pipeline_level_before(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the pipeline level also
        The caller can control the place where the side effect occurs
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_name = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'NAME', format='csv')).before('postprocess/step_rename')

        _ = pipeline_se_name.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'NAME', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )

        stage_with_another_se = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'ANOTHER_NAME', format='csv')).after('postprocess/step_rename')
        _ = stage_with_another_se.run(df)

        df_check = read_df(spark, self.temp_path + 'ANOTHER_NAME', 'csv')
        self.assertListEqual(df_check.schema.names, ["name", "age", "is_employed"])

    def test_add_side_effects_pipeline_level_after_step_selector(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the pipeline level also
        The caller can control the place where the side effect occurs
        We should be able to select the step using a dict style of directory style
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_dir = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'AD', format='csv')).after('transform/step_just_adults')

        pipeline_se_dict = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'EM', format='csv')).after('transform/step_just_employed')

        _ = pipeline_se_dir.run(df).collect()
        _ = pipeline_se_dict.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'AD', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18])
        )

        df_check = read_df(spark, self.temp_path + 'EM', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )

    def test_add_side_effects_pipeline_level_before_step_selector(self):
        """
        We should be able to keep track of all of the 'side effects' involved on a workflow pipeline, at the pipeline level also
        The caller can control the place where the side effect occurs
        We should be able to select the step using a directory naming style
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_dir = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'AD', format='csv')).before('transform/step_just_employed')

        pipeline_se_dict = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'EM', format='csv')).before('postprocess/step_rename')

        _ = pipeline_se_dir.run(df).collect()
        _ = pipeline_se_dict.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'AD', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18])
        )

        df_check = read_df(spark, self.temp_path + 'EM', 'csv')

        self.assertListEqual(
            sorted([r.age for r in df_check.collect()]),
            sorted([int(s[1]) for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )

    def test_add_side_effects_with_parameters(self):
        """
        The side effects operations should accept dictionaries that parameterize the behaviour of the operations
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_dir = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'AD', format='csv', params={'header': False, 'separator': ';'})).before(
            'transform/step_just_employed')

        pipeline_se_dict = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'EM', format='csv', params={'header': False, 'separator': ';'})).before(
            'postprocess/step_rename')

        _ = pipeline_se_dir.run(df).collect()
        _ = pipeline_se_dict.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'AD', 'csv', params={'header': False, 'separator': ';'})

        self.assertListEqual(
            sorted([r[0] for r in df_check.collect()]),
            sorted([s[0] for s in data if int(s[1]) > 18])
        )

        df_check = read_df(spark, self.temp_path + 'EM', 'csv', params={'header': False, 'separator': ';'})

        self.assertListEqual(
            sorted([r[0] for r in df_check.collect()]),
            sorted([s[0] for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )

    def test_add_side_effects_with_parameters_after(self):
        """
        The side effects operations should accept dictionaries that parameterize the behaviour of the operations
        with the 'after' variation
        """
        data = [
            ("Bob", "17", 1),
            ("Sue", "30", 0),
            ("Joe", "17", 0),
            ("Leo", "30", 1),
            ("Jay", "33", 1),
            ("Ali", "26", 1),
        ]

        df = spark.createDataFrame(data, ["name", "age", "employed"])

        @pipeline_step()
        def step_stringify(df):
            return df.withColumn("age", df.age.cast(IntegerType()))

        @pipeline_step()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @pipeline_step()
        def step_just_employed(df):
            return df.filter(df.employed > 0)

        @pipeline_step()
        def step_rename(df):
            return df.withColumnRenamed('employed', 'is_employed')

        preprocess = Stage("preprocess", step_stringify)

        transform = Stage("transform",
                          step_just_adults(threshold=18),
                          step_just_employed
                          )

        postprocess = Stage("postprocess", step_rename)

        pipeline = Pipeline("Pipeline A",
                            preprocess,
                            transform,
                            postprocess
                            )

        pipeline_se_dir = pipeline.with_side_effect(
            write_df(path=self.temp_path + 'AD', format='csv', params={'header': False, 'separator': ';'})).after(
            'transform/step_just_employed')

        _ = pipeline_se_dir.run(df).collect()

        df_check = read_df(spark, self.temp_path + 'AD', 'csv', params={'header': False, 'separator': ';'})

        self.assertListEqual(
            sorted([r[0] for r in df_check.collect()]),
            sorted([s[0] for s in data if int(s[1]) > 18 and int(s[2]) == 1])
        )
