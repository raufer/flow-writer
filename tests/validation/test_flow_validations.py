import unittest
import os
import shutil

from nose.tools import raises

from pyspark.sql.functions import length, log
from pyspark.sql.types import IntegerType

from flow_writer import node
from flow_writer import Pipeline
from flow_writer import Stage
from flow_writer.validation.exceptions import SignalNotValid

from tests import spark as spark


class TestFlowValidation(unittest.TestCase):

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

    def test_validation_checks_when_valid(self):
        """
        When lifting a function to the data abstraction context, we should be able to add validations that will run immediately before the signal enters that _node.
        The validation mechanism should accept a predicate, or a list of predicates, that the input signal should exhaustively respect.
        """
        data = [
            ("Bob", "17", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "16", 10),
            ("Ali", "15", 25),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        def has_age(df):
            return 'age' in df.schema.names

        @node(val=has_age)
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @node()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        preprocess = Stage("preprocess",
            step_cast_to_int(column="age"),
        )

        transform = Stage("transform",
            step_transform(method='linear')(scale_factor=10),
        )

        pipeline = Pipeline('analyse scores', preprocess, transform)

        df_run = pipeline.run(df)

        self.assertListEqual([r.score for r in df_run.collect()], [s[2]*10 for s in data])

    @raises(SignalNotValid)
    def test_validation_fails_when_invalid(self):
        """
        When lifting a function to the data abstraction context, we should be able to add validations that will run immediately before the signal enters that _node.
        The validation mechanism should accept a predicate, or a list of predicates, that the input signal should exhaustively respect.
        If the signal is not valid a SignalNotValid exception should be raised
        """
        data = [
            ("Bob", "17", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "16", 10),
            ("Ali", "15", 25),
        ]

        df = spark.createDataFrame(data, ["name", "number", "score"])

        def has_age(df):
            return 'age' in df.schema.names

        @node(val=has_age)
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @node()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        preprocess = Stage("preprocess",
           step_cast_to_int(column="age"),
        )

        transform = Stage("transform",
                          step_transform(method='linear')(scale_factor=10),
                          )

        pipeline = Pipeline('analyse scores', preprocess, transform)

        _ = pipeline.run(df)

    def test_validation_checks_when_valid_using_lists(self):
        """
        When lifting a function to the data abstraction context, we should be able to add validations that will run immediately before the signal enters that _node.
        The validation mechanism should accept a predicate, or a list of predicates, that the input signal should exhaustively respect.

        We should be able to pass a list of validations
        """
        data = [
            ("Bob", "17", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "16", 10),
            ("Ali", "15", 25),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        def has_age(df):
            return 'age' in df.schema.names

        def has_name(df):
            return 'name' in df.schema.names

        @node(val=[has_age, has_name])
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @node()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        preprocess = Stage("preprocess",
           step_cast_to_int(column="age"),
        )

        transform = Stage("transform",
          step_transform(method='linear')(scale_factor=10),
          step_just_adults(threshold=18),
        )

        pipeline = Pipeline('analyse scores', preprocess, transform)

        df_run = pipeline.run(df)

        self.assertListEqual([r.score for r in df_run.collect()], [160, 290])

    @raises(SignalNotValid)
    def test_validation_fails_when_invalid_passing_a_list(self):
        """
        When lifting a function to the data abstraction context, we should be able to add validations that will run immediately before the signal enters that _node.
        The validation mechanism should accept a predicate, or a list of predicates, that the input signal should exhaustively respect.
        If the signal is not valid a SignalNotValid exception should be raised
        """
        data = [
            ("Bob", "17", 19),
            ("Sue", "30", 16),
            ("Joe", "17", 20),
            ("Leo", "30", 29),
            ("Jay", "16", 10),
            ("Ali", "15", 25),
        ]

        df = spark.createDataFrame(data, ["name", "age", "score"])

        def has_age(df):
            return 'age' in df.schema.names

        def always_fail(df):
            return False

        @node(val=[has_age, always_fail])
        def step_cast_to_int(df, column):
            return df.withColumn(column, df.age.cast(IntegerType()))

        @node()
        def step_transform(df, method, scale_factor):
            if method == 'log':
                return df.withColumn("score", log(df.score) * scale_factor)
            else:
                return df.withColumn("score", df.score * scale_factor)

        @node()
        def step_just_adults(df, threshold):
            return df.filter(df.age > threshold)

        @node()
        def step_count_name_length(df):
            return df.withColumn("name_length", length(df.name))

        preprocess = Stage("preprocess",
                           step_cast_to_int(column="age"),
                           )

        transform = Stage("transform",
                          step_transform(method='linear')(scale_factor=10),
                          )

        pipeline = Pipeline('analyse scores', preprocess, transform)

        _ = pipeline.run(df)
