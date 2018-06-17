import unittest

from flow_writer.abstraction import pipeline_step
from flow_writer.dependency_manager.utils import unfold_step_dependencies, unfold_steps_dependencies, update_dependencies


class TestDependencyManagerUtils(unittest.TestCase):

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

    def test_unfold_step_dependencies(self):
        """
        'unfold_steps_dependencies(step)' should expand the dependencies registered within a step into a
        flat list containing a granular entry per line
        """

        #  no dependencies
        @pipeline_step()
        def step_simple(df, tokenizer):
            pass

        self.assertListEqual(unfold_step_dependencies(step_simple), [])

        #  single dependency
        def loader(path):
            pass

        def writer(path, tokenizer):
            pass

        @pipeline_step(dm={"tokenizer": [loader, writer]})
        def step_tokenize(df, tokenizer):
            pass

        result = unfold_step_dependencies(step_tokenize)

        dependencies = step_tokenize.registry.get("dependencies", list())

        loader = dependencies[0].source
        writer = dependencies[0].sink

        expected = [
            (step_tokenize, "tokenizer", loader, "path"),
            (step_tokenize, "tokenizer", writer, "path"),
            (step_tokenize, "tokenizer", writer, "tokenizer"),
        ]

        self.assertListEqual(result, expected)

        #  multiple dependency
        def loader(path):
            pass

        def writer(path, tokenizer):
            pass

        def loader2(path):
            pass

        def writer2(path, normalizer):
            pass

        @pipeline_step(dm={"tokenizer": [loader, writer], "normalizer": [loader2, writer2]})
        def step_multiple_dependencies(df, tokenizer, normalizer):
            pass

        result = unfold_step_dependencies(step_multiple_dependencies)

        dependencies = step_multiple_dependencies.registry.get("dependencies", list())

        loader = dependencies[0].source
        writer = dependencies[0].sink
        loader2 = dependencies[1].source
        writer2 = dependencies[1].sink

        expected = [
            (step_multiple_dependencies, "tokenizer", loader, "path"),
            (step_multiple_dependencies, "tokenizer", writer, "path"),
            (step_multiple_dependencies, "tokenizer", writer, "tokenizer"),
            (step_multiple_dependencies, "normalizer", loader2, "path"),
            (step_multiple_dependencies, "normalizer", writer2, "path"),
            (step_multiple_dependencies, "normalizer", writer2, "normalizer"),
        ]

        self.assertListEqual(result, expected)

    def test_unfold_step_dependencies_just_source(self):
        """
        'unfold_steps_dependencies(step, flags*)'  should just consider specific parts of the entries
        """

        #  no dependencies
        @pipeline_step()
        def step_simple(df, tokenizer):
            pass

        self.assertListEqual(unfold_step_dependencies(step_simple), [])

        #  single dependency
        def loader(path):
            pass

        def writer(path, tokenizer):
            pass

        @pipeline_step(dm={"tokenizer": [loader, writer]})
        def step_tokenize(df, tokenizer):
            pass

        result = unfold_step_dependencies(step_tokenize, include_sink=True, include_source=False)

        dependencies = step_tokenize.registry.get("dependencies", list())

        writer = dependencies[0].sink

        expected = [
            (step_tokenize, "tokenizer", writer, "path"),
            (step_tokenize, "tokenizer", writer, "tokenizer")
        ]

        self.assertListEqual(result, expected)

        #  multiple dependency
        def loader(path):
            pass

        def writer(path, tokenizer):
            pass

        def loader2(path):
            pass

        def writer2(path, normalizer):
            pass

        @pipeline_step(dm={"tokenizer": [loader, writer], "normalizer": [loader2, writer2]})
        def step_multiple_dependencies(df, tokenizer, normalizer):
            pass

        result = unfold_step_dependencies(step_multiple_dependencies, include_source=True, include_sink=False)

        dependencies = step_multiple_dependencies.registry.get("dependencies", list())

        loader = dependencies[0].source
        loader2 = dependencies[1].source

        expected = [
            (step_multiple_dependencies, "tokenizer", loader, "path"),
            (step_multiple_dependencies, "normalizer", loader2, "path")
        ]

        self.assertListEqual(result, expected)

    def test_unfold_steps_dependencies(self):
        """
        'unfold_steps_dependencies(step)' should concatenate the expansion of every step in 'steps'
        """

        def loader(path):
            pass

        def writer(path, tokenizer):
            pass

        @pipeline_step(dm={"tokenizer": [loader, writer]})
        def step_one(df, tokenizer):
            pass

        def loader2(path):
            pass

        def writer2(path, normalizer):
            pass

        @pipeline_step(dm={"normalizer": [loader2, writer2]})
        def step_two(df, normalizer):
            pass

        result = unfold_steps_dependencies([step_one, step_two])

        dependencies_one = step_one.registry.get("dependencies", list())
        dependencies_two = step_two.registry.get("dependencies", list())

        loader = dependencies_one[0].source
        writer = dependencies_one[0].sink
        loader2 = dependencies_two[0].source
        writer2 = dependencies_two[0].sink

        expected = [
            (step_one, "tokenizer", loader, "path"),
            (step_one, "tokenizer", writer, "path"),
            (step_one, "tokenizer", writer, "tokenizer"),
            (step_two, "normalizer", loader2, "path"),
            (step_two, "normalizer", writer2, "path"),
            (step_two, "normalizer", writer2, "normalizer"),
        ]

        self.assertListEqual(result, expected)

    def test_dependencies_partial_updates(self):
        """
        'update_dependencies(new, old)' update the old dependencies with the values present in the new ones
        should be done in a immutable way
        """

        def f(a, b):
            return a + b

        def g(a, b):
            return a * b

        d1 = {
            'tokenizer': [f, g],
            'normalizer': [f, g]
        }

        def h(x):
            return x

        res = update_dependencies(d1, {'tokenizer': h}, 'sink')
        self.assertEqual(res['tokenizer'][0](1, 1), 2)
        self.assertEqual(res['tokenizer'][1](0), 0)
        self.assertEqual(res['normalizer'][0](1, 1), 2)
        self.assertEqual(res['normalizer'][1](1, 1), 1)

        res = update_dependencies(d1, {'tokenizer': h}, 'source')
        self.assertEqual(res['tokenizer'][0](0), 0)
        self.assertEqual(res['tokenizer'][1](1, 1), 1)
        self.assertEqual(res['normalizer'][0](1, 1), 2)
        self.assertEqual(res['normalizer'][1](1, 1), 1)

        res = update_dependencies(d1, {'tokenizer': h, 'normalizer': h}, 'sink')
        self.assertEqual(res['tokenizer'][0](1, 1), 2)
        self.assertEqual(res['tokenizer'][1](0), 0)
        self.assertEqual(res['normalizer'][0](1, 1), 2)
        self.assertEqual(res['normalizer'][1](0), 0)
