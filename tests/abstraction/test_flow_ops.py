import unittest

from nose.tools import raises

from flow_writer.ops.function_ops import cur, curr, closed_bindings, get_closed_variables
from flow_writer.abstraction import pipeline_step


class TestFlowOps(unittest.TestCase):

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

    def test_curried_function_generator_decorated_version(self):
        """
        To improve usability we should only use a simple decorator to indicate a pipeline step and hide all of the complexity away
        It should trigger as soon as all the elements are available.
        """

        @pipeline_step()
        def power_of(base, exponent, add_at_end=0):
            return base ** exponent + add_at_end

        power_of_two = power_of(exponent=2)

        self.assertEqual(power_of_two(2), 4)

        self.assertEqual(power_of(exponent=2, add_at_end=1)(2), 5)
        self.assertEqual(power_of(exponent=2)(add_at_end=1)(2), 5)

    def test_curried_process_is_not_destructive(self):
        """
        When currying a function, we should take care of possible destructive effects that the currying process might
        apply to the callable structure such as '__doc__' or '__name__'
        """

        @pipeline_step()
        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = power_of(exponent=2)

        self.assertEqual(power_of_two.__name__, power_of.__name__)
        self.assertEqual(power_of_two.__doc__, power_of.__doc__)

    @raises(ValueError)
    def test_rebinding_arguments_not_allowed(self):
        """
        If arguments rebinding is not support an exception should be thrown when kws are repeated
        """

        @pipeline_step(allow_rebinding=False)
        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = power_of(exponent=2)

        power_of_two(exponent=3)
