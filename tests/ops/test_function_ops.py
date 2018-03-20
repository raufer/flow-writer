import unittest

from nose.tools import raises

from flow_writer.ops.function_ops import default_args, get_closed_variables, closed_bindings, lazy, curry


class TestFunctionOps(unittest.TestCase):

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

    def test_curried_function_generator(self):
        """
        Given any function with a variable number of arguments, we should be able to generate a curried version,
        by passing the function through a wrapper.

        We should be can bind arguments in any order we like and how many arguments we feel like at once.
        """

        def power_of(base, exponent, add_at_end=0):
            return base ** exponent + add_at_end

        power_of_two = curry(power_of)(exponent=2)

        self.assertEqual(power_of_two(2), 4)

        self.assertEqual(curry(power_of)(exponent=2, add_at_end=1)(2), 5)
        self.assertEqual(curry(power_of)(exponent=2)(add_at_end=1)(2), 5)

    def test_curried_process_is_not_destructive(self):
        """
        When currying a function, we should take care of possible destructive effects that the currying process might
        apply to the callable structure such as '__doc__' or '__name__'
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curry(power_of)(exponent=2)

        self.assertEqual(power_of_two.__name__, power_of.__name__)
        self.assertEqual(power_of_two.__doc__, power_of.__doc__)

    def test_rebinding_arguments_allowed(self):
        """
        There should be a currying process allowing for the rebinding of arguments
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curry(power_of)(exponent=2)

        power_of_three = power_of_two(exponent=3)

        self.assertEqual(power_of_three(2), 8)

    def test_get_default_args(self):
        """
        The default arguments should be valid for introspection even if a function is curried
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curry(power_of)(exponent=2)

        self.assertEqual(default_args(power_of), {'add_at_end': 0})
        self.assertEqual(default_args(power_of), default_args(power_of_two))

    def test_get_closed_vars(self):
        """
        'get_closed_vars' should extract all of the current binded variables to the callable closure
        """

        def f(x, y, z, w):
            return x + y + z + w

        closurespace = 'kwargs'

        g = curry(f)(x=2)
        self.assertDictEqual(get_closed_variables(g)[closurespace], {'x': 2})

        h = g(y=3, z=4)
        self.assertDictEqual(get_closed_variables(h)[closurespace], {'x': 2, 'y': 3, 'z': 4})

    def test_get_closure_bindings(self):
        """
        'closure_bindings' should return all of currently bound variables by the user.
        Additionally default arguments of the original function should also be retrieved
        """

        def f(df, y, z, w=10):
            return df + y + z + w

        f = curry(f)

        g = f(z=9)
        self.assertDictEqual(closed_bindings(g), {'z': 9, 'w': 10})

        h = g(z=100, w=100)
        self.assertDictEqual(closed_bindings(h), {'z': 100, 'w': 100})

        i = h(y=2)
        self.assertDictEqual(closed_bindings(i), {'y': 2, 'z': 100, 'w': 100})

        g = f(1, 2, 3)
        self.assertEqual(g, 16)

        g = f(1, 2, w=5)
        self.assertDictEqual(closed_bindings(g), {'df': 1, 'y': 2, 'w': 5})

        g = f(1, z=2, w=5)
        self.assertDictEqual(closed_bindings(g), {'df': 1, 'z': 2, 'w': 5})

        g = f(y=1, z=2, w=5)(5)
        self.assertEqual(g, 13)

        g = f(y=1, w=5)(df=1, z=5)
        self.assertEqual(g, 12)

        g = f(4, y=1, z=5)
        self.assertEqual(g, 20)

        g = f(y=1, w=5)(z=5)
        self.assertDictEqual(closed_bindings(g), {'y': 1, 'z': 5, 'w': 5})

        g = f(y=1)(z=5)(2)
        self.assertEqual(g, 18)

        g = f(y=1)(z=5)(w=2)
        self.assertDictEqual(closed_bindings(g), {'y': 1, 'z': 5, 'w': 2})

        g = f(1)(5)(2)
        self.assertEqual(g, 18)

        g = f(y=1)(z=5)(w=2)(w=10)(z=10)(y=10)(10)
        self.assertEqual(g, 40)

        g = f(y=1)(1, y=2, z=1, w=1)
        self.assertEqual(g, 5)

        g = f(1)
        self.assertEqual(g(100, y=2, z=1, w=1), 5)

        g = f(1)
        self.assertEqual(g(df=100, y=2, z=1, w=1), 104)

        g = f(y=1)(1)(5, 2, z=1, w=1)
        self.assertEqual(g, 4)

    def test_original_function_traditional_call(self):
        """
        Pipeline step decorator should not break the function's normal behaviour
        """

        def f(x, y, z, w=10):
            return x + y + z + w

        f = curry(f)

        # self.assertEqual(f(1, 2, 3), 16)
        # self.assertEqual(f(1, 2, 3, 4), 10)
        self.assertDictEqual(closed_bindings(f(1, 2)), {'x': 1, 'y': 2, 'w': 10})

    def test_lazy_call(self):
        """
        '@lazy' should defer the evaluation of 'func', returning a partial application of function with the arguments of its first call
        """

        @lazy
        def f(a, b, c):
            return a + b + c

        self.assertEqual(f(1, 2, 3)(), 6)

        @lazy
        def f(a, b, c):
            return a + b + c

        self.assertEqual(f(1)(2, 3), 6)
