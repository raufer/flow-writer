import unittest

from nose.tools import raises

from flow_writer.ops.exceptions import PositionalRebindNotAllowed
from flow_writer.ops.function_ops import cur, curr, default_args, get_closed_variables, closed_bindings, lazy, copy_func


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

        power_of_two = curr(power_of)(exponent=2)

        self.assertEqual(power_of_two(2), 4)

        self.assertEqual(curr(power_of)(exponent=2, add_at_end=1)(2), 5)
        self.assertEqual(curr(power_of)(exponent=2)(add_at_end=1)(2), 5)

    def test_curried_process_is_not_destructive(self):
        """
        When currying a function, we should take care of possible destructive effects that the currying process might
        apply to the callable structure such as '__doc__' or '__name__'
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curr(power_of)(exponent=2)

        self.assertEqual(power_of_two.__name__, power_of.__name__)
        self.assertEqual(power_of_two.__doc__, power_of.__doc__)

    def test_curried_function_generator_cur_version_base_function(self):
        """
        Additionally we should be able to generate a curried version of the function where the execution is controlled by the caller.
        The execution should just be triggered when an explicit call with no arguments is made.
        """
        def f():
            return 'f'

        g = cur(f)

        self.assertEqual(g(), 'f')

    def test_curried_function_generator_cur_version(self):
        """
        Additionally we should be able to generate a curried version of the function where the execution is controlled by the caller.
        The execution should just be triggered when an explicit call with no arguments is made.
        """

        def power_of(base, exponent, add_at_end=0):
            return base ** exponent + add_at_end

        power_of_two = cur(power_of)(exponent=2)

        self.assertEqual(power_of_two(2)(), 4)

        self.assertEqual(cur(power_of)(exponent=2, add_at_end=1)(2)(), 5)
        self.assertEqual(cur(power_of)(exponent=2)(add_at_end=1)(2)(), 5)

    def test_curried_function_generator_cur_version_rebinding_by_keyword(self):
        """
        The curried version with on demand execution should just allows a rebinding operation by keyword argument.
        """

        def double(x):
            return x * 2

        self.assertEqual(cur(double)(2)(), 4)
        self.assertEqual(cur(double)(2)(x=4)(), 8)

        def sum(x, y, z):
            return x + y + z

        self.assertEqual(cur(sum)(1)(1)(1)(), 3)
        self.assertEqual(cur(sum)(1)(1, 1)(), 3)
        self.assertEqual(cur(sum)(1)(1, 1)(z=2, y=2)(), 5)

    @raises(PositionalRebindNotAllowed)
    def test_curried_function_generator_cur_version_rebinding_by_position(self):
        """
        Since the execution of the function must be done explicitly, attempts to rebind arguments by position
        should be ignored and raise an error
        """

        def double(x):
            return x * 2

        self.assertEqual(cur(double)(2)(4)(), 8)

    @raises(PositionalRebindNotAllowed)
    def test_curried_function_generator_cur_version_rebinding_by_position(self):
        """
        Since the execution of the function must be done explicitly, attempts to rebind arguments by position
        should be ignored and raise an error
        """

        def sum(x, y, z):
            return x + y + z

        self.assertEqual(cur(sum)(1)(1)(1)(), 3)
        self.assertEqual(cur(sum)(1)(1, 1)(), 3)
        self.assertEqual(cur(sum)(1)(1, 1)(z=2, y=2)(2)(), 5)

    def test_rebinding_arguments_allowed(self):
        """
        There should be a currying process allowing for the rebinding of arguments
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curr(power_of)(exponent=2)

        power_of_three = power_of_two(exponent=3)

        self.assertEqual(power_of_three(2), 8)

    def test_rebinding_arguments_allowed_cur_version(self):
        """
        There should be a currying process allowing for the rebinding of arguments
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = cur(power_of)(exponent=2)

        power_of_three = power_of_two(exponent=3)

        self.assertEqual(power_of_three(2)(), 8)

    def test_get_default_args(self):
        """
        The default arguments should be valid for introspection even if a function is curried
        """

        def power_of(base, exponent, add_at_end=0):
            """doc: power of two"""
            return base ** exponent + add_at_end

        power_of_two = curr(power_of)(exponent=2)

        self.assertEqual(default_args(power_of), {'add_at_end': 0})
        self.assertEqual(default_args(power_of), default_args(power_of_two))

    def test_get_closed_vars(self):
        """
        'get_closed_vars' should extract all of the current binded variables to the callable closure
        """

        def f(x, y, z, w):
            return x + y + z + w

        closurespace = 'kwargs'

        g = curr(f)(x=2)
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

        f = curr(f)

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

    def test_get_closure_bindings_cur_version(self):
        """
        'closure_bindings' should return all of currently bound variables by the user.
        By using the 'cur' version, the execution should be deferred until it is explicitly requested
        Additionally default arguments of the original function should also be retrieved
        """

        def f(df, y, z, w=10):
            return df + y + z + w

        f = cur(f)

        g = f(z=9)
        self.assertDictEqual(closed_bindings(g), {'z': 9, 'w': 10})

        h = g(z=100, w=100)
        self.assertDictEqual(closed_bindings(h), {'z': 100, 'w': 100})

        i = h(y=2)
        self.assertDictEqual(closed_bindings(i), {'y': 2, 'z': 100, 'w': 100})

        g = f(1, 2, 3)
        self.assertEqual(g(), 16)

        g = f(1, 2, w=5)
        self.assertDictEqual(closed_bindings(g), {'df': 1, 'y': 2, 'w': 5})

        g = f(1, z=2, w=5)
        self.assertDictEqual(closed_bindings(g), {'df': 1, 'z': 2, 'w': 5})

        g = f(y=1, z=2, w=5)(5)
        self.assertEqual(g(), 13)

        g = f(y=1, w=5)(df=1, z=5)
        self.assertEqual(g(), 12)

        g = f(4, y=1, z=5)
        self.assertEqual(g(), 20)

        g = f(y=1, w=5)(z=5)
        self.assertDictEqual(closed_bindings(g), {'y': 1, 'z': 5, 'w': 5})

        g = f(y=1)(z=5)(2)
        self.assertEqual(g(), 18)

        g = f(y=1)(z=5)(w=2)
        self.assertDictEqual(closed_bindings(g), {'y': 1, 'z': 5, 'w': 2})

        g = f(1)(5)(2)
        self.assertEqual(g(), 18)

        g = f(y=1)(z=5)(w=2)(w=10)(z=10)(y=10)(10)
        self.assertEqual(g(), 40)

        g = f(y=1)(1, y=2, z=1, w=1)
        self.assertEqual(g(), 5)

        g = f(1)
        self.assertEqual(g(100, y=2, z=1, w=1)(), 5)

        g = f(1)
        self.assertEqual(g(df=100, y=2, z=1, w=1)(), 104)

        g = f(y=1)(1)(5, 2, z=1, w=1)
        self.assertEqual(g(), 4)

    def test_original_function_traditional_call(self):
        """
        The currying mechanism should not break the function's normal behaviour when the 'curr' flavour is used.
        """

        def f(x, y, z, w=10):
            return x + y + z + w

        f = curr(f)

        self.assertEqual(f(1, 2, 3), 16)
        self.assertEqual(f(1, 2, 3, 4), 10)
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

    def test_copy_function(self):
        """
        'copy_func(f)' should  make a copy of a given function 'f'
        The function's object attributes should also be properly handled
        """
        def f(a, b):
            return a + b

        f.attr = 1

        g = copy_func(f)
        self.assertEqual(g(1, 1), 2)
        self.assertTrue(g.attr, 1)

        g.attr = 2
        self.assertTrue(g.attr, 2)
        self.assertTrue(f.attr, 1)
