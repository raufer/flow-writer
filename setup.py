from setuptools import setup, find_packages

setup(
    name='flow_writer',
    version='0.1',
    description='A proof of concept library for supporting the creation of ML pipelines',
    packages=find_packages(),
    author='raufer',
    url='',
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "pyyaml==5.4",
        "colorama==0.3.9",
        "termcolor==1.1.0"
    ]
)
