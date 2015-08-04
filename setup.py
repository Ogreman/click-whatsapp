from setuptools import setup

setup(
    name="whatsapp",
    version='1.0',
    py_modules=['whatsapp'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        whatsapp=whatsapp:cli
    ''',
)

