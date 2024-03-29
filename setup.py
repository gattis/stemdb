from distutils.core import setup, Extension

setup(
    name = "stemdb",
    version = "0.1",
    author = "Matt Gattis",
    author_email = "gattis@gmail.com",
    license = "MIT",
    long_description = " ",
    ext_modules = [Extension(
        "stemdb",
        sources = ["stemdb.c"],
        ) ],
    url = "http://code.google.com/p/stemdb/",
)
