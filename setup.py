from distutils.core import setup
from Cython.Build import cythonize

# ext_options = {"compiler_directives": {"profile": True}, "annotate": True}
# setup(
#     ext_modules=cythonize("main.pyx", **ext_options)
# )


setup(
    name='VeratyFS',
    ext_modules=cythonize("VeratyFS.pyx"),
    zip_safe=False,
)