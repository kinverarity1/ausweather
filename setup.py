from setuptools import setup

setup(
    name="ausweather",
    packages=["ausweather"],
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Python package to download Australian rainfall data from the Bureau of Meteorology via the Queensland Government's SILO Patched Point Data service",
    long_description=open("README.md", mode="r").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kinverarity1/ausweather",
    author="Kent Inverarity",
    author_email="kinverarity@hotmail.com",
    license="MIT",
    classifiers=(
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ),
    keywords="rainfall australia bom silo python data-access",
    install_requires=("pandas", "requests", "matplotlib"),
    include_package_data=True,
)
