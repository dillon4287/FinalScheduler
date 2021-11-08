import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="FinalScheduler",
    version="0.0.1",
    author="Dillon Flannery",
    author_email="dflannery@weimar.edu",
    description="Schedules courses for final exam slots",
    url="https://github.com/dillon4287/CodeProjects/Python/FinalScheduler",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages = setuptools.find_packages(),
    scripts=["final_scheduler.py"],
    python_requires=">=3.6",
)