"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="batch-job-handlers",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A lambda that deals with batch job notifications",
    long_description="A lambda that deals with batch job notifications",
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["batch_job_handler=batch_job_handler:main"]},
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse", "boto3"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
