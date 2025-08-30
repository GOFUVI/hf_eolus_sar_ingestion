"""Helper utilities for writing Parquet datasets and STAC metadata to S3.

The ingestion pipeline interacts with AWS S3 using `pyarrow` and emits
SpatioTemporal Asset Catalog (STAC) metadata using the `pystac` package.  The
native APIs for these libraries can be rather verbose, especially when dealing
with temporary network issues like throttling.  This module centralizes that
logic so the main Glue scripts stay focused on data transformation.
"""

from __future__ import annotations

import time
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from pystac.stac_io import DefaultStacIO


class PyarrowS3IO(DefaultStacIO):
    """Minimal STAC I/O implementation using ``pyarrow`` for S3 paths.

    PySTAC normally reads and writes files using standard Python file I/O.  When
    dealing with objects stored on S3 this is not sufficient, because STAC may
    need to load or save JSON documents directly in S3 buckets.  The
    :class:`PyarrowS3IO` class plugs into the PySTAC I/O system and performs
    those operations using ``pyarrow``'s high performance S3 client.
    """

    def _filesystem(self) -> pafs.S3FileSystem:
        """Return an :class:`~pyarrow.fs.S3FileSystem` using environment credentials.

        ``pyarrow`` automatically sources AWS credentials from the environment,
        including optional profile information provided via the ``AWS_PROFILE``
        variable.  By centralizing filesystem creation in a helper method we can
        reuse the same configuration for both reads and writes.
        """
        return pafs.S3FileSystem()

    def read_text(self, href: str) -> str:  # type: ignore[override]
        """Read a text file from either S3 or the local filesystem.

        Parameters
        ----------
        href:
            A URL-style path.  When the path begins with ``s3://`` the content is
            read from S3 using :mod:`pyarrow`.  Otherwise the default PySTAC
            implementation handles the read.
        """
        if href.startswith("s3://"):
            fs = self._filesystem()
            # ``open_input_file`` expects the path without the ``s3://`` prefix
            with fs.open_input_file(href[len("s3://") :]) as _f:
                return _f.read().decode("utf-8")
        # Fallback to DefaultStacIO for non-S3 paths
        return super().read_text(href)

    def write_text(self, href: str, txt: str) -> None:  # type: ignore[override]
        """Write a text file to either S3 or the local filesystem.

        PySTAC delegates all writing of catalog and item JSON to this method.
        The implementation mirrors :meth:`read_text` by detecting S3 URLs and
        routing them through ``pyarrow``.  Non-S3 paths use the default
        filesystem access provided by PySTAC.
        """
        if href.startswith("s3://"):
            fs = self._filesystem()
            with fs.open_output_stream(href[len("s3://") :]) as _f:
                _f.write(txt.encode("utf-8"))
        else:
            super().write_text(href, txt)


def write_dataset_with_retry(
    table,
    root_path,
    partition_cols,
    filesystem,
    retries: int = 5,
    backoff: int = 5,
    basename_template: str | None = None,
):
    """Write a Parquet dataset to S3, retrying on ``SLOW_DOWN`` errors.

    Parameters
    ----------
    table:
        A :class:`pyarrow.Table` containing the data to write.
    root_path:
        Destination path within the target filesystem.  For S3 this is typically
        ``"bucket/prefix"`` without the ``s3://`` scheme.
    partition_cols:
        Column names used to partition the dataset into subdirectories.
    filesystem:
        :class:`pyarrow.fs.FileSystem` instance used for writing (e.g. S3 or
        local filesystem).
    retries:
        Number of attempts before giving up.  The default is conservative and
        mirrors typical AWS backoff recommendations.
    backoff:
        Base backoff delay in seconds.  Each retry waits ``backoff * 2^(attempt-1)``.
    basename_template:
        Optional file name template used by :func:`pyarrow.parquet.write_to_dataset`.

    The AWS S3 service may occasionally respond with ``SLOW_DOWN`` when it is
    throttling requests.  This helper automatically retries the write operation
    with exponential backoff to give S3 time to recover.
    """

    for attempt in range(1, retries + 1):
        try:
            # Build the arguments for :func:`pyarrow.parquet.write_to_dataset`.
            write_kwargs = {
                "root_path": root_path,
                "partition_cols": partition_cols,
                "filesystem": filesystem,
            }
            if basename_template:
                write_kwargs["basename_template"] = basename_template
            pq.write_to_dataset(table, **write_kwargs)
            return
        except OSError as e:
            msg = str(e)
            if "SLOW_DOWN" in msg and attempt < retries:
                # When S3 signals that we are sending requests too quickly,
                # pause for an exponentially increasing amount of time before
                # retrying.  This behavior matches AWS best practices for
                # throttling.
                wait = backoff * (2 ** (attempt - 1))
                time.sleep(wait)
                continue
            # If the error is something other than throttling, or we have
            # exhausted our retries, re-raise the exception for the caller to
            # handle.
            raise


__all__ = ["PyarrowS3IO", "write_dataset_with_retry"]

